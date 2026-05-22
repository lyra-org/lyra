// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use harmony_core::{
    FunctionSpec,
    ModuleExport,
    ModuleSpec,
};
use harmony_luau as luau;
use harmony_luau::{
    DescribeInterface,
    DescribeUserData,
    LuauType,
    LuauTypeInfo,
    ModuleDescriptor,
    ModuleFunctionDescriptor,
    ParameterDescriptor,
    render_definition_file_with_support,
};
use harmony_luau::{
    FieldDescriptor,
    InterfaceDescriptor,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use tokio::net::UdpSocket as TokioUdpSocket;
use tokio::net::{
    TcpListener as TokioTcpListener,
    TcpStream as TokioTcpStream,
};
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::RwLock as TokioRwLock;

/// Wrapper for returning raw bytes as a Lua string from methods that lack
/// direct `Lua` access.
struct LuaBytes(Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct LuaBinaryInput(Vec<u8>);

impl LuaBinaryInput {}

impl LuauTypeInfo for LuaBinaryInput {
    fn luau_type() -> LuauType {
        LuauType::union(vec![String::luau_type(), LuauType::literal("buffer")])
    }
}

impl LuauTypeInfo for LuaBytes {
    fn luau_type() -> LuauType {
        String::luau_type()
    }
}

impl luau::ToLuau for LuaBytes {
    fn write(self, writer: &mut luau::ReturnWriter<'_>) -> luau::runtime::Result<()> {
        writer.write(luau::Value::String(self.0))
    }
}

impl luau::IntoLuauReturn for LuaBytes {
    fn into_luau_return(self) -> luau::runtime::Result<luau::ReturnValues> {
        luau::Value::String(self.0).into_luau_return()
    }
}

#[derive(Clone, Debug)]
struct SocketAddress {
    /// IP address (e.g. "192.168.1.1").
    address: String,
    /// Port number.
    port: u16,
}

impl From<SocketAddr> for SocketAddress {
    fn from(addr: SocketAddr) -> Self {
        Self {
            address: addr.ip().to_string(),
            port: addr.port(),
        }
    }
}

impl luau::ToLuau for SocketAddress {
    fn write(self, writer: &mut luau::ReturnWriter<'_>) -> luau::runtime::Result<()> {
        writer.write(socket_address_value(self))
    }
}

impl luau::IntoLuauReturn for SocketAddress {
    fn into_luau_return(self) -> luau::runtime::Result<luau::ReturnValues> {
        socket_address_value(self).into_luau_return()
    }
}

#[derive(Clone, Debug)]
struct UdpBindOptions {
    /// Local address to bind (e.g. "0.0.0.0").
    address: String,
    /// Local port to bind.
    port: u16,
    /// Enable SO_BROADCAST on the socket.
    broadcast: Option<bool>,
}

#[derive(Clone, Debug)]
struct TcpConnectOptions {
    /// Host to connect to (IP address or hostname).
    host: String,
    /// Port to connect to.
    port: u16,
    /// Connection timeout in seconds.
    timeout: Option<f64>,
}

#[derive(Clone, Debug)]
struct TcpBindOptions {
    /// Local address to bind (e.g. "0.0.0.0").
    address: String,
    /// Local port to listen on.
    port: u16,
}

const MAX_READ_BYTES: usize = 8 * 1024 * 1024; // 8 MB
const UDP_MAX_DATAGRAM: usize = 65535;

struct NetModule;

pub fn module_spec() -> ModuleSpec {
    ModuleSpec::new("harmony/net")
        .capability("harmony.net")
        .function(udp_bind_spec())
        .function(tcp_connect_spec())
        .function(tcp_bind_spec())
        .userdata(UdpSocket::_harmony_userdata_spec())
        .userdata(TcpStream::_harmony_userdata_spec())
        .userdata(TcpListener::_harmony_userdata_spec())
        .install(|_| Ok(ModuleExport::new(NetModule)))
}

fn udp_bind_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("udp_bind")
        .arg_name("options")
        .args::<UdpBindOptions>()
        .returns::<UdpSocket>();
    spec.call_async(Arc::new(udp_bind_callback))
}

fn tcp_connect_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("tcp_connect")
        .arg_name("options")
        .args::<TcpConnectOptions>()
        .returns::<TcpStream>();
    spec.call_async(Arc::new(tcp_connect_callback))
}

fn tcp_bind_spec() -> FunctionSpec {
    let spec = FunctionSpec::async_fn("tcp_bind")
        .arg_name("options")
        .args::<TcpBindOptions>()
        .returns::<TcpListener>();
    spec.call_async(Arc::new(tcp_bind_callback))
}

#[harmony_macros::userdata(name = "UdpSocket")]
#[derive(Clone)]
struct UdpSocket {
    inner: Arc<TokioRwLock<Option<Arc<TokioUdpSocket>>>>,
    timeout: Arc<StdMutex<Option<f64>>>,
    local_addr: Arc<StdMutex<SocketAddress>>,
}

#[harmony_macros::userdata_methods]
impl UdpSocket {
    #[harmony(
        description = "Sets the default timeout in seconds for all subsequent operations. Pass nil to clear.",
        args(seconds: Option<f64>)
    )]
    fn set_timeout(&self, seconds: Option<f64>) -> luau::runtime::Result<()> {
        if let Some(secs) = seconds {
            validate_timeout(secs)?;
        }
        let mut guard = self
            .timeout
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *guard = seconds;
        Ok(())
    }

    #[harmony(
        description = "Returns the local address this socket is bound to. After connect, reflects the interface used to reach the peer."
    )]
    fn address(&self) -> SocketAddress {
        self.local_addr
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    #[harmony(
        description = "Associates the socket with a remote address.",
        args(address: SocketAddress)
    )]
    fn connect(
        &self,
        vm: &luau::Vm,
        address: luau::Table,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let address = socket_address_from_luau(vm, &address)?;
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.connect_inner(address).await
        }))
    }

    #[harmony(skip)]
    async fn connect_inner(&self, address: SocketAddress) -> luau::runtime::Result<()> {
        let guard = self.inner.write().await;
        let socket = guard
            .as_ref()
            .ok_or_else(|| luau_runtime_error("socket is closed"))?;
        let addr = socket_addr(&address)?;
        socket
            .connect(addr)
            .await
            .map_err(|error| luau_runtime_error(format!("connect failed: {error}")))?;

        let local = socket
            .local_addr()
            .map(SocketAddress::from)
            .map_err(|error| luau_runtime_error(format!("failed to get local address: {error}")))?;
        *self
            .local_addr
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = local;
        Ok(())
    }

    #[harmony(
        description = "Sends a datagram on a connected socket.",
        args(data: LuaBinaryInput, timeout: Option<f64>)
    )]
    fn send(
        &self,
        data: luau::Value,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let data = binary_input_from_luau("data", data)?;
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.send_inner(data, timeout).await
        }))
    }

    #[harmony(skip)]
    async fn send_inner(
        &self,
        data: LuaBinaryInput,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<()> {
        let guard = self.inner.read().await;
        let socket = guard
            .as_ref()
            .ok_or_else(|| luau_runtime_error("socket is closed"))?;
        let timeout = effective_timeout(timeout, &self.timeout)?;
        let bytes = data.0;

        with_optional_timeout(timeout, async {
            socket
                .send(&bytes)
                .await
                .map_err(|error| luau_runtime_error(format!("send failed: {error}")))?;
            Ok(())
        })
        .await
    }

    #[harmony(
        description = "Receives a datagram on a connected socket.",
        returns(LuaBytes)
    )]
    fn recv(&self, timeout: Option<f64>) -> luau::runtime::Result<luau::ScheduledFuture> {
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.recv_inner(timeout).await
        }))
    }

    #[harmony(skip)]
    async fn recv_inner(&self, timeout: Option<f64>) -> luau::runtime::Result<LuaBytes> {
        let guard = self.inner.read().await;
        let socket = guard
            .as_ref()
            .ok_or_else(|| luau_runtime_error("socket is closed"))?;
        let timeout = effective_timeout(timeout, &self.timeout)?;

        let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
        let n = with_optional_timeout(timeout, async {
            socket
                .recv(&mut buf)
                .await
                .map_err(|error| luau_runtime_error(format!("recv failed: {error}")))
        })
        .await?;

        buf.truncate(n);
        Ok(LuaBytes(buf))
    }

    #[harmony(
        description = "Receives a datagram. Returns the data and the sender address.",
        returns(LuaBytes, SocketAddress)
    )]
    fn recv_from(&self, timeout: Option<f64>) -> luau::runtime::Result<luau::ScheduledFuture> {
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.recv_from_inner(timeout).await
        }))
    }

    #[harmony(skip)]
    async fn recv_from_inner(
        &self,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<(LuaBytes, SocketAddress)> {
        let guard = self.inner.read().await;
        let socket = guard
            .as_ref()
            .ok_or_else(|| luau_runtime_error("socket is closed"))?;
        let timeout = effective_timeout(timeout, &self.timeout)?;

        let mut buf = vec![0u8; UDP_MAX_DATAGRAM];
        let (n, addr) = with_optional_timeout(timeout, async {
            socket
                .recv_from(&mut buf)
                .await
                .map_err(|error| luau_runtime_error(format!("recv_from failed: {error}")))
        })
        .await?;

        buf.truncate(n);
        Ok((LuaBytes(buf), SocketAddress::from(addr)))
    }

    #[harmony(
        description = "Sends a datagram to the specified address.",
        args(data: LuaBinaryInput, address: SocketAddress, timeout: Option<f64>)
    )]
    fn send_to(
        &self,
        vm: &luau::Vm,
        data: luau::Value,
        address: luau::Table,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let data = binary_input_from_luau("data", data)?;
        let address = socket_address_from_luau(vm, &address)?;
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.send_to_inner(data, address, timeout).await
        }))
    }

    #[harmony(skip)]
    async fn send_to_inner(
        &self,
        data: LuaBinaryInput,
        address: SocketAddress,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<()> {
        let guard = self.inner.read().await;
        let socket = guard
            .as_ref()
            .ok_or_else(|| luau_runtime_error("socket is closed"))?;
        let timeout = effective_timeout(timeout, &self.timeout)?;
        let addr = socket_addr(&address)?;
        let bytes = data.0;

        with_optional_timeout(timeout, async {
            socket
                .send_to(&bytes, addr)
                .await
                .map_err(|error| luau_runtime_error(format!("send_to failed: {error}")))?;
            Ok(())
        })
        .await
    }

    #[harmony(description = "Closes the socket. Further operations will throw.")]
    fn close(&self) -> luau::runtime::Result<luau::ScheduledFuture> {
        let socket = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            socket.close_inner().await
        }))
    }

    #[harmony(skip)]
    async fn close_inner(&self) -> luau::runtime::Result<()> {
        let mut guard = self.inner.write().await;
        *guard = None;
        Ok(())
    }
}

fn udp_bind_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = udp_bind_options_from_luau(frame.vm, &table)?;
    let vm = frame.vm.clone();
    let origin = frame.context.origin.clone();
    let future = luau::ScheduledFuture::new(async move {
        let socket = bind_udp_socket(options).await?;
        UdpSocket::_harmony_userdata_class().create_value(&vm, &origin, socket)
    });
    Ok(future)
}

async fn bind_udp_socket(options: UdpBindOptions) -> luau::runtime::Result<UdpSocket> {
    let addr = parse_bind_address(&options.address, options.port)?;

    let socket = TokioUdpSocket::bind(addr)
        .await
        .map_err(|error| luau_runtime_error(format!("failed to bind UDP socket: {error}")))?;

    if options.broadcast.unwrap_or(false) {
        socket
            .set_broadcast(true)
            .map_err(|error| luau_runtime_error(format!("failed to enable broadcast: {error}")))?;
    }

    let local_addr = socket
        .local_addr()
        .map(SocketAddress::from)
        .map_err(|error| luau_runtime_error(format!("failed to get local address: {error}")))?;

    Ok(UdpSocket {
        inner: Arc::new(TokioRwLock::new(Some(Arc::new(socket)))),
        timeout: Arc::new(StdMutex::new(None)),
        local_addr: Arc::new(StdMutex::new(local_addr)),
    })
}

#[harmony_macros::userdata(name = "TcpStream")]
#[derive(Clone)]
struct TcpStream {
    inner: Arc<TokioMutex<Option<TokioTcpStream>>>,
    timeout: Arc<StdMutex<Option<f64>>>,
    local_addr: SocketAddress,
}

#[harmony_macros::userdata_methods]
impl TcpStream {
    #[harmony(
        description = "Sets the default timeout in seconds for all subsequent operations. Pass nil to clear.",
        args(seconds: Option<f64>)
    )]
    fn set_timeout(&self, seconds: Option<f64>) -> luau::runtime::Result<()> {
        if let Some(secs) = seconds {
            validate_timeout(secs)?;
        }
        let mut guard = self
            .timeout
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *guard = seconds;
        Ok(())
    }

    #[harmony(description = "Returns the local address this socket is bound to.")]
    fn address(&self) -> SocketAddress {
        self.local_addr.clone()
    }

    #[harmony(
        description = "Reads up to max_bytes. Yields until at least 1 byte arrives. Returns empty string on EOF.",
        args(max_bytes: usize, timeout: Option<f64>),
        returns(LuaBytes)
    )]
    fn read(
        &self,
        max_bytes: luau::Value,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let max_bytes = usize_from_luau("max_bytes", max_bytes)?;
        let stream = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            stream.read_inner(max_bytes, timeout).await
        }))
    }

    #[harmony(skip)]
    async fn read_inner(
        &self,
        max_bytes: usize,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<LuaBytes> {
        if max_bytes == 0 {
            return Err(luau_runtime_error("max_bytes must be greater than 0"));
        }
        if max_bytes > MAX_READ_BYTES {
            return Err(luau_runtime_error(format!(
                "max_bytes exceeds limit of {MAX_READ_BYTES} (8 MB)"
            )));
        }

        let timeout = effective_timeout(timeout, &self.timeout)?;
        let mut guard = self.inner.lock().await;
        let stream = guard
            .as_mut()
            .ok_or_else(|| luau_runtime_error("stream is closed"))?;

        let mut buf = vec![0u8; max_bytes];
        let n = with_optional_timeout(timeout, async {
            stream
                .read(&mut buf)
                .await
                .map_err(|error| luau_runtime_error(format!("read failed: {error}")))
        })
        .await?;

        buf.truncate(n);
        Ok(LuaBytes(buf))
    }

    #[harmony(
        description = "Writes all bytes to the stream.",
        args(data: LuaBinaryInput, timeout: Option<f64>)
    )]
    fn write(
        &self,
        data: luau::Value,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let data = binary_input_from_luau("data", data)?;
        let stream = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            stream.write_inner(data, timeout).await
        }))
    }

    #[harmony(skip)]
    async fn write_inner(
        &self,
        data: LuaBinaryInput,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<()> {
        let timeout = effective_timeout(timeout, &self.timeout)?;
        let mut guard = self.inner.lock().await;
        let stream = guard
            .as_mut()
            .ok_or_else(|| luau_runtime_error("stream is closed"))?;

        let bytes = data.0;
        with_optional_timeout(timeout, async {
            stream
                .write_all(&bytes)
                .await
                .map_err(|error| luau_runtime_error(format!("write failed: {error}")))
        })
        .await
    }

    #[harmony(description = "Closes the stream. Further operations will throw.")]
    fn close(&self) -> luau::runtime::Result<luau::ScheduledFuture> {
        let stream = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            stream.close_inner().await
        }))
    }

    #[harmony(skip)]
    async fn close_inner(&self) -> luau::runtime::Result<()> {
        let mut guard = self.inner.lock().await;
        if let Some(mut stream) = guard.take() {
            let _ = stream.shutdown().await;
        }
        Ok(())
    }
}

#[harmony_macros::userdata(name = "TcpListener")]
#[derive(Clone)]
struct TcpListener {
    inner: Arc<TokioMutex<Option<Arc<TokioTcpListener>>>>,
    timeout: Arc<StdMutex<Option<f64>>>,
    local_addr: SocketAddress,
}

#[harmony_macros::userdata_methods]
impl TcpListener {
    #[harmony(
        description = "Sets the default timeout in seconds for all subsequent operations. Pass nil to clear.",
        args(seconds: Option<f64>)
    )]
    fn set_timeout(&self, seconds: Option<f64>) -> luau::runtime::Result<()> {
        if let Some(secs) = seconds {
            validate_timeout(secs)?;
        }
        let mut guard = self
            .timeout
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *guard = seconds;
        Ok(())
    }

    #[harmony(description = "Returns the local address this socket is bound to.")]
    fn address(&self) -> SocketAddress {
        self.local_addr.clone()
    }

    #[harmony(
        description = "Accepts a new connection. Returns the stream and the client address.",
        returns(TcpStream, SocketAddress)
    )]
    fn accept(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<luau::ScheduledFuture> {
        let listener = self.clone();
        let vm = vm.clone();
        let origin = origin.clone();
        Ok(luau::ScheduledFuture::new(async move {
            let (stream, address) = listener.accept_inner(timeout).await?;
            let stream = TcpStream::_harmony_userdata_class().create_value(&vm, &origin, stream)?;
            Ok((stream, address))
        }))
    }

    #[harmony(skip)]
    async fn accept_inner(
        &self,
        timeout: Option<f64>,
    ) -> luau::runtime::Result<(TcpStream, SocketAddress)> {
        let listener = {
            let guard = self.inner.lock().await;
            guard
                .as_ref()
                .cloned()
                .ok_or_else(|| luau_runtime_error("listener is closed"))?
        };
        let timeout = effective_timeout(timeout, &self.timeout)?;

        let (stream, addr) = with_optional_timeout(timeout, async {
            listener
                .accept()
                .await
                .map_err(|error| luau_runtime_error(format!("accept failed: {error}")))
        })
        .await?;

        let local_addr = stream
            .local_addr()
            .map(SocketAddress::from)
            .map_err(|error| luau_runtime_error(format!("failed to get local address: {error}")))?;
        let stream = TcpStream {
            inner: Arc::new(TokioMutex::new(Some(stream))),
            timeout: Arc::new(StdMutex::new(None)),
            local_addr,
        };
        Ok((stream, SocketAddress::from(addr)))
    }

    #[harmony(description = "Closes the listener. Further operations will throw.")]
    fn close(&self) -> luau::runtime::Result<luau::ScheduledFuture> {
        let listener = self.clone();
        Ok(luau::ScheduledFuture::new(async move {
            listener.close_inner().await
        }))
    }

    #[harmony(skip)]
    async fn close_inner(&self) -> luau::runtime::Result<()> {
        let mut guard = self.inner.lock().await;
        *guard = None;
        Ok(())
    }
}

fn tcp_connect_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = tcp_connect_options_from_luau(frame.vm, &table)?;
    let vm = frame.vm.clone();
    let origin = frame.context.origin.clone();
    let future = luau::ScheduledFuture::new(async move {
        let stream = connect_tcp_stream(options).await?;
        TcpStream::_harmony_userdata_class().create_value(&vm, &origin, stream)
    });
    Ok(future)
}

async fn connect_tcp_stream(options: TcpConnectOptions) -> luau::runtime::Result<TcpStream> {
    let addr = format!("{}:{}", options.host, options.port);

    let timeout = options.timeout.map(validate_timeout).transpose()?;
    let connect_fut = TokioTcpStream::connect(&addr);
    let stream = match timeout {
        Some(duration) => tokio::time::timeout(duration, connect_fut)
            .await
            .map_err(|_| luau_runtime_error("connection timed out"))?,
        None => connect_fut.await,
    }
    .map_err(|error| luau_runtime_error(format!("failed to connect: {error}")))?;

    let local_addr = stream
        .local_addr()
        .map(SocketAddress::from)
        .map_err(|error| luau_runtime_error(format!("failed to get local address: {error}")))?;

    Ok(TcpStream {
        inner: Arc::new(TokioMutex::new(Some(stream))),
        timeout: Arc::new(StdMutex::new(None)),
        local_addr,
    })
}

fn tcp_bind_callback(
    mut frame: luau::AsyncCallFrame<'_>,
) -> luau::runtime::Result<luau::ScheduledFuture> {
    let table: luau::Table = frame.args.read_named("options")?;
    let options = tcp_bind_options_from_luau(frame.vm, &table)?;
    let vm = frame.vm.clone();
    let origin = frame.context.origin.clone();
    let future = luau::ScheduledFuture::new(async move {
        let listener = bind_tcp_listener(options).await?;
        TcpListener::_harmony_userdata_class().create_value(&vm, &origin, listener)
    });
    Ok(future)
}

async fn bind_tcp_listener(options: TcpBindOptions) -> luau::runtime::Result<TcpListener> {
    let addr = parse_bind_address(&options.address, options.port)?;

    let listener = TokioTcpListener::bind(addr)
        .await
        .map_err(|error| luau_runtime_error(format!("failed to bind TCP listener: {error}")))?;

    let local_addr = listener
        .local_addr()
        .map(SocketAddress::from)
        .map_err(|error| luau_runtime_error(format!("failed to get local address: {error}")))?;

    Ok(TcpListener {
        inner: Arc::new(TokioMutex::new(Some(Arc::new(listener)))),
        timeout: Arc::new(StdMutex::new(None)),
        local_addr,
    })
}

fn udp_bind_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<UdpBindOptions> {
    Ok(UdpBindOptions {
        address: required_string_field(vm, table, "address")?,
        port: required_u16_field(vm, table, "port")?,
        broadcast: optional_bool_field(vm, table, "broadcast")?,
    })
}

fn tcp_connect_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<TcpConnectOptions> {
    Ok(TcpConnectOptions {
        host: required_string_field(vm, table, "host")?,
        port: required_u16_field(vm, table, "port")?,
        timeout: optional_f64_field(vm, table, "timeout")?,
    })
}

fn tcp_bind_options_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<TcpBindOptions> {
    Ok(TcpBindOptions {
        address: required_string_field(vm, table, "address")?,
        port: required_u16_field(vm, table, "port")?,
    })
}

fn socket_address_from_luau(
    vm: &luau::Vm,
    table: &luau::Table,
) -> luau::runtime::Result<SocketAddress> {
    Ok(SocketAddress {
        address: required_string_field(vm, table, "address")?,
        port: required_u16_field(vm, table, "port")?,
    })
}

fn socket_address_value(address: SocketAddress) -> luau::Value {
    let mut table = luau::OwnedTable::with_capacity(0, 2);
    table.set_field("address", luau::Value::String(address.address.into_bytes()));
    table.set_field("port", luau::Value::Integer(address.port.into()));
    luau::Value::TableData(table)
}

fn required_string_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<String> {
    match table.get_raw(vm, field)? {
        luau::Value::String(value) => String::from_utf8(value)
            .map_err(|error| luau_runtime_error(format!("'{field}' must be valid UTF-8: {error}"))),
        luau::Value::Nil => Err(luau_runtime_error(format!("missing '{field}' field"))),
        other => Err(luau_field_type_error(field, "string", other.type_name())),
    }
}

fn required_u16_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<u16> {
    match table.get_raw(vm, field)? {
        luau::Value::Integer(value) if (0..=u16::MAX as i64).contains(&value) => Ok(value as u16),
        luau::Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value >= 0.0
                && value <= u16::MAX as f64 =>
        {
            Ok(value as u16)
        }
        luau::Value::Nil => Err(luau_runtime_error(format!("missing '{field}' field"))),
        other => Err(luau_field_type_error(
            field,
            "integer in the range 0..65535",
            other.type_name(),
        )),
    }
}

fn optional_f64_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<f64>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Integer(value) => Ok(Some(value as f64)),
        luau::Value::Number(value) => Ok(Some(value)),
        other => Err(luau_field_type_error(field, "number", other.type_name())),
    }
}

fn optional_bool_field(
    vm: &luau::Vm,
    table: &luau::Table,
    field: &'static str,
) -> luau::runtime::Result<Option<bool>> {
    match table.get_raw(vm, field)? {
        luau::Value::Nil => Ok(None),
        luau::Value::Boolean(value) => Ok(Some(value)),
        other => Err(luau_field_type_error(field, "boolean", other.type_name())),
    }
}

fn usize_from_luau(field: &'static str, value: luau::Value) -> luau::runtime::Result<usize> {
    match value {
        luau::Value::Integer(value) if value >= 0 => usize::try_from(value)
            .map_err(|_| luau_runtime_error(format!("'{field}' value is out of range for usize"))),
        luau::Value::Number(value)
            if value.is_finite()
                && value.fract() == 0.0
                && value >= 0.0
                && value <= usize::MAX as f64 =>
        {
            Ok(value as usize)
        }
        other => Err(luau_field_type_error(
            field,
            "non-negative integer",
            other.type_name(),
        )),
    }
}

fn binary_input_from_luau(
    field: &'static str,
    value: luau::Value,
) -> luau::runtime::Result<LuaBinaryInput> {
    match value {
        luau::Value::String(value) | luau::Value::Buffer(value) => Ok(LuaBinaryInput(value)),
        other => Err(luau_field_type_error(
            field,
            "string or buffer",
            other.type_name(),
        )),
    }
}

fn validate_timeout(secs: f64) -> luau::runtime::Result<Duration> {
    if secs <= 0.0 || secs.is_nan() || secs.is_infinite() {
        return Err(luau_runtime_error(
            "timeout must be a finite number greater than 0",
        ));
    }
    Ok(Duration::from_secs_f64(secs))
}

fn effective_timeout(
    per_op: Option<f64>,
    socket_default: &StdMutex<Option<f64>>,
) -> luau::runtime::Result<Option<Duration>> {
    let secs = per_op.or_else(|| *socket_default.lock().unwrap_or_else(|e| e.into_inner()));
    secs.map(validate_timeout).transpose()
}

async fn with_optional_timeout<F, T>(timeout: Option<Duration>, fut: F) -> luau::runtime::Result<T>
where
    F: std::future::Future<Output = luau::runtime::Result<T>>,
{
    match timeout {
        Some(duration) => tokio::time::timeout(duration, fut)
            .await
            .map_err(|_| luau_runtime_error("operation timed out"))?,
        None => fut.await,
    }
}

fn parse_bind_address(address: &str, port: u16) -> luau::runtime::Result<SocketAddr> {
    let ip: std::net::IpAddr = address.parse().map_err(|error| {
        luau_runtime_error(format!("invalid bind address '{address}': {error}"))
    })?;
    Ok(SocketAddr::new(ip, port))
}

fn socket_addr(address: &SocketAddress) -> luau::runtime::Result<SocketAddr> {
    let ip: std::net::IpAddr = address.address.parse().map_err(|error| {
        luau_runtime_error(format!("invalid IP address '{}': {error}", address.address))
    })?;
    Ok(SocketAddr::new(ip, address.port))
}

fn luau_field_type_error(field: &str, expected: &str, actual: &str) -> luau::Error {
    luau_runtime_error(format!(
        "invalid '{field}' field: expected {expected}, got {actual}"
    ))
}

fn luau_runtime_error(message: impl Into<String>) -> luau::Error {
    luau::Error::Runtime(message.into())
}

struct NetModuleDocs;

impl NetModuleDocs {
    fn module_descriptor() -> ModuleDescriptor {
        ModuleDescriptor {
            name: "Net",
            local_name: "net",
            description: Some("UDP and TCP socket networking."),
            fields: Vec::new(),
            functions: vec![
                ModuleFunctionDescriptor {
                    path: vec!["udp_bind"],
                    description: Some("Binds a UDP socket to a local address and port."),
                    params: vec![ParameterDescriptor {
                        name: "options",
                        ty: UdpBindOptions::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("UdpSocket")],
                    yields: true,
                },
                ModuleFunctionDescriptor {
                    path: vec!["tcp_connect"],
                    description: Some(
                        "Connects to a remote TCP host. Hostnames are resolved automatically.",
                    ),
                    params: vec![ParameterDescriptor {
                        name: "options",
                        ty: TcpConnectOptions::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("TcpStream")],
                    yields: true,
                },
                ModuleFunctionDescriptor {
                    path: vec!["tcp_bind"],
                    description: Some("Binds a TCP listener to a local address and port."),
                    params: vec![ParameterDescriptor {
                        name: "options",
                        ty: TcpBindOptions::luau_type(),
                        description: None,
                        variadic: false,
                    }],
                    returns: vec![LuauType::literal("TcpListener")],
                    yields: true,
                },
            ],
        }
    }
}

fn field(name: &'static str, ty: LuauType, description: &'static str) -> FieldDescriptor {
    FieldDescriptor {
        name,
        ty,
        description: Some(description),
    }
}

impl LuauTypeInfo for SocketAddress {
    fn luau_type() -> LuauType {
        LuauType::literal("SocketAddress")
    }
}

impl DescribeInterface for SocketAddress {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "SocketAddress",
            description: None,
            fields: vec![
                field(
                    "address",
                    String::luau_type(),
                    "IP address (e.g. \"192.168.1.1\").",
                ),
                field("port", u16::luau_type(), "Port number."),
            ],
        }
    }
}

impl LuauTypeInfo for UdpBindOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("UdpBindOptions")
    }
}

impl DescribeInterface for UdpBindOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "UdpBindOptions",
            description: None,
            fields: vec![
                field(
                    "address",
                    String::luau_type(),
                    "Local address to bind (e.g. \"0.0.0.0\").",
                ),
                field("port", u16::luau_type(), "Local port to bind."),
                field(
                    "broadcast",
                    Option::<bool>::luau_type(),
                    "Enable SO_BROADCAST on the socket.",
                ),
            ],
        }
    }
}

impl LuauTypeInfo for TcpConnectOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("TcpConnectOptions")
    }
}

impl DescribeInterface for TcpConnectOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "TcpConnectOptions",
            description: None,
            fields: vec![
                field(
                    "host",
                    String::luau_type(),
                    "Host to connect to (IP address or hostname).",
                ),
                field("port", u16::luau_type(), "Port to connect to."),
                field(
                    "timeout",
                    Option::<f64>::luau_type(),
                    "Connection timeout in seconds.",
                ),
            ],
        }
    }
}

impl LuauTypeInfo for TcpBindOptions {
    fn luau_type() -> LuauType {
        LuauType::literal("TcpBindOptions")
    }
}

impl DescribeInterface for TcpBindOptions {
    fn interface_descriptor() -> InterfaceDescriptor {
        InterfaceDescriptor {
            name: "TcpBindOptions",
            description: None,
            fields: vec![
                field(
                    "address",
                    String::luau_type(),
                    "Local address to bind (e.g. \"0.0.0.0\").",
                ),
                field("port", u16::luau_type(), "Local port to listen on."),
            ],
        }
    }
}

pub fn render_luau_definition() -> Result<String, std::fmt::Error> {
    render_definition_file_with_support(
        &NetModuleDocs::module_descriptor(),
        &[],
        &[
            SocketAddress::interface_descriptor(),
            UdpBindOptions::interface_descriptor(),
            TcpConnectOptions::interface_descriptor(),
            TcpBindOptions::interface_descriptor(),
        ],
        &[
            UdpSocket::class_descriptor(),
            TcpStream::class_descriptor(),
            TcpListener::class_descriptor(),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::{
        module_spec,
        render_luau_definition,
    };

    #[test]
    fn exposes_handwritten_module_spec() {
        let spec = module_spec();

        assert_eq!(spec.id.0.as_ref(), "harmony/net");
        assert_eq!(spec.capability.as_ref().unwrap().0.as_ref(), "harmony.net");
        assert_eq!(spec.functions.len(), 3);
        assert!(spec.functions.iter().all(|function| function.yields));
        assert_eq!(spec.userdata.len(), 3);
        assert_eq!(spec.userdata[0].name.as_ref(), "UdpSocket");
        assert_eq!(spec.userdata[1].name.as_ref(), "TcpStream");
        assert_eq!(spec.userdata[2].name.as_ref(), "TcpListener");
        assert!(
            spec.userdata
                .iter()
                .flat_map(|userdata| userdata.methods.iter())
                .any(|method| method.name.as_ref() == "accept")
        );
    }

    #[test]
    fn renders_net_module_definition() {
        let rendered = render_luau_definition().expect("render harmony/net docs");

        assert!(rendered.contains("@class Net"));
        assert!(rendered.contains("@interface SocketAddress"));
        assert!(rendered.contains("@interface UdpBindOptions"));
        assert!(rendered.contains("@interface TcpConnectOptions"));
        assert!(rendered.contains("@interface TcpBindOptions"));
        assert!(rendered.contains("function net.udp_bind(options: UdpBindOptions): UdpSocket"));
        assert!(rendered.contains("string | buffer"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn luau_udp_socket_round_trips_datagram() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("net", &table)?;

        let root = vm.load_chunk(&harmony_luau::Chunk::new(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    local socket = net.udp_bind({ address = "127.0.0.1", port = 0 })
                    socket:set_timeout(0.05)
                    local address = socket:address()
                    socket:send_to("ping", address, 0.05)
                    local data, sender = socket:recv_from(0.05)
                    socket:close()
                    udp_data = data
                    udp_sender = sender.address
                    udp_port = address.port
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;
        scheduler.spawn_luau_thread(
            harmony_core::CallContext::default(),
            vm.clone(),
            thread,
            vec![],
        );

        for _ in 0..20 {
            scheduler.poll_ready();
            if vm.eval(
                std::sync::Arc::<[u8]>::from(&b"return udp_data"[..]),
                harmony_luau::ChunkOrigin::default(),
            )? == vec![harmony_luau::Value::String(b"ping".to_vec())]
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return udp_data, udp_sender, udp_port"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(values[0], harmony_luau::Value::String(b"ping".to_vec()));
        assert_eq!(
            values[1],
            harmony_luau::Value::String(b"127.0.0.1".to_vec())
        );
        assert!(
            matches!(values[2], harmony_luau::Value::Integer(port) if port > 0),
            "expected bound UDP port, got {:?}",
            values[2]
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn luau_tcp_listener_accepts_stream() -> harmony_luau::runtime::Result<()> {
        let vm = harmony_luau::Vm::new()?;
        vm.data().insert(harmony_core::LocalScheduler::new())?;
        let scheduler = vm.data().get::<harmony_core::LocalScheduler>()?;
        let spec = module_spec();
        let table =
            harmony_core::install_luau_module(&vm, &harmony_core::ChunkOrigin::default(), &spec)?;
        vm.set_global_table("net", &table)?;

        let root = vm.load_chunk(&harmony_luau::Chunk::new(
            std::sync::Arc::<[u8]>::from(
                &br#"
                    local listener = net.tcp_bind({ address = "127.0.0.1", port = 0 })
                    listener:set_timeout(0.05)
                    local address = listener:address()
                    local client = net.tcp_connect({
                        host = address.address,
                        port = address.port,
                        timeout = 0.05,
                    })
                    client:write("hello", 0.05)
                    local server, peer = listener:accept(0.05)
                    local data = server:read(5, 0.05)
                    client:close()
                    server:close()
                    listener:close()
                    tcp_data = data
                    tcp_peer = peer.address
                    tcp_port = address.port
                "#[..],
            ),
            harmony_luau::ChunkOrigin::default(),
        ))?;
        let thread = vm.create_thread(&root)?;
        scheduler.spawn_luau_thread(
            harmony_core::CallContext::default(),
            vm.clone(),
            thread,
            vec![],
        );

        for _ in 0..40 {
            scheduler.poll_ready();
            if vm.eval(
                std::sync::Arc::<[u8]>::from(&b"return tcp_data"[..]),
                harmony_luau::ChunkOrigin::default(),
            )? == vec![harmony_luau::Value::String(b"hello".to_vec())]
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let values = vm.eval(
            std::sync::Arc::<[u8]>::from(&b"return tcp_data, tcp_peer, tcp_port"[..]),
            harmony_luau::ChunkOrigin::default(),
        )?;
        assert_eq!(values[0], harmony_luau::Value::String(b"hello".to_vec()));
        assert_eq!(
            values[1],
            harmony_luau::Value::String(b"127.0.0.1".to_vec())
        );
        assert!(
            matches!(values[2], harmony_luau::Value::Integer(port) if port > 0),
            "expected bound TCP port, got {:?}",
            values[2]
        );
        Ok(())
    }
}
