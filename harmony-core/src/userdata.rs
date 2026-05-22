use std::{
    marker::PhantomData,
    sync::Arc,
};

use harmony_luau as luau;

type SyncMethod<T> =
    Arc<dyn for<'vm> Fn(T, luau::CallFrame<'vm>) -> luau::runtime::Result<()> + Send + Sync>;
type AsyncMethod<T> = Arc<
    dyn for<'vm> Fn(T, luau::AsyncCallFrame<'vm>) -> luau::runtime::Result<luau::ScheduledFuture>
        + Send
        + Sync,
>;
type EqMethod = Arc<
    dyn Fn(
            &luau::Vm,
            &luau::UserData,
            &luau::UserData,
            luau::UserDataTag,
        ) -> luau::runtime::Result<bool>
        + Send
        + Sync,
>;

pub struct UserDataClass<T> {
    class_name: &'static str,
    methods: Vec<UserDataMethod<T>>,
    variants: Vec<UserDataVariant<T>>,
    equality: Option<EqMethod>,
    _marker: PhantomData<fn() -> T>,
}

pub trait UserDataType: Clone + 'static {
    const CLASS_NAME: &'static str;
    const DESCRIPTION: Option<&'static str> = None;
}

struct UserDataMethod<T> {
    name: &'static str,
    argument_names: Vec<Arc<str>>,
    callback: UserDataMethodCallback<T>,
}

struct UserDataVariant<T> {
    name: &'static str,
    value: T,
}

enum UserDataMethodCallback<T> {
    Sync(SyncMethod<T>),
    Async(AsyncMethod<T>),
}

struct UserDataClassRuntime<T> {
    metatable: luau::Table,
    _marker: PhantomData<fn() -> T>,
}

impl<T> UserDataClass<T>
where
    T: Clone + 'static,
{
    pub fn new(class_name: &'static str) -> Self {
        Self {
            class_name,
            methods: Vec::new(),
            variants: Vec::new(),
            equality: None,
            _marker: PhantomData,
        }
    }

    pub fn variant(mut self, name: &'static str, value: T) -> Self {
        self.variants.push(UserDataVariant { name, value });
        self
    }

    pub fn equality(mut self) -> Self
    where
        T: PartialEq,
    {
        self.equality = Some(Arc::new(equals_userdata::<T>));
        self
    }

    pub fn method<const N: usize>(
        mut self,
        name: &'static str,
        argument_names: [&'static str; N],
        callback: impl for<'vm> Fn(T, luau::CallFrame<'vm>) -> luau::runtime::Result<()>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.methods.push(UserDataMethod {
            name,
            argument_names: argument_names.into_iter().map(Arc::from).collect(),
            callback: UserDataMethodCallback::Sync(Arc::new(callback)),
        });
        self
    }

    pub fn async_method<const N: usize>(
        mut self,
        name: &'static str,
        argument_names: [&'static str; N],
        callback: impl for<'vm> Fn(
            T,
            luau::AsyncCallFrame<'vm>,
        ) -> luau::runtime::Result<luau::ScheduledFuture>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.methods.push(UserDataMethod {
            name,
            argument_names: argument_names.into_iter().map(Arc::from).collect(),
            callback: UserDataMethodCallback::Async(Arc::new(callback)),
        });
        self
    }

    pub fn create(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        value: T,
    ) -> luau::runtime::Result<luau::UserData> {
        let tag = vm.userdata_tag::<T>()?;
        let userdata = vm.create_userdata(tag, value)?;
        let metatable = self.metatable(vm, origin, tag)?;
        userdata.set_metatable_raw(vm, Some(&metatable))?;
        Ok(userdata)
    }

    pub fn create_value(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        value: T,
    ) -> luau::runtime::Result<luau::Value> {
        self.create(vm, origin, value).map(luau::Value::UserData)
    }

    pub fn read_userdata(
        &self,
        vm: &luau::Vm,
        userdata: &luau::UserData,
    ) -> luau::runtime::Result<T> {
        let tag = vm.userdata_tag::<T>()?;
        userdata.borrow::<T>(vm, tag).map(|value| value.clone())
    }

    pub fn read_value(
        &self,
        vm: &luau::Vm,
        name: impl Into<Arc<str>>,
        value: luau::Value,
    ) -> luau::runtime::Result<T> {
        match value {
            luau::Value::UserData(userdata) => self.read_userdata(vm, &userdata),
            other => Err(luau::Error::ArgumentType {
                name: name.into(),
                expected: self.class_name,
                actual: other.type_name(),
            }),
        }
    }

    pub fn create_variant_table(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
    ) -> luau::runtime::Result<luau::Table> {
        let table = vm.create_table_with_capacity(0, self.variants.len() as i32)?;
        for variant in &self.variants {
            let value = self.create_value(vm, origin, variant.value.clone())?;
            table.set_raw(vm, variant.name, value)?;
        }
        table.set_readonly(vm, true)?;
        Ok(table)
    }

    pub fn install_variant_table(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        root: &luau::Table,
    ) -> luau::runtime::Result<()> {
        let table = self.create_variant_table(vm, origin)?;
        root.set_table_raw(vm, self.class_name, &table)
    }

    fn metatable(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        tag: luau::UserDataTag,
    ) -> luau::runtime::Result<luau::Table> {
        match vm.data().get::<UserDataClassRuntime<T>>() {
            Ok(runtime) => return Ok(runtime.metatable.clone()),
            Err(luau::Error::MissingContext { .. }) => {}
            Err(error) => return Err(error),
        }

        let methods = vm.create_table_with_capacity(0, self.methods.len() as i32)?;
        for method in &self.methods {
            self.install_method(vm, origin, tag, &methods, method)?;
        }

        let metatable = vm.create_table_with_capacity(0, 1)?;
        metatable.set_table_raw(vm, "__index", &methods)?;
        if let Some(equality) = &self.equality {
            self.install_equality_method(vm, origin, tag, &metatable, equality.clone())?;
        }
        methods.set_readonly(vm, true)?;
        metatable.set_readonly(vm, true)?;
        vm.data().insert(UserDataClassRuntime::<T> {
            metatable: metatable.clone(),
            _marker: PhantomData,
        })?;
        Ok(metatable)
    }

    fn install_method(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        tag: luau::UserDataTag,
        methods: &luau::Table,
        method: &UserDataMethod<T>,
    ) -> luau::runtime::Result<()> {
        let options = luau::NativeFunctionOptions::new(origin.clone())
            .function_name(format!("{}.{}", self.class_name, method.name))
            .argument_names(method.argument_names.clone())
            .use_thread_context_origin(true);
        let function = match &method.callback {
            UserDataMethodCallback::Sync(callback) => {
                let callback = callback.clone();
                vm.create_function_with_options(
                    options,
                    Arc::new(move |mut frame| {
                        let this = read_self(&mut frame.args, frame.vm, tag)?;
                        callback(this, frame)
                    }),
                )?
            }
            UserDataMethodCallback::Async(callback) => {
                let callback = callback.clone();
                vm.create_function_with_options(
                    options,
                    crate::modules::async_luau_callback(Arc::new(move |mut frame| {
                        let this = read_self(&mut frame.args, frame.vm, tag)?;
                        callback(this, frame)
                    })),
                )?
            }
        };
        methods.set_function_raw(vm, method.name, &function)
    }

    fn install_equality_method(
        &self,
        vm: &luau::Vm,
        origin: &luau::ChunkOrigin,
        tag: luau::UserDataTag,
        metatable: &luau::Table,
        equality: EqMethod,
    ) -> luau::runtime::Result<()> {
        let options = luau::NativeFunctionOptions::new(origin.clone())
            .function_name(format!("{}.__eq", self.class_name))
            .argument_names([Arc::from("lhs"), Arc::from("rhs")])
            .use_thread_context_origin(true);
        let function = vm.create_function_with_options(
            options,
            Arc::new(move |mut frame| {
                let lhs: luau::UserData = frame.args.read_named("lhs")?;
                let rhs: luau::UserData = frame.args.read_named("rhs")?;
                frame.returns.write(equality(frame.vm, &lhs, &rhs, tag)?)
            }),
        )?;
        metatable.set_function_raw(vm, "__eq", &function)
    }
}

fn read_self<T>(
    args: &mut luau::ArgReader<'_>,
    vm: &luau::Vm,
    tag: luau::UserDataTag,
) -> luau::runtime::Result<T>
where
    T: Clone + 'static,
{
    let userdata: luau::UserData = args.read_named("self")?;
    userdata.borrow::<T>(vm, tag).map(|value| value.clone())
}

fn equals_userdata<T>(
    vm: &luau::Vm,
    lhs: &luau::UserData,
    rhs: &luau::UserData,
    tag: luau::UserDataTag,
) -> luau::runtime::Result<bool>
where
    T: PartialEq + 'static,
{
    if lhs.tag() != tag || rhs.tag() != tag {
        return Ok(false);
    }
    Ok(*lhs.borrow::<T>(vm, tag)? == *rhs.borrow::<T>(vm, tag)?)
}
