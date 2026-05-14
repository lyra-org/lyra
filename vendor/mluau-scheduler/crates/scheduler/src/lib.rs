mod r#async;
pub mod taskmgr;
pub mod userdata;
pub mod schedulers;

pub use r#async::LuaSchedulerAsync;
pub use r#async::LuaSchedulerAsyncUserData;

pub const IS_SEND: bool = false;

// Use XRc in case we want to add a Send feature in the future
#[cfg(feature = "send")]
pub type XRc<T> = std::sync::Arc<T>;
#[cfg(not(feature = "send"))]
pub type XRc<T> = std::rc::Rc<T>;

#[cfg(feature = "send")]
pub type XWeak<T> = std::sync::Weak<T>;
#[cfg(not(feature = "send"))]
pub type XWeak<T> = std::rc::Weak<T>;

// Use XRefCell in case we want to add a Send feature in the future
#[cfg(not(feature = "send"))]
pub type XRefCell<T> = std::cell::RefCell<T>;
#[cfg(feature = "send")]
pub struct XRefCell<T>(std::sync::RwLock<T>);
#[cfg(feature = "send")]
impl<T> XRefCell<T> {
    pub fn new(x: T) -> Self {
        Self(std::sync::RwLock::new(x))
    }

    pub fn borrow(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.0.read().unwrap()
    }

    pub fn borrow_mut(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.0.write().unwrap()
    }
}

/// A trait that adds `Send` requirement if `send` feature is enabled.
#[cfg(feature = "send")]
pub trait MaybeSend: Send {}
#[cfg(feature = "send")]
impl<T: Send> MaybeSend for T {}

#[cfg(not(feature = "send"))]
pub trait MaybeSend {}
#[cfg(not(feature = "send"))]
impl<T> MaybeSend for T {}

/// A trait that adds `Sync` requirement if `send` feature is enabled.
#[cfg(feature = "send")]
pub trait MaybeSync: Sync {}
#[cfg(feature = "send")]
impl<T: Sync> MaybeSync for T {}

#[cfg(not(feature = "send"))]
pub trait MaybeSync {}
#[cfg(not(feature = "send"))]
impl<T> MaybeSync for T {}
