use std::cell::RefCell;
use crate::runtime::driver::{Handle};
use crate::builder;

/// Owns the driver and resides in thread-local storage.
pub(crate) struct RuntimeContext {
  driver: RefCell<Handle>,
}

impl RuntimeContext {
  /// Construct the context with an uninitialized driver.
  pub(crate) fn new() -> RuntimeContext {
    RuntimeContext {
      driver: RefCell::new(Handle::new(&builder()).expect("Internal error")),
    }
  }

  pub(crate) fn handle(&self) -> Handle {
    self.driver.borrow().clone()
  }
}

//SAFETY: RuntimeContext is only accessed via thread_local
unsafe impl Send for RuntimeContext {}
unsafe impl Sync for RuntimeContext {}