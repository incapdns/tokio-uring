use crate::runtime::driver::{Handle, WeakHandle};
use std::cell::{Cell, RefCell};

/// Owns the driver and resides in thread-local storage.
pub(crate) struct RuntimeContext {
  driver: RefCell<Option<Handle>>,
  on_thread_park: Cell<fn()>
}

fn noop(){}

impl RuntimeContext {
  /// Construct the context with an uninitialized driver.
  pub(crate) fn new() -> RuntimeContext {
    RuntimeContext {
      driver: RefCell::new(None),
      on_thread_park: Cell::new(noop),
    }
  }

  pub(crate) fn handle(&self) -> Handle {
    self.driver.borrow().clone().unwrap()
  }

  #[inline(always)]
  pub(crate) fn call_on_thread_park(&self) {
    self.on_thread_park.get()();
  }

  pub(crate) fn set_on_thread_park(&self, callback: fn()) {
    self.on_thread_park.set(callback)
  }

  /// Initialize the driver.
  pub(crate) fn set_handle(&self, handle: Handle) {
    let mut guard = self.driver.borrow_mut();

    assert!(guard.is_none(), "Attempted to initialize the driver twice");

    *guard = Some(handle);
  }

  pub(crate) fn unset_driver(&self) {
    let mut guard = self.driver.borrow_mut();

    assert!(guard.is_some(), "Attempted to clear nonexistent driver");

    *guard = None;
  }

  /// Check if driver is initialized
  #[allow(dead_code)]
  pub(crate) fn is_set(&self) -> bool {
    self.driver
      .try_borrow()
      .map(|b| b.is_some())
      .unwrap_or(false)
  }

  #[allow(dead_code)]
  pub(crate) fn weak(&self) -> Option<WeakHandle> {
    self.driver.borrow().as_ref().map(Into::into)
  }
}

impl Drop for RuntimeContext {
  fn drop(&mut self) {
    self.unset_driver();
  }
}

//SAFETY: RuntimeContext is only accessed via thread_local
unsafe impl Send for RuntimeContext {}
unsafe impl Sync for RuntimeContext {}
