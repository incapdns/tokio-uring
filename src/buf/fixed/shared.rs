use crate::buf::fixed::handle::CheckedOutBuf;
use crate::buf::fixed::{FixedBuf, FixedBuffers};
use crate::runtime::CONTEXT;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;

pub(crate) fn process(
  this: Rc<RefCell<dyn FixedBuffers>>,
  cob: Option<CheckedOutBuf>,
) -> Option<FixedBuf> {
  cob.map(|data| {
    // Safety: the validity of buffer data is ensured by
    // plumbing::Pool::try_next
    unsafe { FixedBuf::new(this, data) }
  })
}

pub(crate) fn register(this: Rc<RefCell<dyn FixedBuffers>>) -> io::Result<()> {
  CONTEXT.with(|x| x.handle().register_buffers(this))
}

pub(crate) fn unregister() -> io::Result<()> {
  CONTEXT.with(|x| x.handle().unregister_buffers())
}
