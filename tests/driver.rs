use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tempfile::NamedTempFile;

use tokio_uring::{buf::IoBuf, fs::File};

#[path = "../src/future.rs"]
#[allow(warnings)]
mod future;

#[test]
fn complete_ops_on_drop() {
  use std::sync::Arc;

  struct MyBuf {
    data: Vec<u8>,
    _ref_cnt: Arc<()>,
  }

  unsafe impl IoBuf for MyBuf {
    fn stable_ptr(&self) -> *const u8 {
      self.data.stable_ptr()
    }

    fn bytes_init(&self) -> usize {
      self.data.bytes_init()
    }

    fn bytes_total(&self) -> usize {
      self.data.bytes_total()
    }
  }

  unsafe impl tokio_uring::buf::IoBufMut for MyBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
      self.data.stable_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
      self.data.set_init(pos);
    }
  }

  // Used to test if the buffer dropped.
  let ref_cnt = Arc::new(());

  let tempfile = tempfile();

  let vec = vec![0; 50 * 1024 * 1024];
  let mut file = std::fs::File::create(tempfile.path()).unwrap();
  std::io::Write::write_all(&mut file, &vec).unwrap();

  let file = tokio_uring::start(async {
    let file = File::create(tempfile.path()).await.unwrap();
    poll_once(async {
      file
        .read_at(
          MyBuf {
            data: vec![0; 64 * 1024],
            _ref_cnt: ref_cnt.clone(),
          },
          25 * 1024 * 1024,
        )
        .await
        .0
        .unwrap();
    })
    .await;

    // It's important, because the primary future will not release
    // after the result of block_on, since shutdown does not guarantee
    // the instant release of resources after the end of mt runtime
    // to release resources due to multi thread environment
    tokio_uring::no_op().await.expect("Not in runtime?");

    file
  });

  assert_eq!(Arc::strong_count(&ref_cnt), 1);

  // little sleep
  std::thread::sleep(std::time::Duration::from_millis(100));

  drop(file);
}

#[test]
fn too_many_submissions() {
  let tempfile = tempfile();

  tokio_uring::start(async {
    for _ in 0..600 {
      let file = File::create(tempfile.path()).await.unwrap();
      poll_once(async {
        file
          .write_at(b"hello world".to_vec(), 0)
          .submit()
          .await
          .0
          .unwrap();
      })
      .await;
      tokio_uring::fs::remove_file(tempfile.path())
        .await
        .expect("Internal Error");
    }
  });
}

#[test]
fn completion_overflow_lightweight() {
  let mut builder = tokio_uring::uring_builder();
  let builder = builder.setup_cqsize(400);
  internal_completion_overflow(false, 20000, 200, builder.clone());
}

#[test]
fn completion_overflow() {
  let mut builder = tokio_uring::uring_builder();
  let builder = builder.setup_cqsize(4);
  internal_completion_overflow(true, 2000, 2, builder.clone());
}

fn internal_completion_overflow(
  has_timer: bool,
  spawn_cnt: u32,
  entries: u32,
  builder: io_uring::Builder,
) {
  use std::process;
  use std::{thread, time};
  use tokio::task::JoinSet;

  for _ in 1..20 {
    let total = Arc::new(AtomicU32::new(0));
    let total_rt = total.clone();

    if has_timer {
      thread::spawn(move || {
        thread::sleep(time::Duration::from_millis(1000)); // 1000 times longer than it takes on a slow machine
        let finished = total.load(atomic::Ordering::Acquire);
        if finished != (spawn_cnt * 2) {
          eprintln!(
            "Timeout reached ({}). The uring completions are hung.",
            finished
          );
          process::exit(1);
        }
      });
    }

    tokio_uring::builder()
      .entries(entries)
      .uring_builder(&builder)
      .start(async move {
        let mut js = JoinSet::new();

        for _ in 0..spawn_cnt {
          js.spawn_local(tokio_uring::no_op());
        }

        for _ in 0..spawn_cnt {
          js.spawn(tokio_uring::no_op());
        }

        while let Some(res) = js.join_next().await {
          let _ = res.unwrap().unwrap();
          total_rt.fetch_add(1, atomic::Ordering::Release);
        }
      });
  }
}

fn tempfile() -> NamedTempFile {
  NamedTempFile::new().unwrap()
}

async fn poll_once(future: impl std::future::Future) {
  // use std::future::Future;
  use std::task::Poll;
  use tokio::pin;

  pin!(future);

  std::future::poll_fn(|cx| {
    assert!(future.as_mut().poll(cx).is_pending());
    Poll::Ready(())
  })
  .await;
}
