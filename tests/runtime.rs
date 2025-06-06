use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

#[test]
fn use_tokio_types_from_runtime() {
  tokio_uring::start(async {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let task = tokio::spawn(async move {
      let _socket = TcpStream::connect(addr).await.unwrap();
    });

    // Accept a connection
    let (_socket, _) = listener.accept().await.unwrap();

    // Wait for the task to complete
    task.await.unwrap();
  });
}

#[test]
fn spawn_a_task() {
  use std::cell::RefCell;
  use std::rc::Rc;

  tokio_uring::start(async {
    let cell = Rc::new(RefCell::new(1));
    let c = cell.clone();
    let mut js = JoinSet::new();

    js.spawn_local(async move {
      *c.borrow_mut() = 2;
    });

    js.join_all().await;
    assert_eq!(2, *cell.borrow());
  });
}
