use std::future::Future;

use crate::{Connection, ReadyListener, Server, server::SelectAll};
use futures_util::FutureExt;

/// A trait for accepting incoming connections and creating services dynamically.
///
/// This trait allows custom logic to decide whether to accept a connection and which
/// service instance to use for handling it.
pub trait MultiService<Service, Listener>
where
    Service: crate::Service<Listener::Socket>,
    Listener: crate::Listener,
{
    /// Accept a connection and return a service to handle it.
    ///
    /// Returns `Some(service)` if the connection should be accepted, or `None` to reject it.
    fn accept(&self, socket: &mut Connection<Listener::Socket>) -> impl Future<Output = Option<Service>>;

    /// runs the thing
    fn run(&mut self, mut listener: Listener) -> impl std::future::Future<Output = crate::Result<()>> { async move {
        let mut run_futures = Vec::new();
        let mut new_run_futures = Vec::new();
        let mut finished_run_future_idx = None;

        loop {
            if let Some(idx) = finished_run_future_idx.take() {
                let _ = run_futures.remove(idx);
            }
            run_futures.append(&mut new_run_futures);

            let mut run_select_all = SelectAll::new(None);

            for future in run_futures.iter_mut() {
                // SAFETY: I think this is fine because run_futures is not mutated below
                unsafe {
                    run_select_all.push_unchecked(future);
                }
            }

            futures_util::select_biased! {
                conn = listener.accept().fuse() => {
                    let mut conn = conn?;
                    if let Some(service) = self.accept(&mut conn).await {
                        let listener = ReadyListener::<Listener::Socket>::new(conn);
                        let server = Server::new(listener, service);
                        new_run_futures.push(server.run());
                    }
                },
                (idx, _) = run_select_all.fuse() => {
                    finished_run_future_idx = Some(idx);
                },
            }
        }
    } }
}