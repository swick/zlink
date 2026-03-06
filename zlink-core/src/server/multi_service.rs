use core::future::Future;

use crate::{Connection, ReadyListener, Server};
use futures_util::{
    stream::{FuturesUnordered, StreamExt},
    FutureExt,
};

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
    fn accept(
        &self,
        socket: &mut Connection<Listener::Socket>,
    ) -> impl Future<Output = Option<Service>>;

    /// runs the thing
    fn run(
        &mut self,
        mut listener: Listener,
    ) -> impl Future<Output = crate::Result<()>> {
        async move {
            let mut run_futures = FuturesUnordered::new();

            loop {
                futures_util::select_biased! {
                    conn = listener.accept().fuse() => {
                        let mut conn = conn?;
                        if let Some(service) = self.accept(&mut conn).await {
                            let listener = ReadyListener::<Listener::Socket>::new(conn);
                            let server = Server::new(listener, service);
                            run_futures.push(server.run());
                        }
                    },
                    _ = run_futures.next().fuse() => {
                        // A server future completed, just continue
                    },
                }
            }
        }
    }
}
