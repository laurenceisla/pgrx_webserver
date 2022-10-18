mod web_server_sync;
mod web_server_async;

use std::net::TcpListener;
use pgx::*;
use pgx::datum::{FromDatum, IntoDatum};
use pgx::log;
use pgx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};

// Tutorial: https://github.com/tcdi/pgx/blob/master/pgx-examples/bgworker/src/lib.rs
// TODO: Verify the --bgworker argument for `cargo pgx new`
#[pg_guard]
pub extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Background Web Service")
        .set_function("run_service_sync")
        .set_library("pgx_demo")
        .set_argument(42i32.into_datum())
        .enable_spi_access()
        .load();
}

pg_module_magic!();

#[pg_extern]
fn hello_pgx_demo() -> &'static str {
    "Hello, pgx_demo"
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn run_service_sync(arg: pg_sys::Datum) {
    let arg = unsafe { i32::from_datum(arg, false, pg_sys::INT4OID) };

    // From the tutorial:
    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // From the tutorial:
    // we want to be able to use SPI against the specified database (postgres), as the superuser which
    // did the initdb. You can specify a specific user with Some("my_user")
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!(
        "Hello from inside the {} BGWorker!  Argument value={}",
        BackgroundWorker::get_name(),
        arg.unwrap()
    );

    let listener = TcpListener::bind("127.0.0.1:9000").unwrap();

    for stream in listener.incoming() {
        // Currently this is blocking, a SIGTERM will not stop the worker
        // until another request is made.
        // Possible solutions:
        // https://www.zupzup.org/epoll-with-rust/
        // https://doc.rust-lang.org/std/net/struct.TcpListener.html#method.set_nonblocking
        // Use tokio
        if BackgroundWorker::sigterm_received() {
            break;
        }

        let stream = stream.unwrap();

        web_server_sync::handle_connection(stream).expect("TODO: panic message");
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn run_service_async() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    web_server_async::handle_connection().expect("TODO: panic message");
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_pgx_demo() {
        assert_eq!("Hello, pgx_demo", crate::hello_pgx_demo());
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
