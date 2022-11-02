extern crate core;

mod web_server_sync;
mod web_server_async;
mod auth;

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
        .set_function("run_service_async")
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

        web_server_sync::handle_connection(stream).expect("TODO: panic message sync webserver");
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn run_service_async() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    web_server_async::handle_connection().expect("TODO: panic message async webserver");
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::time::Duration;
    use curl::easy::{Easy, List};

    use pgx::*;

    #[pg_test]
    fn test_hello_pgx_demo() {
        assert_eq!("Hello, pgx_demo", crate::hello_pgx_demo());
    }

    // Test examples taken from https://github.com/alexcrichton/curl-rust/blob/main/tests/easy.rs
    macro_rules! t {
        ($e:expr) => {
            match $e {
                Ok(e) => e,
                Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
            }
        };
    }

    fn handle() -> Easy {
        let mut e = Easy::new();
        t!(e.timeout(Duration::new(20,0)));
        e
    }

    // TODO: simplify these tests.
    #[pg_test]
    fn get_root() {
        let mut all = Vec::<u8>::new();
        {
            let mut handle = handle();
            t!(handle.url("http://127.0.0.1:9000/"));
            let mut handle = handle.transfer();
            t!(handle.write_function(|data| {
                all.extend(data);
                Ok(data.len())
            }));
            t!(handle.perform());
        }
        assert_eq!(all, b"<h1>Hello from PostgreSQL!</h1>");
    }

    #[pg_test]
    fn get_protected_if_authorized() {
        let mut all = Vec::<u8>::new();
        {
            let mut handle = handle();
            t!(handle.url("http://127.0.0.1:9000/authorized"));
            let mut list = List::new();
            list.append("Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoid2ViX3VzZXIiLCJhdWQiOiJhdWRpZW5jZSIsImV4cCI6MTY3NzkxMzM2MX0.dpsT6rHeH3AQxu9tmb2DBgN8wt27GSUD7y1HWtAwgCk").unwrap();
            t!(handle.http_headers(list));

            let mut transfer = handle.transfer();
            t!(transfer.write_function(|data| {
                all.extend(data);
                Ok(data.len())
            }));
            t!(transfer.perform());
        }
        assert_eq!(all, b"<h1>Hello from PostgreSQL, authenticated user!</h1>");
    }

    #[pg_test]
    fn get_protected_if_unauthorized() {
        let mut all = Vec::<u8>::new();
        {
            let mut handle = handle();
            t!(handle.url("http://127.0.0.1:9000/authorized"));
            let mut list = List::new();
            list.append("Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoid2ViX2Fub24iLCJhdWQiOiJhdWRpZW5jZSIsImV4cCI6MTY3NzkxMzM2MX0.GxxshNhfl_2D7HpNReFkEcduQfv3Y5DbCA9ZzWyWBUM").unwrap();
            t!(handle.http_headers(list));

            let mut transfer = handle.transfer();
            t!(transfer.write_function(|data| {
                all.extend(data);
                Ok(data.len())
            }));
            t!(transfer.perform());
        }
        assert_eq!(all, b"<h1>You are not allowed to see this page!</h1>");
    }

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        // - "shared_preload_libraries" needed for the background worker
        vec!["shared_preload_libraries = 'pgx_demo.so'"]
    }
}
