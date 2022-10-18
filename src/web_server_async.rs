use std::panic;
use std::panic::AssertUnwindSafe;
use tokio::runtime::Builder;
use tokio::net::TcpListener;
use tokio::io::{BufReader, AsyncBufReadExt, AsyncWriteExt};

use httparse::Request;
use httparse::Status::{Complete, Partial};
use pgx::bgworkers::BackgroundWorker;

use pgx::log;
use pgx::Spi;

pub fn handle_connection() -> Result<(), Box<dyn std::error::Error>> {
    // new_current_thread starts the Tokio runtime in a single thread
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()?;

    // Run a Tokio future on the current thread
    rt.block_on(async {

        let listener = TcpListener::bind("127.0.0.1:9000").await?;

        loop {
            // This still awaits for a request call to close the server.
            // May need another background worker that listens to signals and shuts down or
            // maybe it's possible in the same worker?
            // See: https://tokio.rs/tokio/topics/shutdown
            // TODO: implement graceful shutdown
            if pgx::bgworkers::BackgroundWorker::sigterm_received() {
                return Ok(());
            }

            let (mut stream, _) = listener.accept().await?;

            // This allows many tasks to run concurrently
            // It SHOULD run them in the same thread ('cause the runtime is started in a single thread)
            tokio::spawn(async move {
                const NUM_OF_HEADERS: usize = 16;

                let mut headers = [httparse::EMPTY_HEADER; NUM_OF_HEADERS];
                let mut request = Request::new(&mut headers);

                let mut buf_reader = BufReader::new(&mut stream);

                let received = buf_reader.fill_buf().await.unwrap();

                let result = request.parse(received);

                match result {
                    Ok(Complete(_n)) => {
                        log!("Complete request.")
                    },
                    Ok(Partial) => {
                        log!("Partial request.");
                    },
                    Err(_e) => {
                        log!("Error parsing the request.");
                        // TODO: Add more info about the error
                    }
                }

                let method = request.method.unwrap();
                let path = request.path.unwrap();

                // Message that should be returned from PostgreSQL.
                // Using &str instead of String returns the content but it returns 7F (DEL) code for each character.
                let mut message = "Could not access PostgreSQL".to_string();
                // Wrapper to allow UnwindSafe inside BackgroundWorker.
                // See: https://doc.rust-lang.org/stable/std/panic/struct.AssertUnwindSafe.html
                // See: https://stackoverflow.com/questions/65762689/how-can-assertunwindsafe-be-used-with-the-catchunwind-future
                // TODO: Check if this is safe.
                let mut wrapper = AssertUnwindSafe(&mut message);

                // Note: it DOES work with static paths to the webpages.
                // TODO: find a way to get the dir path with pgx (it returns the path of the pg storage)
                let (resp_status, query) =
                    if method == "GET" && path == "/" {
                        // Hardcoding response
                        // ("HTTP/1.1 200 OK", "<h1>Hello world!</h1>")
                        // Using SPI
                        ("HTTP/1.1 200 OK", "SELECT '<h1>Hello from PostgreSQL!</h1>' AS message;")
                    }
                    else {
                        // Hardcoding response
                        // ("HTTP/1.1 400 NOT FOUND", "<h1>404 Not Found</h1>")
                        // Using SPI
                        ("HTTP/1.1 400 NOT FOUND","SELECT '<h1>404 Not Found from PostgreSQL!</h1>' AS message;")
                    };

                // The BackgroundWorker needs to open a transaction in order to call an Spi
                // See: https://github.com/tcdi/pgx/tree/master/pgx-examples/bgworker
                BackgroundWorker::transaction(|| {
                    panic::catch_unwind(move || {
                        **wrapper = Spi::get_one::<String>(query).expect("NULL");
                    }).expect("TODO: panic message");
                });

                let buf_length = received.len();
                buf_reader.consume(buf_length);

                let contents = message;

                let content_length = contents.len();

                let response = format!(
                    "{resp_status}\r\nContent-Length: {content_length}\r\n\r\n{contents}"
                );

                stream.write_all(response.as_bytes()).await.unwrap();
            });
        }
    })
}
