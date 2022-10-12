use tokio::runtime::Builder;
use tokio::net::TcpListener;
use tokio::io::{BufReader, AsyncBufReadExt, AsyncWriteExt};

use httparse::Request;
use httparse::Status::{Complete, Partial};

use pgx::log;

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

                let (status, filename) =
                    if method == "GET" && path == "/" {
                        ("HTTP/1.1 200 OK", "<h1>Hello world!</h1>")
                    }
                    else {
                        ("HTTP/1.1 400 NOT FOUND", "<h1>404 Not Found</h1>")
                    };

                let length = received.len();
                buf_reader.consume(length);

                let contents = filename;
                let length = contents.len();

                let response = format!(
                    "{status}\r\nContent-Length: {length}\r\n\r\n{contents}"
                );

                stream.write_all(response.as_bytes()).await.unwrap();
            });
        }
    })
}