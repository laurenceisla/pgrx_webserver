use std::{io::{prelude::*}, net::{TcpStream}, panic};
use std::io::{BufReader, BufRead};
use std::panic::AssertUnwindSafe;
use httparse::Status::{Complete, Partial};
use pgx::bgworkers::BackgroundWorker;
use pgx::log;
use pgx::Spi;

pub fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {

    const NUM_OF_HEADERS: usize = 16;

    let mut headers = [httparse::EMPTY_HEADER; NUM_OF_HEADERS];
    let mut request = httparse::Request::new(&mut headers);

    let mut buf_reader = BufReader::new(&mut stream);

    let received = buf_reader.fill_buf().unwrap();

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
    let (status, query) =
        if method == "GET" && path == "/" {
            // Hardcoding response
            // ("HTTP/1.1 200 OK", "<h1>Hello world!</h1>")
            // Using files
            // ("HTTP/1.1 200 OK", "/path/to/pgx_demo/web/html/index.html")
            // Using SPI
            ("HTTP/1.1 200 OK", "SELECT '<h1>Hello from PostgreSQL!</h1>' AS message;")
        }
        else {
            // Hardcoding response
            // ("HTTP/1.1 400 NOT FOUND", "<h1>404 Not Found</h1>")
            // Using files
            // ("HTTP/1.1 400 NOT FOUND", "/path/to/pgx_demo/web/html/404.html")
            // Using SPI
            ("HTTP/1.1 400 NOT FOUND","SELECT '<h1>404 Not Found from PostgreSQL!</h1>' AS message;")
        };

    // The BackgroundWorker needs to open a transaction in order to call an Spi
    // See: https://github.com/tcdi/pgx/tree/master/pgx-examples/bgworker
    // TODO: this should be async (?)
    BackgroundWorker::transaction(|| {
        panic::catch_unwind(move || {
            **wrapper = Spi::get_one::<String>(query).expect("NULL");
        }).expect("TODO: panic message");
    });

    let length = received.len();
    buf_reader.consume(length);

    // let contents = fs::read_to_string(filename).unwrap();
    // let contents = filename;
    // let contents = "Hi";
    let contents = message;

    let length = contents.len();

    let response = format!(
        "{status}\r\nContent-Length: {length}\r\n\r\n{contents}"
    );

    stream.write_all(response.as_bytes()).unwrap();

    Ok(())
}
