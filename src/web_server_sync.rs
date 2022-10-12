use std::{
    // fs,
    io::{prelude::*},
    net::{TcpStream},
};
use std::io::{BufReader,BufRead};
use httparse::Status::{Complete, Partial};
use pgx::log;

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

    // Note: it DOES work with static paths to the webpages.
    // TODO: find a way to get the dir path with pgx (it returns the path of the pg storage)
    let (status, filename) =
        if method == "GET" && path == "/" {
            ("HTTP/1.1 200 OK", "<h1>Hello world!</h1>")
            // ("HTTP/1.1 200 OK", "/path/to/pgx_demo/web/html/index.html")
        }
        else {
            ("HTTP/1.1 400 NOT FOUND", "<h1>404 Not Found</h1>")
            // ("HTTP/1.1 400 OK", "/path/to/pgx_demo/web/html/404.html")
        };

    let length = received.len();
    buf_reader.consume(length);

    // let contents = fs::read_to_string(filename).unwrap();
    let contents = filename;
    let length = contents.len();

    let response = format!(
        "{status}\r\nContent-Length: {length}\r\n\r\n{contents}"
    );

    stream.write_all(response.as_bytes()).unwrap();

    Ok(())
}
