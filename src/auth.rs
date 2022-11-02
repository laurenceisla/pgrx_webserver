use jsonwebtoken::{decode, errors, Algorithm, Validation, DecodingKey};

use serde_json::Value;

use std::collections::{HashMap, HashSet};
use std::str::from_utf8;

// Should come from the configuration file
const JWT_SECRET: &[u8] = b"reallyreallyreallyreallyverysafe";
const JWT_AUDIENCE: &str = "audience";

// This should be the role that executes the query after authentication (SET ROLE x;)
const AUTH_ROLE: &str = "web_user";

// pub struct Claims(HashMap<String,Value>);

fn extract_bearer_auth(bearer: &[u8]) -> &str {
    let mut iter = bearer.splitn(2,|x| x == &b' ');

    // TODO: Handle from_utf8(), it allows ascii data.
    if iter.next().unwrap().to_ascii_lowercase() == b"bearer" {
        from_utf8(iter.next().unwrap_or(b"")).unwrap_or("")
    } else {
        ""
    }
}

// Parses the token and returns the claims as a HashMap of JSON values
fn parse_token(jwt: &str) -> Result<HashMap<String,Value>, String> {
    // TODO: make it work with RSA
    let mut validation = Validation::new(Algorithm::HS256);
    // Removes "exp" from required claims (still validates if it's present)
    validation.required_spec_claims = HashSet::new();
    // TODO: Verify if PostgREST accepts jwt without aud even if jwt-aud is set
    validation.set_audience(&[JWT_AUDIENCE]);

    match decode::<HashMap<String, Value>> (
        jwt,
        &DecodingKey::from_secret(JWT_SECRET),
        &validation
    ) {
        Ok(data) => Ok(data.claims),
        Err(e) => match *e.kind() {
            errors::ErrorKind::ExpiredSignature => Err("JWT expired".to_string()),
            _ => Err(format!("Invalid token: {:?}. Token: {}", e.kind(), jwt))
        }
    }
}

// Simple authentication.
pub fn authenticate(bearer_token: Option<&[u8]>) -> Result<Option<HashMap<String,Value>>, String> {
    match bearer_token {
        Some(bt) => match parse_token(extract_bearer_auth(bt)) {
            Ok(claims) => Ok(Some(claims)),
            Err(e) => Err(e)
        },
        None => Ok(None)
    }
}

// A simple authorization
pub fn is_authorized(claims: HashMap<String,Value>) -> bool {
    match claims["role"].as_str() {
        Some(AUTH_ROLE) => true,
        _ => false
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
// }
