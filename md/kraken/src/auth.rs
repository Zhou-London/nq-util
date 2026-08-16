//! Kraken Spot REST authentication and websocket token retrieval.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256, Sha512};

const REST_BASE: &str = "https://api.kraken.com";
const TOKEN_PATH: &str = "/0/private/GetWebSocketsToken";

/// An API key pair. The secret is held decoded and is never logged; the type
/// deliberately implements neither `Debug` nor `Display`.
pub struct Credentials {
    key: String,
    secret: Vec<u8>,
}

impl Credentials {
    /// Reads `KRAKEN_API_KEY` and `KRAKEN_API_SECRET`, falling back to a
    /// `key = value` file at `KRAKEN_CREDENTIALS` or
    /// `~/.config/kraken/credentials`.
    pub fn load() -> Result<Self> {
        let (key, secret) = match (
            std::env::var("KRAKEN_API_KEY"),
            std::env::var("KRAKEN_API_SECRET"),
        ) {
            (Ok(key), Ok(secret)) => (key, secret),
            _ => Self::from_file()?,
        };
        let secret = BASE64
            .decode(secret.trim())
            .context("api secret is not valid base64")?;
        if key.is_empty() || secret.is_empty() {
            bail!("api key or secret is empty");
        }
        Ok(Self {
            key: key.trim().to_owned(),
            secret,
        })
    }

    fn from_file() -> Result<(String, String)> {
        let path = Self::credentials_path()?;
        let text = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "set KRAKEN_API_KEY and KRAKEN_API_SECRET, or write them to {}",
                path.display()
            )
        })?;

        let mut key = None;
        let mut secret = None;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, value)) = line.split_once('=') else {
                continue;
            };
            match name.trim() {
                "key" => key = Some(value.trim().to_owned()),
                "secret" => secret = Some(value.trim().to_owned()),
                _ => {}
            }
        }

        match (key, secret) {
            (Some(key), Some(secret)) => Ok((key, secret)),
            _ => bail!("{} must define both `key` and `secret`", path.display()),
        }
    }

    fn credentials_path() -> Result<PathBuf> {
        if let Ok(path) = std::env::var("KRAKEN_CREDENTIALS") {
            return Ok(PathBuf::from(path));
        }
        let home = std::env::var("HOME").context("HOME is not set")?;
        Ok(PathBuf::from(home).join(".config/kraken/credentials"))
    }

    /// `API-Sign`: base64(HMAC-SHA512(uri_path || SHA256(nonce || body), secret)).
    fn sign(&self, uri_path: &str, nonce: u64, body: &str) -> String {
        let mut sha = Sha256::new();
        sha.update(nonce.to_string().as_bytes());
        sha.update(body.as_bytes());
        let payload = sha.finalize();

        let mut mac = Hmac::<Sha512>::new_from_slice(&self.secret)
            .expect("hmac-sha512 accepts a key of any length");
        mac.update(uri_path.as_bytes());
        mac.update(&payload);
        BASE64.encode(mac.finalize().into_bytes())
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    #[serde(default)]
    error: Vec<String>,
    result: Option<TokenResult>,
}

#[derive(Deserialize)]
struct TokenResult {
    token: String,
}

/// Fetches a websockets token. The token must be used within 15 minutes but
/// does not expire once a subscription on it is established.
pub async fn websockets_token(http: &reqwest::Client, creds: &Credentials) -> Result<String> {
    // Kraken requires a strictly increasing nonce per key; milliseconds since
    // the epoch is monotonic enough for a single-process client.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the unix epoch")
        .as_millis() as u64;
    let body = format!("{{\"nonce\":{nonce}}}");
    let signature = creds.sign(TOKEN_PATH, nonce, &body);

    let response = http
        .post(format!("{REST_BASE}{TOKEN_PATH}"))
        .header("API-Key", &creds.key)
        .header("API-Sign", signature)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .context("GetWebSocketsToken request failed")?
        .error_for_status()
        .context("GetWebSocketsToken returned an http error")?
        .json::<TokenResponse>()
        .await
        .context("GetWebSocketsToken returned malformed json")?;

    if !response.error.is_empty() {
        bail!("GetWebSocketsToken: {}", response.error.join(", "));
    }
    response
        .result
        .map(|result| result.token)
        .context("GetWebSocketsToken returned no token")
}
