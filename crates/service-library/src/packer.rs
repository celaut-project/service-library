//! A client of the packer service, to pack a service from another service.
//!
//! The gateway has no call to pack: `nodo pack` is a command of the operator. The packer is
//! a service like any other
//! ([`bee-rpc-protocol/celaut-packer-service`](https://github.com/bee-rpc-protocol/celaut-packer-service)):
//! a service starts it as a dependency and talks to it over HTTP on the address of its
//! instance (`uri`, `ip:port`), which is what this module does. The HTTP is the little that
//! the packer needs (one request, `Connection: close`, plain TCP to an instance of the same
//! node), so the library takes no HTTP crate and the `celaut` binary stays small.
//!
//! A project and a package go through files and never through memory: a zip and a `.bee`
//! can be large.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::package;
use crate::Error;

/// Time to open the connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Time for the calls that answer at once.
const SHORT_TIMEOUT: Duration = Duration::from_secs(30);
/// Time to upload a dependency.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);
/// The most that is read of an answer that is not a file.
const MAX_SMALL_BODY: usize = 64 * 1024;
/// The longest line of a head.
const MAX_LINE: usize = 16 * 1024;

/// A package that the packer built.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packed {
    /// The id of the service, from the header `X-Service-Id`, in lowercase hex. It is the
    /// id that the file names: [`Packer::pack`] checked it.
    pub service_id: String,
    /// The `.celaut.bee` file that was written.
    pub path: PathBuf,
    /// Its size in bytes.
    pub size: u64,
}

/// The packer service at the address of an instance of it.
#[derive(Debug, Clone)]
pub struct Packer {
    uri: String,
    pack_timeout: Option<Duration>,
}

impl Packer {
    /// The packer at `uri`, as `ip:port` (a scheme `http://` and a final `/` are accepted).
    /// A build can take minutes, so [`Packer::pack`] has no time limit unless you set one
    /// with [`Packer::with_pack_timeout`].
    pub fn new(uri: &str) -> Self {
        let uri = uri.strip_prefix("http://").unwrap_or(uri);
        Packer {
            uri: uri.trim_end_matches('/').to_string(),
            pack_timeout: None,
        }
    }

    /// Limit the whole of a [`Packer::pack`] call (upload, build and download).
    pub fn with_pack_timeout(mut self, timeout: Duration) -> Self {
        self.pack_timeout = Some(timeout);
        self
    }

    /// `GET /health`: the packer is ready. An error that [`Error::is_unreachable`] says
    /// that the instance does not answer yet.
    pub async fn health(&self) -> Result<(), Error> {
        let answer = self
            .request("GET /health", "GET", "/health", None, None, Some(SHORT_TIMEOUT))
            .await?;
        answer.ok("GET /health")
    }

    /// `POST /pack`: send the zip at `project_zip` (`Dockerfile`, `service.json` and
    /// optionally `pack_config.json`) and write the package to `out`. The file is written
    /// as a partial file and renamed when it is complete, so `out` is never a cut file; and
    /// it is read back: the id that the file names must be the one that the packer said.
    ///
    /// The packer answers 415 (not a zip), 400 (the configuration or the packing is wrong,
    /// with a list of what to fix) and 413 (too large); each is an [`Error::Packer`] with
    /// the text of the answer.
    pub async fn pack(&self, project_zip: &Path, out: &Path) -> Result<Packed, Error> {
        let part = partial(out);
        let call = self.request(
            "POST /pack",
            "POST",
            "/pack",
            Some(("application/zip", project_zip)),
            Some(&part),
            self.pack_timeout,
        );
        let answer = match call.await {
            Ok(answer) => answer,
            Err(e) => {
                let _ = tokio::fs::remove_file(&part).await;
                return Err(e);
            }
        };
        if let Err(e) = answer.ok("POST /pack") {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(e);
        }
        let service_id = match answer.header("x-service-id") {
            Some(id) => id.trim().to_ascii_lowercase(),
            None => {
                let _ = tokio::fs::remove_file(&part).await;
                return Err(failed("POST /pack", "the answer has no X-Service-Id"));
            }
        };
        let info = match package::inspect(&part) {
            Ok(info) => info,
            Err(e) => {
                let _ = tokio::fs::remove_file(&part).await;
                return Err(failed("POST /pack", format!("the package is bad: {e}")));
            }
        };
        if hex::encode(info.service_id) != service_id {
            let _ = tokio::fs::remove_file(&part).await;
            return Err(failed(
                "POST /pack",
                format!(
                    "the packer said {service_id} and the package is {}",
                    hex::encode(info.service_id)
                ),
            ));
        }
        tokio::fs::rename(&part, out)
            .await
            .map_err(|e| failed("POST /pack", format!("{}: {e}", out.display())))?;
        let size = tokio::fs::metadata(out).await.map(|m| m.len()).unwrap_or(0);
        Ok(Packed {
            service_id,
            path: out.to_path_buf(),
            size,
        })
    }

    /// `GET /registry/<id>`: the packer already has this dependency.
    pub async fn has_dependency(&self, service_id: &str) -> Result<bool, Error> {
        let id = checked_id(service_id)?;
        let operation = format!("GET /registry/{id}");
        let answer = self
            .request(&operation, "GET", &format!("/registry/{id}"), None, None, Some(SHORT_TIMEOUT))
            .await?;
        match answer.status {
            404 => Ok(false),
            200 => {
                let body = String::from_utf8_lossy(&answer.small);
                // {"present": true}: the packer answers JSON and the library takes no parser
                // for one flag.
                Ok(body.replace(' ', "").contains("\"present\":true"))
            }
            _ => answer.ok(&operation).map(|_| false),
        }
    }

    /// `POST /registry/<id>`: give the packer a dependency of the project that is only in
    /// the registry of this node. `bundle` is a gzip tar with `service`, `metadata` (if
    /// any) and `blocks/<id>` (what `nodo pack` builds). The packer starts with an empty
    /// registry, so a project whose `pack_config.json` names a service by its id needs this.
    pub async fn upload_dependency(&self, service_id: &str, bundle: &Path) -> Result<(), Error> {
        let id = checked_id(service_id)?;
        let operation = format!("POST /registry/{id}");
        let answer = self
            .request(
                &operation,
                "POST",
                &format!("/registry/{id}"),
                Some(("application/gzip", bundle)),
                None,
                Some(UPLOAD_TIMEOUT),
            )
            .await?;
        answer.ok(&operation)
    }

    async fn request(
        &self,
        operation: &str,
        method: &str,
        path: &str,
        body: Option<(&str, &Path)>,
        sink: Option<&Path>,
        timeout: Option<Duration>,
    ) -> Result<Answer, Error> {
        let work = self.exchange(operation, method, path, body, sink);
        match timeout {
            Some(limit) => tokio::time::timeout(limit, work)
                .await
                .map_err(|_| unreachable(operation, "timed out"))?,
            None => work.await,
        }
    }

    async fn exchange(
        &self,
        operation: &str,
        method: &str,
        path: &str,
        body: Option<(&str, &Path)>,
        sink: Option<&Path>,
    ) -> Result<Answer, Error> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&self.uri))
            .await
            .map_err(|_| unreachable(operation, "timed out connecting"))?
            .map_err(|e| unreachable(operation, e.to_string()))?;
        let (read, mut write) = stream.into_split();

        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n", self.uri);
        let mut file = None;
        if let Some((content_type, source)) = body {
            let f = File::open(source)
                .await
                .map_err(|e| failed(operation, format!("{}: {e}", source.display())))?;
            let length = f
                .metadata()
                .await
                .map_err(|e| failed(operation, format!("{}: {e}", source.display())))?
                .len();
            head.push_str(&format!("Content-Type: {content_type}\r\nContent-Length: {length}\r\n"));
            file = Some(f);
        } else if method == "POST" {
            head.push_str("Content-Length: 0\r\n");
        }
        head.push_str("\r\n");

        // The upload and the answer go together: a packer that refuses a file that is too
        // large (413) answers before it has read it all.
        let send = async {
            write.write_all(head.as_bytes()).await?;
            if let Some(mut f) = file {
                tokio::io::copy(&mut f, &mut write).await?;
            }
            write.flush().await
        };
        let receive = read_answer(BufReader::new(read), sink);
        tokio::pin!(send);
        tokio::pin!(receive);
        let mut sent = false;
        let answer = loop {
            tokio::select! {
                done = &mut send, if !sent => {
                    // A broken pipe after an early answer is not the error: the answer says why.
                    if let Err(e) = done {
                        match tokio::time::timeout(Duration::from_millis(500), &mut receive).await {
                            Ok(Ok(answer)) => break answer,
                            _ => return Err(unreachable(operation, e.to_string())),
                        }
                    }
                    sent = true;
                }
                answer = &mut receive => break answer.map_err(|e| match e {
                    Read::Io(m) => unreachable(operation, m),
                    Read::Bad(m) => failed(operation, m),
                })?,
            }
        };
        Ok(answer)
    }
}

/// The path that a package is written to before it is complete.
fn partial(out: &Path) -> PathBuf {
    let mut name = out.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(".part");
    out.with_file_name(name)
}

/// An id in the path of a request: hex only, so it cannot change the request.
fn checked_id(id: &str) -> Result<String, Error> {
    let id = id.trim().to_ascii_lowercase();
    if id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(id)
    } else {
        Err(Error::Protocol(format!("{id:?} is not a service id (64 hex digits)")))
    }
}

fn failed(operation: &str, message: impl Into<String>) -> Error {
    Error::Packer {
        operation: operation.to_string(),
        status: None,
        unreachable: false,
        message: message.into(),
    }
}

fn unreachable(operation: &str, message: impl Into<String>) -> Error {
    Error::Packer {
        operation: operation.to_string(),
        status: None,
        unreachable: true,
        message: message.into(),
    }
}

enum Read {
    /// The connection failed.
    Io(String),
    /// The packer did not speak HTTP.
    Bad(String),
}

impl From<std::io::Error> for Read {
    fn from(e: std::io::Error) -> Self {
        Read::Io(e.to_string())
    }
}

/// What the packer answered. A body goes to the file when the status is 200 and a file was
/// asked for; otherwise it is kept (up to 64 KiB) in `small`.
struct Answer {
    status: u16,
    headers: Vec<(String, String)>,
    small: Vec<u8>,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn ok(&self, operation: &str) -> Result<(), Error> {
        if self.status == 200 {
            return Ok(());
        }
        Err(Error::Packer {
            operation: operation.to_string(),
            status: Some(self.status),
            unreachable: false,
            message: String::from_utf8_lossy(&self.small).trim().to_string(),
        })
    }
}

async fn read_line<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Result<String, Read> {
    let mut line = Vec::new();
    let n = reader
        .take(MAX_LINE as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if n == 0 || line.last() != Some(&b'\n') {
        return Err(Read::Io("the connection closed in the head".into()));
    }
    Ok(String::from_utf8_lossy(&line).trim_end().to_string())
}

async fn read_answer<R: AsyncBufReadExt + Unpin>(
    mut reader: R,
    sink: Option<&Path>,
) -> Result<Answer, Read> {
    let status_line = read_line(&mut reader).await?;
    let status = status_line
        .strip_prefix("HTTP/1.")
        .and_then(|r| r.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| Read::Bad(format!("not an HTTP status line: {status_line:?}")))?;
    let mut headers = Vec::new();
    loop {
        let line = read_line(&mut reader).await?;
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    let find = |name: &str| headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str());
    let chunked = find("transfer-encoding").is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    let length = find("content-length").and_then(|v| v.parse::<u64>().ok());

    let mut out: Option<File> = None;
    if status == 200 {
        if let Some(path) = sink {
            out = Some(
                File::create(path)
                    .await
                    .map_err(|e| Read::Bad(format!("{}: {e}", path.display())))?,
            );
        }
    }
    let mut small = Vec::new();
    async fn emit(
        out: &mut Option<File>,
        small: &mut Vec<u8>,
        data: &[u8],
    ) -> Result<(), Read> {
        match out {
            Some(file) => file.write_all(data).await?,
            None => {
                // The rest of a long error is dropped.
                let room = MAX_SMALL_BODY.saturating_sub(small.len());
                small.extend_from_slice(&data[..data.len().min(room)]);
            }
        }
        Ok(())
    }

    let mut buf = vec![0u8; 64 * 1024];
    if chunked {
        loop {
            let size_line = read_line(&mut reader).await?;
            let digits = size_line.split(';').next().unwrap_or("").trim();
            let mut left = u64::from_str_radix(digits, 16)
                .map_err(|_| Read::Bad(format!("bad chunk size {size_line:?}")))?;
            if left == 0 {
                // The trailers, up to the empty line.
                while !read_line(&mut reader).await?.is_empty() {}
                break;
            }
            while left > 0 {
                let want = buf.len().min(left as usize);
                let n = reader.read(&mut buf[..want]).await?;
                if n == 0 {
                    return Err(Read::Io("the connection closed in a chunk".into()));
                }
                emit(&mut out, &mut small, &buf[..n]).await?;
                left -= n as u64;
            }
            read_line(&mut reader).await?; // the CRLF after the chunk
        }
    } else {
        let mut left = length;
        loop {
            let want = match left {
                Some(0) => break,
                Some(l) => buf.len().min(l as usize),
                None => buf.len(),
            };
            let n = reader.read(&mut buf[..want]).await?;
            if n == 0 {
                if left.is_some() {
                    return Err(Read::Io("the connection closed before the end of the body".into()));
                }
                break;
            }
            emit(&mut out, &mut small, &buf[..n]).await?;
            if let Some(l) = left.as_mut() {
                *l -= n as u64;
            }
        }
    }
    if let Some(mut file) = out {
        file.flush().await?;
    }
    Ok(Answer {
        status,
        headers,
        small,
    })
}
