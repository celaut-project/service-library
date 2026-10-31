//! A service as a `.bee` file, to start it on a node that does not have it.
//!
//! `nodo export` writes the Metadata at index 1 and the Service at index 2; the answer of
//! `GetService` has the hashes at 4, the Metadata at 5 and the Service at 6, which are the
//! indices of `StartService`. [`read`] gives the Buffers of a file for `StartService`, one
//! record at a time and never the whole file in memory: a service can be large.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use bee_rpc::bee_file::read_bee;
use bee_rpc::buffer::Buffer;
use celaut_proto::celaut::metadata::hash_tag::Hash;
use celaut_proto::celaut::Metadata;
use prost::Message;

use crate::gateway::SHA3_256_TYPE;
use crate::Error;

/// A part that is bigger than this, before the Service, is not a Metadata or a hash.
const MAX_HEAD_PART: usize = 16 * 1024 * 1024;

/// The indices of the parts of a `.bee` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// The indices of `StartService` and of `GetService`: 4 hashes, 5 Metadata, 6 Service.
    Gateway,
    /// The indices of `nodo export`: 1 Metadata, 2 Service.
    NodoExport,
}

impl Layout {
    fn metadata_index(self) -> i32 {
        match self {
            Layout::Gateway => 5,
            Layout::NodoExport => 1,
        }
    }

    fn service_index(self) -> i32 {
        match self {
            Layout::Gateway => 6,
            Layout::NodoExport => 2,
        }
    }

    /// The index that a node expects for a part that has `index` in a file of this layout.
    fn for_node(self, index: i32) -> i32 {
        match (self, index) {
            (Layout::NodoExport, 1) => 5,
            (Layout::NodoExport, 2) => 6,
            (_, other) => other,
        }
    }
}

/// What the head of a package says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageInfo {
    /// How the file numbers its parts.
    pub layout: Layout,
    /// The id of the service (SHA3-256): from the hashes or the Metadata of the file.
    pub service_id: [u8; 32],
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::Protocol(format!("the package is not valid: {}", reason.into()))
}

fn id_in_hash(hash: &Hash) -> Option<[u8; 32]> {
    if hash.r#type == SHA3_256_TYPE {
        hash.value.as_slice().try_into().ok()
    } else {
        None
    }
}

/// Read the head of the `.bee` file at `path`, up to the Service, and say its layout and the
/// id of the service. It reads no block of the Service. It does not check that the blocks
/// hash to the id: the node does.
pub fn inspect(path: &Path) -> Result<PackageInfo, Error> {
    let file = File::open(path).map_err(|e| invalid(format!("{}: {e}", path.display())))?;
    let mut layout = None;
    let mut service_id = None;
    // The part that is open: its index and bytes.
    let mut open: Option<(i32, Vec<u8>)> = None;
    for buffer in read_bee(BufReader::new(file)) {
        let buffer = buffer.map_err(|e| invalid(e.to_string()))?;
        if let Some(head) = &buffer.head {
            if open.is_none() {
                let detected = *layout.get_or_insert(match head.index {
                    1 | 2 => Layout::NodoExport,
                    _ => Layout::Gateway,
                });
                if head.index == detected.service_index() {
                    break;
                }
                open = Some((head.index, Vec::new()));
            }
        }
        if let Some((_, data)) = open.as_mut() {
            if buffer.block.is_some() {
                return Err(invalid("a block before the Service"));
            }
            if let Some(chunk) = &buffer.chunk {
                data.extend_from_slice(chunk);
                if data.len() > MAX_HEAD_PART {
                    return Err(invalid("a part before the Service is too large"));
                }
            }
        }
        if buffer.separator == Some(true) {
            if let (Some((index, data)), Some(l)) = (open.take(), layout) {
                if index == l.metadata_index() {
                    let metadata = Metadata::decode(data.as_slice())
                        .map_err(|e| invalid(format!("the Metadata: {e}")))?;
                    let found = metadata
                        .hashtag
                        .iter()
                        .flat_map(|t| t.hash.iter())
                        .find_map(id_in_hash);
                    service_id = service_id.or(found);
                } else if index == 4 {
                    if let Ok(hash) = Hash::decode(data.as_slice()) {
                        service_id = service_id.or(id_in_hash(&hash));
                    }
                }
            }
        }
    }
    let layout = layout.ok_or_else(|| invalid("the file is empty"))?;
    let service_id = service_id.ok_or_else(|| invalid("no SHA3-256 id of the service"))?;
    Ok(PackageInfo { layout, service_id })
}

/// The Buffers of the file at `path` for a node, with the indices of `StartService`. They
/// come from a task that reads the file one record at a time. A file error ends the stream
/// with an error, so the node gets markers that make it refuse a cut input.
pub fn read(
    path: PathBuf,
    layout: Layout,
) -> impl tokio_stream::Stream<Item = Result<Buffer, bee_rpc::Error>> + Send {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Buffer, bee_rpc::Error>>(4);
    tokio::task::spawn_blocking(move || {
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(e) => {
                let _ = tx.blocking_send(Err(e.into()));
                return;
            }
        };
        for item in read_bee(BufReader::new(file)) {
            let item = item.map(|mut buffer| {
                if let Some(head) = buffer.head.as_mut() {
                    head.index = layout.for_node(head.index);
                }
                buffer
            });
            let stop = item.is_err();
            if tx.blocking_send(item).is_err() || stop {
                return;
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bee_rpc::bee_file::write_bee;
    use bee_rpc::frame::{serialize_part, Part};
    use celaut_proto::celaut::metadata::HashTag;
    use tokio_stream::StreamExt;

    fn metadata_bytes(id: [u8; 32]) -> Vec<u8> {
        Metadata {
            hashtag: Some(HashTag {
                hash: vec![Hash {
                    r#type: SHA3_256_TYPE.to_vec(),
                    value: id.to_vec(),
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn file(name: &str, metadata_index: i32, service_index: i32) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("service-library-{}-{name}.bee", std::process::id()));
        let mut buffers =
            serialize_part(&Part::new(metadata_index, metadata_bytes([5; 32]))).collect::<Vec<_>>();
        buffers.extend(serialize_part(&Part::new(service_index, vec![1, 2, 3])));
        write_bee(&mut File::create(&path).unwrap(), buffers).unwrap();
        path
    }

    #[test]
    fn a_nodo_export_gives_its_layout_and_the_id_from_the_metadata() {
        let path = file("export", 1, 2);
        let info = inspect(&path).unwrap();
        assert_eq!(info.layout, Layout::NodoExport);
        assert_eq!(info.service_id, [5; 32]);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_gateway_file_gives_its_layout() {
        let path = file("gateway", 5, 6);
        assert_eq!(inspect(&path).unwrap().layout, Layout::Gateway);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_missing_or_empty_file_is_an_error() {
        assert!(inspect(Path::new("/nonexistent/service.bee")).is_err());
        let path =
            std::env::temp_dir().join(format!("service-library-{}-empty.bee", std::process::id()));
        File::create(&path).unwrap();
        assert!(inspect(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn the_indices_of_a_nodo_export_become_those_of_the_node() {
        let path = file("read", 1, 2);
        let items: Vec<Buffer> = read(path.clone(), Layout::NodoExport)
            .map(|b| b.unwrap())
            .collect()
            .await;
        let heads: Vec<i32> = items
            .iter()
            .filter_map(|b| b.head.as_ref())
            .map(|h| h.index)
            .collect();
        assert_eq!(heads, vec![5, 6]);
        std::fs::remove_file(path).unwrap();
    }
}
