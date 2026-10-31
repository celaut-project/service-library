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
    /// Set when the package is a directory of the block store and not a `.bee` file.
    pub directory: Option<DirectorySource>,
}

/// A service that is stored as directories of the block store: the Service is the
/// multiblock object `__services__/<hash>` and the Metadata is the object
/// `__metadata__/<hash>`, with their blocks in one block directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectorySource {
    /// `Enviroment.block_dir`: where the blocks that the objects name are.
    pub block_dir: PathBuf,
    /// The object of the Metadata, if the service has one.
    pub metadata: Option<PathBuf>,
    /// The object of the Service.
    pub service: PathBuf,
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
    Ok(PackageInfo {
        layout,
        service_id,
        directory: None,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The package of a service that is in `root/__services__/<hash>` (and, if it is there,
/// `root/__metadata__/<hash>`), with its blocks in `block_dir`. `service_id` is the SHA3-256
/// id, and the name of the entries is its lowercase hex. Only the existence of the objects
/// is checked: the blocks that are a single file are checked against their id when they
/// are read to be sent, and the node checks the rest.
pub fn inspect_directory(
    root: &Path,
    block_dir: &Path,
    service_id: [u8; 32],
) -> Result<PackageInfo, Error> {
    let name = hex(&service_id);
    let service = root.join("__services__").join(&name);
    if !service.exists() {
        return Err(invalid(format!("{} does not exist", service.display())));
    }
    if !block_dir.is_dir() {
        return Err(invalid(format!(
            "{} is not a directory of blocks",
            block_dir.display()
        )));
    }
    let metadata = root.join("__metadata__").join(&name);
    Ok(PackageInfo {
        layout: Layout::Gateway,
        service_id,
        directory: Some(DirectorySource {
            block_dir: block_dir.to_path_buf(),
            metadata: metadata.exists().then_some(metadata),
            service,
        }),
    })
}

/// The Buffers of a package for a node: the file at `path`, or the objects of the
/// directory source in `info`, with the indices of `StartService`.
pub fn read_package(
    path: PathBuf,
    info: &PackageInfo,
) -> impl tokio_stream::Stream<Item = Result<Buffer, bee_rpc::Error>> + Send {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Buffer, bee_rpc::Error>>(4);
    match info.directory.clone() {
        None => {
            let layout = info.layout;
            tokio::spawn(async move {
                use tokio_stream::StreamExt;
                let mut stream = Box::pin(read(path, layout));
                while let Some(item) = stream.next().await {
                    let stop = item.is_err();
                    if tx.send(item).await.is_err() || stop {
                        return;
                    }
                }
            });
        }
        Some(source) => {
            tokio::task::spawn_blocking(move || {
                let store = bee_rpc::store::Store::new(&source.block_dir);
                let parts = source
                    .metadata
                    .iter()
                    .map(|p| (5, p))
                    .chain(std::iter::once((6, &source.service)));
                for (index, object) in parts {
                    let buffers = match store.read_message(index, object) {
                        Ok(buffers) => buffers,
                        Err(e) => {
                            let _ = tx.blocking_send(Err(e));
                            return;
                        }
                    };
                    for item in buffers {
                        let stop = item.is_err();
                        if tx.blocking_send(item).is_err() || stop {
                            return;
                        }
                    }
                }
            });
        }
    }
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Check that each block of the package holds what its id names: the bytes of its chunks,
/// with those of the blocks inside it, hash (SHA3-256) to its id. A node refuses a block that
/// does not, but only after it got the whole package. The file is read once, one record at a
/// time. This reads the whole file, so call it from a thread that may block.
pub fn verify_blocks(path: &Path) -> Result<(), Error> {
    let file = File::open(path).map_err(|e| invalid(format!("{}: {e}", path.display())))?;
    // The open blocks, outermost first: their id and the hash of what they hold so far.
    let mut open: Vec<([u8; 32], bee_rpc::hash::Hasher)> = Vec::new();
    for buffer in read_bee(BufReader::new(file)) {
        let buffer = buffer.map_err(|e| invalid(e.to_string()))?;
        if let Some(block) = &buffer.block {
            let id: [u8; 32] = block
                .hashes
                .iter()
                .find(|h| h.r#type == SHA3_256_TYPE)
                .and_then(|h| h.value.as_slice().try_into().ok())
                .ok_or_else(|| invalid("a block has no SHA3-256 id"))?;
            if let Some((_, hasher)) = open.pop_if(|(last, _)| *last == id) {
                // The end marker of the innermost open block.
                let got = hasher.finalize();
                if got != id {
                    return Err(invalid(format!(
                        "the block {} holds other bytes: they hash to {}",
                        hex::encode(id),
                        hex::encode(got)
                    )));
                }
            } else if open.iter().any(|(other, _)| *other == id) {
                return Err(invalid(format!(
                    "the block {} ends inside a block that it holds",
                    hex::encode(id)
                )));
            } else {
                open.push((id, bee_rpc::hash::HashAlgorithm::Sha3_256.hasher()));
            }
        }
        if let Some(chunk) = &buffer.chunk {
            for (_, hasher) in open.iter_mut() {
                hasher.update(chunk);
            }
        }
    }
    match open.last() {
        Some((id, _)) => Err(invalid(format!(
            "the file is cut inside the block {}",
            hex::encode(id)
        ))),
        None => Ok(()),
    }
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

    /// A file with one block that holds `content` and is named `id`.
    fn block_file(name: &str, id: [u8; 32], content: &[u8]) -> PathBuf {
        use bee_rpc::buffer::buffer::{block::Hash as BlockHash, Block};
        let marker = || Buffer {
            block: Some(Block {
                hashes: vec![BlockHash {
                    r#type: SHA3_256_TYPE.to_vec(),
                    value: id.to_vec(),
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let chunk = Buffer {
            chunk: Some(content.to_vec()),
            ..Default::default()
        };
        let path =
            std::env::temp_dir().join(format!("service-library-{}-{name}.bee", std::process::id()));
        write_bee(
            &mut File::create(&path).unwrap(),
            [marker(), chunk, marker()],
        )
        .unwrap();
        path
    }

    #[test]
    fn a_block_that_holds_what_its_id_names_verifies() {
        let id = bee_rpc::hash::HashAlgorithm::Sha3_256.digest(b"hello");
        let path = block_file("good-block", id.as_slice().try_into().unwrap(), b"hello");
        verify_blocks(&path).unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn a_block_that_holds_other_bytes_does_not_verify() {
        let id = bee_rpc::hash::HashAlgorithm::Sha3_256.digest(b"hello");
        let path = block_file("bad-block", id.as_slice().try_into().unwrap(), b"HELLO");
        let err = verify_blocks(&path).unwrap_err().to_string();
        assert!(err.contains("holds other bytes"), "{err}");
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

    /// A root with the Metadata as a plain file and the Service as a multiblock object whose
    /// only block holds `content`. `corrupt` changes the block after it got its id.
    fn directory(name: &str, content: &[u8], corrupt: bool) -> (PathBuf, PathBuf, [u8; 32]) {
        let base =
            std::env::temp_dir().join(format!("service-library-{}-{name}.dir", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let id = [7u8; 32];
        let name = hex(&id);
        let blocks = base.join("blocks");
        let service = base.join("__services__").join(&name);
        std::fs::create_dir_all(&blocks).unwrap();
        std::fs::create_dir_all(&service).unwrap();
        std::fs::create_dir_all(base.join("__metadata__")).unwrap();
        std::fs::write(base.join("__metadata__").join(&name), metadata_bytes(id)).unwrap();
        let block_id = hex(&bee_rpc::hash::HashAlgorithm::Sha3_256.digest(content));
        let stored = if corrupt {
            b"other".to_vec()
        } else {
            content.to_vec()
        };
        std::fs::write(blocks.join(&block_id), stored).unwrap();
        std::fs::write(
            service.join("_.json"),
            format!("[[\"{block_id}\", [{}]]]", content.len()),
        )
        .unwrap();
        (base, blocks, id)
    }

    async fn heads_and_bytes(path: PathBuf, info: &PackageInfo) -> (Vec<i32>, Vec<u8>, usize) {
        let mut stream = Box::pin(read_package(path, info));
        let (mut heads, mut bytes, mut markers) = (Vec::new(), Vec::new(), 0);
        while let Some(buffer) = stream.next().await {
            let buffer = buffer.unwrap();
            heads.extend(buffer.head.map(|h| h.index));
            if let Some(chunk) = buffer.chunk {
                bytes.extend_from_slice(&chunk);
            }
            if buffer.block.is_some() {
                markers += 1;
            }
        }
        (heads, bytes, markers)
    }

    #[tokio::test]
    async fn a_directory_package_gives_the_metadata_and_the_service_blocks() {
        let content = vec![9u8; 5000];
        let (base, blocks, id) = directory("ok", &content, false);
        let info = inspect_directory(&base, &blocks, id).unwrap();
        assert_eq!(info.service_id, id);
        assert!(info.directory.as_ref().unwrap().metadata.is_some());
        let (heads, bytes, markers) = heads_and_bytes(base.clone(), &info).await;
        assert_eq!(heads, vec![5, 6]);
        assert_eq!(markers, 2, "the start and the end marker of the block");
        assert!(bytes.ends_with(&content));
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn a_directory_package_without_metadata_sends_only_the_service() {
        let (base, blocks, id) = directory("nometa", b"abc", false);
        std::fs::remove_file(base.join("__metadata__").join(hex(&id))).unwrap();
        let info = inspect_directory(&base, &blocks, id).unwrap();
        assert!(info.directory.as_ref().unwrap().metadata.is_none());
        let (heads, _, _) = heads_and_bytes(base.clone(), &info).await;
        assert_eq!(heads, vec![6]);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn a_block_that_does_not_hold_its_id_is_not_sent_whole() {
        let (base, blocks, id) = directory("bad", &[1u8; 100], true);
        let info = inspect_directory(&base, &blocks, id).unwrap();
        let mut stream = Box::pin(read_package(base.clone(), &info));
        let mut failed = false;
        while let Some(item) = stream.next().await {
            failed |= item.is_err();
        }
        assert!(failed, "the stream must end with an error");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn a_directory_without_the_service_is_refused() {
        let base =
            std::env::temp_dir().join(format!("service-library-{}-empty", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        assert!(inspect_directory(&base, &base, [1; 32]).is_err());
        let _ = std::fs::remove_dir_all(base);
    }
}
