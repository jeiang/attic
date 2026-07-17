use std::sync::Arc;

use digest::Output as DigestOutput;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncRead};
use tokio::sync::OnceCell;

use attic::io::HashReader;

pub type CompressorFn<C> = Box<dyn FnOnce(C) -> Box<dyn AsyncRead + Unpin + Send> + Send>;

/// Applies compression to a stream, computing hashes along the way.
///
/// Our strategy is to stream directly onto a UUID-keyed file on the
/// storage backend, performing compression and computing the hashes
/// along the way. We delete the file if the hashes do not match.
///
/// ```text
///                    ┌───────────────────────────────────►NAR Hash
///                    │
///                    │
///                    ├───────────────────────────────────►NAR Size
///                    │
///              ┌─────┴────┐  ┌──────────┐  ┌───────────┐
/// NAR Stream──►│NAR Hasher├─►│Compressor├─►│File Hasher├─►File Stream
///              └──────────┘  └──────────┘  └─────┬─────┘
///                                                │
///                                                ├───────►File Hash
///                                                │
///                                                │
///                                                └───────►File Size
/// ```
///
/// `with_precomputed_nar_hash` skips the NAR Hasher stage above (the NAR
/// hash/size are supplied up front instead), for callers that already
/// computed them over trusted data.
pub struct CompressionStream {
    stream: Box<dyn AsyncRead + Unpin + Send>,
    nar_compute: Arc<OnceCell<(DigestOutput<Sha256>, usize)>>,
    file_compute: Arc<OnceCell<(DigestOutput<Sha256>, usize)>>,
}

impl CompressionStream {
    /// Creates a new compression stream.
    pub fn new<R>(stream: R, compressor: CompressorFn<HashReader<R, Sha256>>) -> Self
    where
        R: AsyncBufRead + Unpin + Send + 'static,
    {
        // compute NAR hash and size
        let (stream, nar_compute) = HashReader::new(stream, Sha256::new());

        // compress NAR
        let stream = compressor(stream);

        // compute file hash and size
        let (stream, file_compute) = HashReader::new(stream, Sha256::new());

        Self {
            stream: Box::new(stream),
            nar_compute,
            file_compute,
        }
    }

    /// Creates a new compression stream with a precomputed NAR hash and size.
    ///
    /// For callers that already computed the uncompressed (NAR) hash and
    /// size over trusted in-memory data, this avoids a redundant hashing
    /// pass over the same bytes: the inner NAR-hash `HashReader` stage is
    /// skipped entirely, and `nar_hash_and_size()` returns the supplied
    /// value immediately instead of being finalized as the stream is read.
    pub fn with_precomputed_nar_hash<R>(
        stream: R,
        compressor: CompressorFn<R>,
        nar_hash_and_size: (DigestOutput<Sha256>, usize),
    ) -> Self
    where
        R: AsyncBufRead + Unpin + Send + 'static,
    {
        // compress NAR directly - the NAR hash/size are already known
        let stream = compressor(stream);

        // compute file hash and size
        let (stream, file_compute) = HashReader::new(stream, Sha256::new());

        let nar_compute = Arc::new(OnceCell::new_with(Some(nar_hash_and_size)));

        Self {
            stream: Box::new(stream),
            nar_compute,
            file_compute,
        }
    }

    /// Returns the stream of the compressed object.
    pub fn stream(&mut self) -> &mut (impl AsyncRead + Unpin) {
        &mut self.stream
    }

    /// Returns the NAR hash and size.
    ///
    /// The hash is only finalized when the stream is fully read.
    /// Otherwise, returns `None`.
    pub fn nar_hash_and_size(&self) -> Option<&(DigestOutput<Sha256>, usize)> {
        self.nar_compute.get()
    }

    /// Returns the file hash and size.
    ///
    /// The hash is only finalized when the stream is fully read.
    /// Otherwise, returns `None`.
    pub fn file_hash_and_size(&self) -> Option<&(DigestOutput<Sha256>, usize)> {
        self.file_compute.get()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use attic::testing::get_fake_data;
    use tokio::io::AsyncReadExt;

    use super::*;

    /// Returns a compressor that passes the stream through unmodified.
    fn identity_compressor<C: AsyncRead + Unpin + Send + 'static>() -> CompressorFn<C> {
        Box::new(|c| Box::new(c))
    }

    #[tokio::test]
    async fn with_precomputed_nar_hash_matches_new() {
        // A few hundred KiB of deterministic pseudo-random data.
        let data = get_fake_data(300 * 1024);

        let mut hasher = Sha256::new();
        hasher.update(&data);
        let nar_hash = hasher.finalize();
        let nar_size = data.len();

        // Drive the stream through the regular constructor, which hashes
        // the uncompressed data as it flows through.
        let mut stream_new =
            CompressionStream::new(Cursor::new(data.clone()), identity_compressor());
        let mut out_new = Vec::new();
        stream_new
            .stream()
            .read_to_end(&mut out_new)
            .await
            .expect("reading compressed stream (new) failed");

        // Drive the stream through the precomputed-hash constructor, seeded
        // with the hash/size computed manually above.
        let mut stream_precomputed = CompressionStream::with_precomputed_nar_hash(
            Cursor::new(data.clone()),
            identity_compressor(),
            (nar_hash.clone(), nar_size),
        );
        let mut out_precomputed = Vec::new();
        stream_precomputed
            .stream()
            .read_to_end(&mut out_precomputed)
            .await
            .expect("reading compressed stream (precomputed) failed");

        // The compressed output must be identical either way.
        assert_eq!(out_new, out_precomputed);

        // The file hash/size (computed downstream of compression in both
        // cases) must also be identical.
        assert_eq!(
            stream_new.file_hash_and_size(),
            stream_precomputed.file_hash_and_size()
        );

        // The precomputed NAR hash/size must be exactly what was seeded in.
        assert_eq!(
            stream_precomputed.nar_hash_and_size(),
            Some(&(nar_hash, nar_size))
        );
    }
}
