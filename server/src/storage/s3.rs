//! S3 remote files.

use std::sync::Arc;
use std::time::Duration;

use aws_config::{BehaviorVersion, retry::RetryConfig};
use aws_sdk_s3::{
    Client,
    config::Builder as S3ConfigBuilder,
    config::{Credentials, Region},
    operation::get_object::builders::GetObjectFluentBuilder,
    presigning::PresigningConfig,
    types::{CompletedMultipartUpload, CompletedPart},
};
use bytes::BytesMut;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio::sync::Semaphore;

use super::{Download, RemoteFile, StorageBackend};
use crate::error::{ErrorKind, ServerError, ServerResult};
use attic::io::read_chunk_async;
use attic::util::Finally;

/// The chunk size for each part in a multipart upload.
const CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// The size of the initial read used to decide between a single PutObject
/// and a multipart upload.
///
/// Most `upload_file` calls carry a small CDC chunk (at most a few hundred
/// KiB compressed), so eagerly allocating a full `CHUNK_SIZE` (8 MiB) buffer
/// for every call wastes allocator headroom. We first read up to this
/// smaller size; only if it fills up do we grow the buffer up to the full
/// `CHUNK_SIZE` and proceed with the existing PutObject-vs-multipart
/// decision (unchanged: PutObject iff the whole stream fits in one part).
const INITIAL_READ_SIZE: usize = 512 * 1024;

/// Maximum number of multipart parts that may be buffered/in-flight
/// (read from the stream but not yet fully uploaded) at once.
///
/// Without this bound, a fast reader races ahead of slower S3 uploads and
/// the entire stream ends up buffered in memory as a backlog of pending
/// parts. Permits are acquired before reading each part's data so the
/// reader itself is throttled, not just the uploads.
const MAX_CONCURRENT_MULTIPART_PARTS: usize = 4;

/// The S3 remote file storage backend.
#[derive(Debug)]
pub struct S3Backend {
    client: Client,
    config: S3StorageConfig,
}

/// S3 remote file storage configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct S3StorageConfig {
    /// The AWS region.
    region: String,

    /// The name of the bucket.
    bucket: String,

    /// Custom S3 endpoint.
    ///
    /// Set this if you are using an S3-compatible object storage (e.g., Minio).
    endpoint: Option<String>,

    /// S3 credentials.
    ///
    /// If not specified, it's read from the `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY` environment variables.
    credentials: Option<S3CredentialsConfig>,
}

/// S3 credential configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct S3CredentialsConfig {
    /// Access key ID.
    access_key_id: String,

    /// Secret access key.
    secret_access_key: String,
}

/// Reference to a file in an S3-compatible storage bucket.
///
/// We store the region and bucket to facilitate migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3RemoteFile {
    /// Name of the S3 region.
    pub region: String,

    /// Name of the bucket.
    pub bucket: String,

    /// Key of the file.
    pub key: String,
}

impl S3Backend {
    pub async fn new(config: S3StorageConfig) -> ServerResult<Self> {
        let s3_config = Self::config_builder(&config)
            .await?
            .region(Region::new(config.region.to_owned()))
            .retry_config(RetryConfig::adaptive())
            .build();

        Ok(Self {
            client: Client::from_conf(s3_config),
            config,
        })
    }

    async fn config_builder(config: &S3StorageConfig) -> ServerResult<S3ConfigBuilder> {
        let shared_config = aws_config::load_defaults(BehaviorVersion::v2026_01_12()).await;
        let mut builder = S3ConfigBuilder::from(&shared_config);

        if let Some(credentials) = &config.credentials {
            builder = builder.credentials_provider(Credentials::new(
                &credentials.access_key_id,
                &credentials.secret_access_key,
                None,
                None,
                "s3",
            ));
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }

        Ok(builder)
    }

    async fn get_client_from_db_ref<'a>(
        &self,
        file: &'a RemoteFile,
    ) -> ServerResult<(Client, &'a S3RemoteFile)> {
        let file = if let RemoteFile::S3(file) = file {
            file
        } else {
            return Err(ErrorKind::StorageError(anyhow::anyhow!(
                "Does not understand the remote file reference"
            ))
            .into());
        };

        // FIXME: Ugly
        let client = if self.client.config().region().unwrap().as_ref() == file.region {
            self.client.clone()
        } else {
            // FIXME: Cache the client instance
            let s3_conf = Self::config_builder(&self.config)
                .await?
                .region(Region::new(file.region.to_owned()))
                .build();
            Client::from_conf(s3_conf)
        };

        Ok((client, file))
    }

    async fn get_download(
        &self,
        req: GetObjectFluentBuilder,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        if prefer_stream {
            let output = req.send().await.map_err(ServerError::storage_error)?;

            Ok(Download::AsyncRead(Box::new(output.body.into_async_read())))
        } else {
            // FIXME: Configurable expiration
            let presign_config = PresigningConfig::expires_in(Duration::from_secs(600))
                .map_err(ServerError::storage_error)?;

            let presigned = req
                .presigned(presign_config)
                .await
                .map_err(ServerError::storage_error)?;

            Ok(Download::Url(presigned.uri().to_string()))
        }
    }
}

impl StorageBackend for S3Backend {
    async fn upload_file(
        &self,
        name: String,
        mut stream: &mut (dyn AsyncRead + Unpin + Send),
    ) -> ServerResult<RemoteFile> {
        let buf = BytesMut::with_capacity(INITIAL_READ_SIZE);
        let small_chunk = read_chunk_async(&mut stream, buf)
            .await
            .map_err(ServerError::storage_error)?;

        let first_chunk = if small_chunk.len() < INITIAL_READ_SIZE {
            // The whole stream fit in the small initial buffer.
            small_chunk
        } else {
            // The initial buffer filled up - grow to the full part size and
            // keep reading to determine whether we need multipart upload.
            let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
            buf.extend_from_slice(&small_chunk);
            drop(small_chunk);

            read_chunk_async(&mut stream, buf)
                .await
                .map_err(ServerError::storage_error)?
        };

        if first_chunk.len() < CHUNK_SIZE {
            // do a normal PutObject
            let put_object = self
                .client
                .put_object()
                .bucket(&self.config.bucket)
                .key(&name)
                .body(first_chunk.into())
                .send()
                .await
                .map_err(ServerError::storage_error)?;

            tracing::debug!("put_object -> {:#?}", put_object);

            return Ok(RemoteFile::S3(S3RemoteFile {
                region: self.config.region.clone(),
                bucket: self.config.bucket.clone(),
                key: name,
            }));
        }

        let multipart = self
            .client
            .create_multipart_upload()
            .bucket(&self.config.bucket)
            .key(&name)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        let upload_id = multipart.upload_id().unwrap();

        let cleanup = Finally::new({
            let bucket = self.config.bucket.clone();
            let client = self.client.clone();
            let upload_id = upload_id.to_owned();
            let name = name.clone();

            async move {
                tracing::warn!("Upload was interrupted - Aborting multipart upload");

                let r = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(name)
                    .upload_id(upload_id)
                    .send()
                    .await;

                if let Err(e) = r {
                    tracing::warn!("Failed to abort multipart upload: {}", e);
                }
            }
        });

        let part_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_MULTIPART_PARTS));

        let mut part_number = 1;
        let mut parts = Vec::new();
        let mut first_chunk = Some(first_chunk);

        loop {
            // Acquire a permit before reading the next part's data (or,
            // for the already-read first chunk, before spawning its
            // upload). This bounds how many parts may be buffered/
            // in-flight at once - acquiring the permit after the read
            // would still let the reader race ahead and buffer the
            // whole stream.
            let permit = part_semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("part semaphore should never be closed");

            let chunk = if part_number == 1 {
                first_chunk.take().unwrap()
            } else {
                let buf = BytesMut::with_capacity(CHUNK_SIZE);
                read_chunk_async(&mut stream, buf)
                    .await
                    .map_err(ServerError::storage_error)?
            };

            if chunk.is_empty() {
                break;
            }

            let client = self.client.clone();
            let bucket = self.config.bucket.clone();
            let key = name.clone();
            let upload_id = upload_id.to_owned();
            let fut = tokio::task::spawn(async move {
                let result = client
                    .upload_part()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .body(chunk.into())
                    .send()
                    .await;

                // Release the permit only once the upload has finished,
                // regardless of outcome.
                drop(permit);

                result
            });

            parts.push(fut);
            part_number += 1;
        }

        let uploaded_parts = join_all(parts).await;
        let mut completed_parts = Vec::with_capacity(uploaded_parts.len());

        for (idx, join_result) in uploaded_parts.into_iter().enumerate() {
            let part = join_result.unwrap().map_err(ServerError::storage_error)?;
            let part_number = idx + 1;
            completed_parts.push(
                CompletedPart::builder()
                    .set_e_tag(part.e_tag().map(str::to_string))
                    .set_part_number(Some(part_number as i32))
                    .build(),
            );
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        let completion = self
            .client
            .complete_multipart_upload()
            .bucket(&self.config.bucket)
            .key(&name)
            .upload_id(upload_id)
            .multipart_upload(completed_multipart_upload)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("complete_multipart_upload -> {:#?}", completion);

        cleanup.cancel();

        Ok(RemoteFile::S3(S3RemoteFile {
            region: self.config.region.clone(),
            bucket: self.config.bucket.clone(),
            key: name,
        }))
    }

    async fn delete_file(&self, name: String) -> ServerResult<()> {
        let deletion = self
            .client
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&name)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file -> {:#?}", deletion);

        Ok(())
    }

    async fn delete_file_db(&self, file: &RemoteFile) -> ServerResult<()> {
        let (client, file) = self.get_client_from_db_ref(file).await?;

        let deletion = client
            .delete_object()
            .bucket(&file.bucket)
            .key(&file.key)
            .send()
            .await
            .map_err(ServerError::storage_error)?;

        tracing::debug!("delete_file -> {:#?}", deletion);

        Ok(())
    }

    async fn download_file_db(
        &self,
        file: &RemoteFile,
        prefer_stream: bool,
    ) -> ServerResult<Download> {
        let (client, file) = self.get_client_from_db_ref(file).await?;

        let req = client.get_object().bucket(&file.bucket).key(&file.key);

        self.get_download(req, prefer_stream).await
    }

    async fn make_db_reference(&self, name: String) -> ServerResult<RemoteFile> {
        Ok(RemoteFile::S3(S3RemoteFile {
            region: self.config.region.clone(),
            bucket: self.config.bucket.clone(),
            key: name,
        }))
    }
}
