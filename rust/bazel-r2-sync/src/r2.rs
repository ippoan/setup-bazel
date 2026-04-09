use anyhow::{Context, Result};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;

pub struct R2Client {
    bucket: Box<Bucket>,
}

impl R2Client {
    pub fn new(endpoint: &str, bucket_name: &str) -> Result<Self> {
        let access_key =
            std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID not set")?;
        let secret_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY not set")?;

        let region = Region::Custom {
            region: "auto".to_string(),
            endpoint: endpoint.to_string(),
        };

        let credentials = Credentials::new(Some(&access_key), Some(&secret_key), None, None, None)
            .map_err(|e| anyhow::anyhow!("credentials error: {e}"))?;

        let bucket = Bucket::new(bucket_name, region, credentials)
            .map_err(|e| anyhow::anyhow!("bucket error: {e}"))?;

        Ok(Self { bucket })
    }

    /// Download an object and return its bytes. Returns None if 404.
    pub async fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response = self
            .bucket
            .get_object(key)
            .await
            .with_context(|| format!("R2 GET {key}"))?;

        let status = response.status_code();
        if status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&status) {
            anyhow::bail!("R2 GET {key}: status {status}");
        }
        Ok(Some(response.to_vec()))
    }

    /// Upload bytes to an object.
    pub async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let response = self
            .bucket
            .put_object(key, data)
            .await
            .with_context(|| format!("R2 PUT {key}"))?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            anyhow::bail!("R2 PUT {key}: status {status}");
        }
        Ok(())
    }
}
