/// Convert this to an API with the following
/// * read_from_s3
/// * save_to_s3
/// * configure s3 access
/// * configure app norms (dir structure)
///
/// 1. list filenames with sizes
/// 2. struct DataFile { bucket, key, display_name, size, last_modified }
///
use aws_sdk_s3::config::{AppName, Region};
use aws_sdk_s3::operation::list_buckets::builders::ListBucketsFluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::Client;
use bytes::Bytes;
use dotenv::dotenv;

use lazy_static::lazy_static;

use s3_client::error::Result;
use s3_client::error::{into, Kind};
use s3_client::etl_obj::*;

use serde::Serialize;

// later put these in a yaml file
//const TEST_PROJECT: &str = "fef57333-67c0-4825-9765-5bf48f3d5f63";
const TEST_PROJECT: &str = "f2afe5c4-92f0-41c4-a8a6-c0d85ed0b9fd";
const ETL_OBJ_FILENAME: &str = "etlObj.json";

#[tokio::main]
async fn main() -> Result<()> {
    let client = init_client(ENDPOINT_URL.clone()).await;
    // ... make some calls with the client

    let response = list_buckets(&client)
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request))?;

    for bucket in response.buckets().unwrap().iter() {
        println!("{:?}", bucket.name().unwrap());
    }

    let project_keys = list_project_objects(&client, TEST_PROJECT).await?;
    println!("📁 project keys -------------------------------------- ");
    dbg!(&project_keys);

    println!("📁 diamond filenames -------------------------------------- ");
    let filenames = list_diamond_keys(&client, TEST_PROJECT, key_to_filename).await?;
    dbg!(&filenames);

    // download a file
    let path = ObjectPath::new(TEST_PROJECT, "etlObj.json")
        .with_diamonds()
        .with_bucket()
        .build();
    println!("📁 single file ------------ {}", &path);

    let bytes = download_bytes(&client, &path).await?;

    let v: EtlObject = serde_json::from_slice(&bytes)
        .map_err(|e| into(e, Kind::MalformedData).with_msg("EtlObject from json"))?;
    println!("{v}");

    // use client put_object to save the EtlObject v to the path value
    write_file(&client, TEST_PROJECT, "etlObj_vUploaded.json", &v).await?;

    Ok(())
}

async fn init_client(endpoint_url: impl AsRef<str>) -> Client {
    dotenv().ok();
    let sdk_config = ::aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url.as_ref())
        .app_name(AppName::new("TestAndControl".to_string()).unwrap())
        .build();
    Client::from_conf(config)
}

/// Write data to a S3 file from memory
async fn write_file<T: Serialize>(
    client: &Client,
    project_id: impl AsRef<str>,
    path: impl AsRef<str>,
    data: &T,
) -> Result<()> {
    // serialize the rust data
    let data = serde_json::to_vec(data).map_err(|e| into(e, Kind::MalformedData))?;
    let data = Bytes::from(data);

    let path = ObjectPath::new(project_id.as_ref(), path.as_ref())
        .with_diamonds()
        .build();

    let _ = client
        .put_object()
        .bucket(BUCKET_NAME.clone())
        .key(&path)
        .body(data.into())
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request).with_key(&path))?;

    Ok(())
}

/// A function that takes a Client and path
/// and returns the bytes required to deserialize it using serde_json::from_slice
/// or serde_json::from_reader
/// let path = "luci-space/fef57333-67c0-4825-9765-5bf48f3d5f63/shared/diamonds/fef57333-67c0-4825-9765-5bf48f3d5f63/warehouse.json";
/// 🔑 Do not set the bucket value in the client. The path must be fully qualified with the bucket
///    name luci-space
async fn download_bytes(client: &Client, path: impl AsRef<str>) -> Result<Vec<u8>> {
    let result = client
        .get_object()
        .key(path.as_ref())
        .response_content_type("application/json")
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request).with_key(&path))?;

    let bytes = result
        .body
        .collect()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Response))?
        .into_bytes();

    Ok(bytes.to_vec())
}

fn list_buckets(client: &Client) -> ListBucketsFluentBuilder {
    client.list_buckets()
}

/// Utility function that extracts the key values from the S3 Objects
fn fmap_obj_key<F, B>(objects: &ListObjectsV2Output, func: F) -> Vec<B>
where
    F: Fn(&str) -> B,
{
    objects
        .contents()
        .map(|objects| {
            objects
                .iter()
                .filter_map(|object| (object.key.as_ref()).and_then(|k| Some(func(k))))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// list filenames from the diamond directory
async fn list_diamond_keys<F>(
    client: &Client,
    project_id: impl AsRef<str>,
    pp_key_fn: F,
) -> Result<Vec<String>>
where
    F: Fn(&str) -> String + Send + Sync, // The closure takes &str and returns String
{
    // local fn that extracts filename from collection of objects
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET_NAME.clone())
        .prefix(diamonds_path(project_id.as_ref()))
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request).with_msg("Error listing diamonds"))?;

    Ok(fmap_obj_key(&objects, pp_key_fn))
}

async fn list_project_objects(client: &Client, project_id: impl AsRef<str>) -> Result<Vec<String>> {
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET_NAME.clone())
        .prefix(project_id.as_ref())
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request).with_msg("Error listing diamonds"))?;

    Ok(fmap_obj_key(&objects, |x| x.to_string()))
}

#[derive(Debug)]
pub struct ObjectPath {
    inner: String,
    project_id: String,
}

/// instantiate using the body in mk_key, add methods that use the bodies in with_diamonds and with_bucket
impl ObjectPath {
    pub fn new(project_id: impl AsRef<str>, filename: impl AsRef<str>) -> Self {
        let path = filename.as_ref();
        // if the first char is a '/' remove it
        let path = if path.starts_with('/') {
            &path[1..]
        } else {
            path
        };
        let inner = format!("{}/{}", project_id.as_ref(), &path);
        Self {
            inner,
            project_id: project_id.as_ref().to_string(),
        }
    }
    pub fn with_diamonds(mut self) -> Self {
        self.inner = format!("{}/shared/diamonds/{}", self.project_id, self.inner);
        self
    }
    pub fn with_bucket(mut self) -> Self {
        let bucket_name = BUCKET_NAME.clone();
        self.inner = format!("{}/{}", bucket_name, self.inner);
        self
    }
    pub fn as_str(&self) -> &str {
        &self.inner
    }
    pub fn build(self) -> String {
        self.inner
    }
}

/// Used to save/retrieve a file to project diamonds folder
fn diamonds_path(project_id: impl Into<String>) -> String {
    let pid: String = project_id.into();
    format!("{}/shared/diamonds/{}", pid.clone(), pid.clone())
}

/// Utility that extracts filename from object key
fn key_to_filename(key: &str) -> String {
    let filename = if let Some(filename) = key.rsplit_once('/').map(|(_, filename)| filename) {
        filename
    } else {
        key
    };
    filename.to_string()
}

lazy_static! {
    pub static ref BUCKET_NAME: String = {
        dotenv().ok();
        std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set")
    };
    pub static ref REGION_NAME: String = {
        dotenv().ok();
        std::env::var("AWS_REGION").expect("The aws_region name must be set")
    };
    pub static ref ENDPOINT_URL: String = {
        dotenv().ok();
        std::env::var("S3_HOST_BASE").expect("The s3_host_base value must be set")
    };
}
