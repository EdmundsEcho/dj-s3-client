/// stop
/// TODO:
///
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
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::list_buckets::builders::ListBucketsFluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::builders::{
    ListObjectsV2FluentBuilder, ListObjectsV2OutputBuilder,
};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
use aws_sdk_s3::types::error::{InvalidObjectState, NotFound};
use aws_sdk_s3::Client;
use aws_sdk_s3::Error;
use dotenv::dotenv;
use serde_json::{from_slice, Value};

use lazy_static::lazy_static;

use s3_client::error::Result;
use s3_client::error::{into, Kind};
use s3_client::etl_obj::*;

const TEST_PROJECT: &str = "fef57333-67c0-4825-9765-5bf48f3d5f63";
const ETL_OBJ_FILENAME: &str = "etlObj.json";

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let bucket_name = std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set");
    let region_name = std::env::var("AWS_REGION").expect("The region name must be set");
    let endpoint_url = std::env::var("S3_HOST_BASE").expect("The host base must be set");
    // let endpoint_url = std::env::var("S3_HOST_BUCKET").expect("The host bucket must be set");
    println!("Bucket name: {bucket_name}");
    println!("Region name: {region_name}");
    println!("Endpoint url: {endpoint_url}");
    // let config = aws_config::load_from_env().await;

    let sdk_config = ::aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .force_path_style(false)
        .app_name(AppName::new("TestAndControl".to_string()).unwrap())
        .build();

    // output force_path_style
    // println!("Config look for force_path_style");
    // println!("{:?}", config);

    let client = Client::from_conf(config);
    // ... make some calls with the client

    let response = list_buckets(&client)
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request))?;

    for bucket in response.buckets().unwrap().iter() {
        println!("{:?}", bucket.name().unwrap());
    }

    let objects = list_objects(&client, TEST_PROJECT)
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request))?;

    if let Some(objects) = objects.contents() {
        println!("📁 ---->>>> List objects");
        for object in objects.iter() {
            println!("{:?}", object);
        }
        println!("📁 END");
    }

    // some sort of side effect
    // call fmap using string -> Option<String>
    let keys = fmap_obj_key(&objects, |key| {
        println!("{}", &key);
        Some(key.to_string())
    });

    /*
    for key in keys.iter() {
        client
            .get_object()
            .bucket(BUCKET_NAME.clone())
            .key(mk_key(TEST_PROJECT, key))
            // .response_content_type("application/json")
            .send()
            .await
            .map_err(|sdk_err| into(sdk_err, Kind::Request).with_key(key))?;
    } */

    println!("📁 diamond filenames -------------------------------------- ");
    let filenames = list_diamonds(&client, TEST_PROJECT).await?;
    dbg!(&filenames);

    // download a file

    // https://luci-space.lucivia.net/fef57333-67c0-4825-9765-5bf48f3d5f63/shared/diamonds/fef57333-67c0-4825-9765-5bf48f3d5f63/etlObj.json   //
    // https://luci-space.sfo3.digitaloceanspaces.com
    let path = diamond_prefix(TEST_PROJECT);
    let path = format!("luci-space/{path}/etlObj.json");
    // let path = "luci-space/fef57333-67c0-4825-9765-5bf48f3d5f63/shared/diamonds/fef57333-67c0-4825-9765-5bf48f3d5f63/warehouse.json";
    println!("📁 single file ------------ {}", &path);

    let result = client
        .get_object()
        .key(&path)
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

    // Temp comment out
    // let v: EtlObject = serde_json::from_slice(&bytes).map_err(|e| into(e, Kind::Decode))?;
    // dbg!(v);
    use s3_client::etl_obj::*;
    use std::fs::File;
    use std::io::Read;
    let path = "/Users/edmund/Downloads/etlObj.json";
    let mut file = File::open(path) // type
        .map_err(|err| into(err, Kind::Request))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents) // type
        .map_err(|err| into(err, Kind::Request))?;

    // Deserialize the JSON data
    let etl_object: EtlObject =
        serde_json::from_str(&contents).map_err(|err| into(err, Kind::Request))?;
    println!("{:#?}", etl_object);

    // let response: &str = std::str::from_utf8(&bytes).map_err(|e| Error::InvalidObjectState(e))?;
    // let temp: Value = serde_json::from_str(response).unwrap();

    // println!("{:?}", temp);

    /*
        let data = reader_file(&client, TEST_PROJECT, &path).send().await?;
    let stream = data
        .body
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let stream_reader = StreamReader::new(stream);
    */

    Ok(())
}

async fn init_client() -> Result<Client> {
    dotenv().ok();
    let bucket_name = std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set");
    let region_name = std::env::var("AWS_REGION").expect("The region name must be set");
    let endpoint_url = std::env::var("S3_HOST_BASE").expect("The host base must be set");
    println!("Bucket name: {bucket_name}");
    println!("Region name: {region_name}");
    println!("Endpoint url: {endpoint_url}");

    let sdk_config = ::aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .app_name(AppName::new("TestAndControl".to_string()).unwrap())
        .build();
    let client = Client::from_conf(config);

    Ok(client)
}

/// list objects in a project /shared/diamonds/*.feather among others
fn list_objects(client: &Client, project_id: impl AsRef<str>) -> ListObjectsV2FluentBuilder {
    let objects = client
        .list_objects_v2()
        .bucket(BUCKET_NAME.clone())
        .prefix(project_id.as_ref());
    objects
}

fn list_buckets(client: &Client) -> ListBucketsFluentBuilder {
    client.list_buckets()
}

fn fmap_obj_key<F, B>(objects: &ListObjectsV2Output, func: F) -> Vec<B>
where
    F: Fn(&str) -> Option<B>,
{
    objects
        .contents()
        .map(|objects| {
            objects
                .iter()
                .filter_map(|object| (object.key.as_ref()).and_then(|k| func(k)))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// list filenames from the diamond directory
async fn list_diamonds(client: &Client, project_id: impl AsRef<str>) -> Result<Vec<String>> {
    // local fn that extracts filename from collection of objects
    let objects = list_diamond_objects(client, project_id)
        .send()
        .await
        .map_err(|sdk_err| into(sdk_err, Kind::Request).with_msg("Error listing diamonds"))?;

    Ok(fmap_obj_key(&objects, key_to_filename))
}

fn list_diamond_objects(
    client: &Client,
    project_id: impl AsRef<str>,
) -> ListObjectsV2FluentBuilder {
    client
        .list_objects_v2()
        .bucket(BUCKET_NAME.clone())
        .prefix(diamond_prefix(project_id.as_ref()))
}

/// Write data to a S3 file from memory
fn write_file(
    client: &Client,
    project_id: impl AsRef<str>,
    path: impl AsRef<str>,
) -> PutObjectFluentBuilder {
    client
        .put_object()
        .bucket(BUCKET_NAME.clone())
        .key(mk_key(project_id, path))
}

/// Read data from a S3 file into memory
/// Use case in py:  pd.read_feather(io.BytesIO(resp['Body'].read()))
fn reader_file(
    client: &Client,
    project_id: impl AsRef<str>,
    path: impl AsRef<str>,
) -> GetObjectFluentBuilder {
    let path = mk_key(project_id, path);
    let path = format!("luci-drive/{}", &path);
    println!("Path: {}", &path);
    client.get_object().bucket(BUCKET_NAME.clone()).key(&path)
}

/// Used to save/retrieve a file to project diamonds folder
fn diamond_prefix(project_id: impl Into<String>) -> String {
    let pid: String = project_id.into();
    format!("{}/shared/diamonds/{}", pid.clone(), pid.clone())
}

/// Used to save/retrieve a file on the S3 resource
fn mk_key(project_id: impl AsRef<str>, path: impl AsRef<str>) -> String {
    let path = path.as_ref();
    format!("{}/{}", project_id.as_ref(), &path[1..])
}

fn key_to_filename(key: &str) -> Option<String> {
    key.rsplit_once('/')
        .map(|(_, filename)| filename.to_string())
}

lazy_static! {
    pub static ref BUCKET_NAME: String = {
        dotenv().ok();
        let bucket_name = std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set");
        bucket_name
    };
}
/*
pub async fn list_objects(client: &Client, bucket_name: &str) -> Result<(), Error> {
    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;
    println!("Objects in bucket:");
    for obj in objects.contents() {
        println!("{:?}", obj.key().unwrap());
    }

    Ok(())
} */

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum EtlUnit {
    #[serde(rename = "quality")]
    Quality(QualityUnit),
    #[serde(rename = "mvalue")]
    Measurement(MeasurementUnit),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QualityUnit {
    pub subject: String,
    pub codomain: String,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MeasurementUnit {
    pub subject: String,
    pub mspan: String,
    pub mcomps: Vec<String>,
    pub codomain: String,
    #[serde(rename = "codomain-reducer")]
    pub codomain_reducer: String,
    #[serde(rename = "slicing-reducer")]
    pub slicing_reducer: String,
}

type EtlUnits = HashMap<String, EtlUnit>;
