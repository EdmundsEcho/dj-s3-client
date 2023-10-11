// stop
//
use aws_sdk_s3::config::{AppName, Region};
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::list_objects_v2::builders::{
    ListObjectsV2FluentBuilder, ListObjectsV2OutputBuilder,
};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::types::error::{InvalidObjectState, NotFound};
use aws_sdk_s3::Client;
use aws_sdk_s3::Error;
use dotenv::dotenv;
use serde_json::Value;

use lazy_static::lazy_static;

const TEST_PROJECT: &str = "fef57333-67c0-4825-9765-5bf48f3d5f63";
const ETL_OBJ_FILENAME: &str = "etlObj.json";

#[tokio::main]
async fn main() -> Result<(), Error> {
    dotenv().ok();

    let bucket_name = std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set");
    let region_name = std::env::var("AWS_REGION").expect("The region name must be set");
    let endpoint_url = std::env::var("S3_HOST_BASE").expect("The host base must be set");
    println!("Bucket name: {bucket_name}");
    println!("Region name: {region_name}");
    println!("Endpoint url: {endpoint_url}");
    let config = aws_config::load_from_env().await;

    let sdk_config = ::aws_config::load_from_env().await;
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .app_name(AppName::new("TestAndControl".to_string()).unwrap())
        .build();

    let client = Client::from_conf(config);
    // ... make some calls with the client

    let response = client.list_buckets().send().await?;

    for bucket in response.buckets().unwrap().iter() {
        println!("{:?}", bucket.name().unwrap());
    }

    let objects = list_objects(&client, TEST_PROJECT).send().await?;
    if let Some(objects) = objects.contents() {
        for object in objects.iter() {
            println!("List objects");
            println!("{:?}", object);
        }
    }

    println!("📁 diamond filenames -------------------------------------- ");
    let filenames = list_diamonds(&client, TEST_PROJECT).await?;
    dbg!(&filenames);

    // download a file
    let path = diamond_prefix(TEST_PROJECT);
    let path = format!("{path}/etlObj.json");
    let result = reader_file(&client, TEST_PROJECT, &path)
        .response_content_type("application/json")
        .send()
        .await?;
    let bytes = result
        .body
        .collect()
        .await
        .map_err(|e| aws_sdk_s3::Error::NotFound(aws_sdk_s3::types::error::NotFound))?
        // NoSuchUpload(crate::types::error::NoSuchUpload),
        .into_bytes();
    let response: &str = std::str::from_utf8(&bytes).map_err(|e| Error::InvalidObjectState(e))?;
    let temp: Value = serde_json::from_str(response).unwrap();

    println!("{:?}", temp);

    /*
        let data = reader_file(&client, TEST_PROJECT, &path).send().await?;
    let stream = data
        .body
        .map(|result| result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
    let stream_reader = StreamReader::new(stream);
    */

    Ok(())
}

/// list objects in a project /shared/diamonds/*.feather among others
fn list_objects(client: &Client, project_id: impl AsRef<str>) -> ListObjectsV2FluentBuilder {
    client
        .list_objects_v2()
        .bucket(BUCKET_NAME.clone())
        .prefix(project_id.as_ref())
}

/// compose getting the collection of diamond objects with parsing the filenames
/// WIP: encapsulate the function that operates on key
async fn list_diamonds(client: &Client, project_id: impl AsRef<str>) -> Result<Vec<String>, Error> {
    // local fmap-like function that points the param that impl Fn to the key value of a S3 object.
    // WIP to encapsulate function application on key
    fn fmap_obj_key(objects: &ListObjectsV2Output) -> Option<Vec<String>> {
        objects.contents().map(|objects| {
            objects
                .iter()
                .filter_map(|object| {
                    (object.key.as_ref())
                        .and_then(|k| k.rsplit_once('/').map(|res| res.1.to_string()))
                })
                .collect::<Vec<_>>()
        })
    }
    let objects = list_diamond_objects(client, project_id).send().await?;
    if let Some(filenames) = fmap_obj_key(&objects) {
        Ok(filenames)
    } else {
        Ok(Vec::new())
    }
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

/// Read data from a S3 file into memory
/// Use case in py:  pd.read_feather(io.BytesIO(resp['Body'].read()))
fn reader_file(
    client: &Client,
    project_id: impl AsRef<str>,
    path: impl AsRef<str>,
) -> GetObjectFluentBuilder {
    client
        .get_object()
        .bucket(BUCKET_NAME.clone())
        .key(mk_key(project_id, path))
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
