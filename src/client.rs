// use color_eyre::Result;
use dotenv::dotenv;
// use eyre::WrapErr;

use aws_sdk_s3::config::{AppName, Region};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::Config as S3Config;

use crate::error::{Error, Result};
use crate::response::{Body, Method, Response};
use crate::sync_wrapper::SyncWrapper;

use std::fmt;
use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

/// Async `Client` to make Requests with.
///
/// Requests include Method: Read/Write, Filename: String.
/// Request has a Client.
///
/// Default settings with ability to tweak using `Client::builder()`.
///
const APP_NAME: &str = "TestAndControl";
const ETL_OBJ_FILENAME: &str = "etlObj.json";
const TEST_PROJECT: &str = "fef57333-67c0-4825-9765-5bf48f3d5f63";

#[derive(Debug)]
pub(crate) struct Request {
    pub(crate) method: Method,
    pub(crate) filename: String,
    pub(crate) content_type: Option<String>,
}
impl Request {
    pub(crate) fn new(
        method: Method,
        filename: impl AsRef<str>,
        content_type: Option<String>,
    ) -> Self {
        Self {
            method,
            filename: filename.as_ref().to_string(),
            content_type,
        }
    }
}

pub struct ResponseFuture {
    inner: SyncWrapper<Pin<Box<dyn Future<Output = crate::Result<Response>> + Send>>>,
}
impl Future for ResponseFuture {
    type Output = crate::Result<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.get_mut().as_mut().poll(cx)
    }
}
impl ResponseFuture {
    fn new<F>(value: F) -> Self
    where
        F: Future<Output = crate::Result<Response>> + Send + 'static,
    {
        Self {
            inner: SyncWrapper::new(Box::pin(value)),
        }
    }
}

impl fmt::Debug for ResponseFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Future<Response>")
    }
}

pub type Filename = String;

pub struct Client {
    pub(crate) inner: S3Client,
    pub(crate) config: IOConfig,
}

#[must_use]
pub struct ClientBuilder {
    config: IOConfig,
}

impl ClientBuilder {
    pub async fn build(self) -> Result<Client> {
        let config = IOConfig::from_env().await;
        let client = S3Client::from_conf(config.io_cfg.clone());

        Ok(Client {
            inner: client,
            config,
        })
    }
}

impl Client {
    pub fn list_files(&self, filename: impl AsRef<str>) -> ResponseFuture {
        let req = Request::new(Method::List(Body::Files), filename, None);
        self.request(req)
    }

    pub fn read(&self, filename: impl AsRef<str>, content_type: String) -> ResponseFuture {
        let req = Request::new(Method::Read(Body::File), filename, Some(content_type));
        self.request(req)
    }

    pub fn write(&self, filename: impl AsRef<str>) -> ResponseFuture {
        let req = Request::new(Method::Write(Body::Empty), filename, None);
        self.request(req)
    }

    pub fn request(&self, req: Request) -> ResponseFuture {
        // engage the S3 request
        ResponseFuture::new(todo!())
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("Client");
        builder.finish()
    }
}

impl tower_service::Service<Request> for Client {
    type Response = Response;
    type Error = crate::error::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.request(req)
    }
}

impl tower_service::Service<Request> for &'_ Client {
    type Response = Response;
    type Error = crate::error::Error;
    type Future = ResponseFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.request(req)
    }
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("ClientBuilder");
        builder.finish()
    }
}

/// Augment the SdkConfig with values that are reused across client requests
struct IOConfigBuilder {
    etl_obj_filename: Option<String>,
    app_name: Option<String>,
    bucket_name: Option<String>,
    io_cfg: Option<S3Config>,
    test_project_id: Option<String>,
}

/// IO for the TNC App
struct IOConfig {
    error: Option<crate::error::Error>,
    etl_obj_filename: String,
    app_name: String,
    bucket_name: String,
    io_cfg: S3Config,
    test_project_id: Option<String>,
}

impl IOConfig {
    pub async fn from_env() -> Self {
        dotenv().ok();
        init_tracer();

        info!("Loading configuration");

        let bucket_name = std::env::var("S3_BUCKET_NAME").expect("The bucket name must be set");
        let endpoint_url = std::env::var("S3_HOST_BASE").expect("The host base must be set");
        let test_project_id = std::env::var("TEST_PROJECT_ID").ok();

        let sdk_config = ::aws_config::load_from_env().await;
        let sdk_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .app_name(AppName::new("TestAndControl".to_string()).unwrap())
            .build();

        debug!("Sdk Config: {:?}", sdk_config);
        IOConfig {
            error: None,
            etl_obj_filename: ETL_OBJ_FILENAME.to_string(),
            app_name: APP_NAME.to_string(),
            bucket_name,
            io_cfg: sdk_config,
            test_project_id,
        }
    }
}

fn init_tracer() {
    #[cfg(debug_assertions)]
    let tracer = tracing_subscriber::fmt();
    #[cfg(not(debug_assertions))]
    let tracer = tracing_subscriber::fmt().json();

    tracer.with_env_filter(EnvFilter::from_default_env()).init();
}
