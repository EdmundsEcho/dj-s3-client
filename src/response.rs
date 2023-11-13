use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::list_buckets::ListBucketsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;

#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) body: Body,
}

#[derive(Debug)]
pub(crate) enum Body {
    Buckets(ListBucketsOutput),
    Files(ListObjectsV2Output),
    File(GetObjectOutput),
    Empty,
}

#[derive(Debug)]
pub enum Method {
    Read(Body),
    Write(Body),
    List(Body),
}

impl Response {
    #[inline]
    pub fn new(body: Body) -> Response {
        Response { body }
    }
    #[inline]
    pub fn into_body(self) -> Body {
        self.body
    }
}
