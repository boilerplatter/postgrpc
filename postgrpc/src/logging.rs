use std::time::Duration;
use tonic::{codegen::http::Request as HttpRequest, Code};
use tower_http::{
    classify::{GrpcErrorsAsFailures, GrpcFailureClass, SharedClassifier},
    trace::{
        DefaultOnBodyChunk, DefaultOnEos, DefaultOnRequest, DefaultOnResponse, MakeSpan, OnFailure,
        TraceLayer,
    },
};

/// Custom HTTP trace layer for logging gRPC requests appropriately
type LoggingLayer = TraceLayer<
    SharedClassifier<GrpcErrorsAsFailures>,
    PostgrpcSpan,
    DefaultOnRequest,
    DefaultOnResponse,
    DefaultOnBodyChunk,
    DefaultOnEos,
    PostgrpcFailure,
>;

/// Span creation handler for PostgRPC request/response traces
#[derive(Clone)]
pub struct PostgrpcSpan;

impl<B> MakeSpan<B> for PostgrpcSpan {
    fn make_span(&mut self, request: &HttpRequest<B>) -> tracing::Span {
        tracing::info_span!(
            "postgrpc",
            uri = %request.uri(),
            headers = ?request.headers()
        )
    }
}

/// Failure handler with improved gRPC code differentiation
#[derive(Clone)]
pub struct PostgrpcFailure;

impl OnFailure<GrpcFailureClass> for PostgrpcFailure {
    fn on_failure(&mut self, failure: GrpcFailureClass, latency: Duration, _span: &tracing::Span) {
        let latency = format!("{} ms", latency.as_millis());

        match failure {
            GrpcFailureClass::Code(code) => {
                let readable_code = Code::from_i32(code.into());

                match readable_code {
                    Code::NotFound
                    | Code::InvalidArgument
                    | Code::PermissionDenied
                    | Code::Unauthenticated => {
                        tracing::warn!(code = ?readable_code, latency = %latency)
                    }
                    _ => {
                        tracing::error!(code = ?readable_code, latency = %latency)
                    }
                }
            }
            GrpcFailureClass::Error(error) => {
                tracing::error!(error = %error, latency = %latency)
            }
        }
    }
}

/// Create a logging middleware layer for a gRPC service
pub fn create() -> LoggingLayer {
    TraceLayer::new_for_grpc()
        .make_span_with(PostgrpcSpan)
        .on_failure(PostgrpcFailure)
}
