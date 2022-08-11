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
        // TODO: add configurable valuable support
        let headers = request.headers();
        let uri = request.uri();
        let user_agent = headers
            .get("user-agent")
            .map(|header| header.to_str())
            .transpose()
            .unwrap_or_default()
            .unwrap_or_default();

        tracing::info_span!(
            "postgrpc",
            %uri,
            %user_agent,
        )
    }
}

/// Failure handler with improved gRPC code differentiation
#[derive(Clone)]
pub struct PostgrpcFailure;

impl OnFailure<GrpcFailureClass> for PostgrpcFailure {
    fn on_failure(&mut self, failure: GrpcFailureClass, latency: Duration, _span: &tracing::Span) {
        // TODO: add latency unit configuration
        let latency = format!("{}s", latency.as_secs_f32());

        match failure {
            GrpcFailureClass::Code(code) => {
                // TODO: figure out how to include tonic status message data
                let code = Code::from_i32(code.into());
                let description = code.description();

                match code {
                    Code::NotFound
                    | Code::InvalidArgument
                    | Code::PermissionDenied
                    | Code::Unauthenticated
                    | Code::Unimplemented => {
                        tracing::warn!(?code, %description, %latency)
                    }
                    _ => {
                        tracing::error!(?code, %description, %latency)
                    }
                }
            }
            GrpcFailureClass::Error(error) => {
                tracing::error!(%error, %latency)
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
