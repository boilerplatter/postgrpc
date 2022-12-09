// GENERATED FILE: recompile with protoc; do not edit directly
#[allow(clippy::redundant_static_lifetimes)]
pub const QUERY: &'static ::prost::ExtensionImpl<Query> = &::prost::ExtensionImpl::<Query> {
    extendable_type_id: ".google.protobuf.MethodOptions",
    field_tag: 72295729,
    proto_int_type: ::prost::ProtoIntType::Default,
    _phantom: ::core::marker::PhantomData {},
};

/// Registers all protobuf extensions defined in this module
#[allow(dead_code)]
pub fn register_extensions(registry: &mut ::prost::ExtensionRegistry) {
    registry.register(QUERY);
}
/// SQL Query validation configuration for an rpc method
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Query {
    #[prost(oneof = "query::Source", tags = "1, 2")]
    pub source: ::core::option::Option<query::Source>,
}

/// Nested message and enum types in `Query`.
pub mod query {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Source {
        /// file path of external .sql file
        #[prost(string, tag = "1")]
        File(::prost::alloc::string::String),
        /// inlined sql query
        #[prost(string, tag = "2")]
        Sql(::prost::alloc::string::String),
    }
}
