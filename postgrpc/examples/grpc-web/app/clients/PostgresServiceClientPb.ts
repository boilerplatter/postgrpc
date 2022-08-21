/**
 * @fileoverview gRPC-Web generated client stub for postgres.v1
 * @enhanceable
 * @public
 */

// GENERATED CODE -- DO NOT EDIT!


/* eslint-disable */
// @ts-nocheck


import * as grpcWeb from 'grpc-web';

import * as google_protobuf_struct_pb from 'google-protobuf/google/protobuf/struct_pb';
import * as postgres_pb from './postgres_pb';


export class PostgresClient {
  client_: grpcWeb.AbstractClientBase;
  hostname_: string;
  credentials_: null | { [index: string]: string; };
  options_: null | { [index: string]: any; };

  constructor (hostname: string,
               credentials?: null | { [index: string]: string; },
               options?: null | { [index: string]: any; }) {
    if (!options) options = {};
    if (!credentials) credentials = {};
    options['format'] = 'text';

    this.client_ = new grpcWeb.GrpcWebClientBase(options);
    this.hostname_ = hostname;
    this.credentials_ = credentials;
    this.options_ = options;
  }

  methodInfoQuery = new grpcWeb.MethodDescriptor(
    '/postgres.v1.Postgres/Query',
    grpcWeb.MethodType.SERVER_STREAMING,
    postgres_pb.QueryRequest,
    google_protobuf_struct_pb.Struct,
    (request: postgres_pb.QueryRequest) => {
      return request.serializeBinary();
    },
    google_protobuf_struct_pb.Struct.deserializeBinary
  );

  query(
    request: postgres_pb.QueryRequest,
    metadata?: grpcWeb.Metadata) {
    return this.client_.serverStreaming(
      this.hostname_ +
        '/postgres.v1.Postgres/Query',
      request,
      metadata || {},
      this.methodInfoQuery);
  }

}

