import * as jspb from 'google-protobuf'

import * as google_protobuf_struct_pb from 'google-protobuf/google/protobuf/struct_pb';
import * as google_api_annotations_pb from './google/api/annotations_pb';


export class QueryRequest extends jspb.Message {
  getStatement(): string;
  setStatement(value: string): QueryRequest;

  getValuesList(): Array<google_protobuf_struct_pb.Value>;
  setValuesList(value: Array<google_protobuf_struct_pb.Value>): QueryRequest;
  clearValuesList(): QueryRequest;
  addValues(value?: google_protobuf_struct_pb.Value, index?: number): google_protobuf_struct_pb.Value;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): QueryRequest.AsObject;
  static toObject(includeInstance: boolean, msg: QueryRequest): QueryRequest.AsObject;
  static serializeBinaryToWriter(message: QueryRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): QueryRequest;
  static deserializeBinaryFromReader(message: QueryRequest, reader: jspb.BinaryReader): QueryRequest;
}

export namespace QueryRequest {
  export type AsObject = {
    statement: string,
    valuesList: Array<google_protobuf_struct_pb.Value.AsObject>,
  }
}

