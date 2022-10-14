# `protoc-gen-postgrpc`


WORKFLOW:
0. migrate the database to the correct schema (if needed)
1. start with a proto file describing the service wanted
2. configure the SQL -> proto transcoder with an option pointing to the requested SQL query
3. generate validated postgrpc-compatible services through a protoc plugin (a la protoc-gen-tonic)
4. expose protoc plugin internals to postgrpc-lib through build.rs, rebuilding on SQL or proto file change
5. for those building postgrpc-bin from source, use macros to configure built services dynamically
Extra: is it possible to compile postgrpc-compatible services as dylibs from proto or SQL files directly? 
