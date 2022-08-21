const { exec } = require('child_process')
const npm = require('npm')
const path = require('path')

// load npm configuration
npm.load(() => {
  // get protoc plugin paths
  const POSTGRES_RESOURCE_PROTO_PATH = path.normalize('../../../proto')
  const OUT_DIR = path.normalize('./clients')
  const PROTOC_GEN_GRPC = path.join(npm.bin, 'grpc_tools_node_protoc')
  const PROTOC_GEN_GRPC_WEB = path.join(npm.bin, 'protoc-gen-grpc-web')

  // get protoc command
  const command = `${PROTOC_GEN_GRPC} \
      --plugin=protoc-gen-grpc-web_proto=${PROTOC_GEN_GRPC_WEB} \
      --js_out=import_style=commonjs,binary:${OUT_DIR} \
      --grpc-web_out=import_style=typescript,mode=grpcwebtext:${OUT_DIR} \
      --proto_path=${POSTGRES_RESOURCE_PROTO_PATH} \
      postgres.proto google/api/annotations.proto google/api/http.proto`

  // run the protoc command in a child process
  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`Exec error: ${error}`)
      process.exit(1)
    }

    if (stderr) {
      console.error(`protoc error: ${stderr}`)
      process.exit(1)
    }

    console.log(stdout)
  })
})
