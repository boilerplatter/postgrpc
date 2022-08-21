import { Struct } from 'google-protobuf/google/protobuf/struct_pb'
import { QueryRequest } from './clients/postgres_pb'
import { PostgresClient } from './clients/PostgresServiceClientPb'

// find the note container
const notes = document.querySelector('ul')

// set up the gRPC client and dev tools
const client = new PostgresClient(process.env.POSTGRPC_URL)
const enableDevTools = (window as any).__GRPCWEB_DEVTOOLS__ || (() => { });
enableDevTools([client])

// initialize the request for notes
const query = new QueryRequest()
query.setStatement('SELECT * FROM notes')

// make the request through the generated gRPC-web client
const rows = client.query(query)

rows.on('data', (response: Struct) => {
  const note = document.createElement('li')
  note.textContent = response.toJavaScript().note as string
  notes.appendChild(note)
})

rows.on('error', error => console.error(error))
