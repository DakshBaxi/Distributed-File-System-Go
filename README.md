DFS-Go — A Distributed File System in Go (Inspired by GFS/HDFS)

DFS-Go is a lightweight, educational distributed file system built entirely in Go.
It implements the core features of systems like Google File System (GFS) and Hadoop HDFS, including:

Chunked file storage

NameNode metadata manager

Multiple DataNodes for distributed storage

Replication for fault tolerance

Heartbeats for node health & chunk inventory

Streaming uploads/downloads

Client-side failover

Persistent metadata and chunk indexes

This project is designed for learning distributed systems, networking, and Go, while staying clean and minimal enough for others to run and contribute.

✨ Features
✅ Core:

Chunk-based file storage (default 4MB per chunk)

Upload & download files with streaming (no full-buffer)

Replication across multiple DataNodes

Auto-registration of DataNodes with NameNode

Heartbeats: DataNodes send chunk list + health every 5 seconds

Client-side failover (if one DataNode is down, use replica)

Persistent disk storage for NameNode and DataNode

Clean JSON-based HTTP APIs

🔧 Technologies:

Go (no external dependencies beyond stdlib)

HTTP-based protocol (easy debugging with curl)

JSON metadata

Goroutines for concurrency

Locks & sync primitives for safety

🧠 Architecture Overview

DFS-Go consists of three main components:

   +--------------------------------------------------------+
   |                        NameNode                        |
   |--------------------------------------------------------|
   | - Tracks files → chunks → DataNode locations           |
   | - Stores metadata in files.json & nodes.json           |
   | - Provides upload planning (chunk placement)           |
   | - Receives heartbeats from DataNodes                   |
   | - Simplified replica placement (round-robin)           |
   +--------------------------------------------------------+
         ^                                      ^
         |  (GET /files?path=...)               | (POST /heartbeat)
         |                                      |
         |                                      |
   +-------------------+               +-------------------+
   |     DataNode 1    |               |     DataNode 2    |
   |-------------------|               |-------------------|
   | - Stores chunks   |               | - Stores chunks   |
   | - Keeps index     |               | - Keeps index     |
   | - Serves chunks   |               | - Serves chunks   |
   | - Sends heartbeats|               | - Sends heartbeats|
   +-------------------+               +-------------------+
         ^        ^                           ^       ^
         |        |                           |       |
         |    (HTTP PUT/GET chunks)           |       |
         +------------------+-------------------+-----+
                            |
                    +----------------+
                    |     Client     |
                    |----------------|
                    | Upload file    |
                    | Download file  |
                    | Retry replicas |
                    +----------------+

📦 How It Works
1. Upload Flow

Client reads local file & splits into chunks

Client asks NameNode:
POST /files { path, num_chunks, replication }

NameNode:

Creates chunk IDs

Chooses DataNodes for replication

Stores metadata

Returns placement plan

Client uploads each chunk to all assigned DataNodes

DataNodes write chunk → update index → persist metadata

2. Download Flow

Client requests metadata from NameNode (GET /files?path=...)

For each chunk:

Try each DataNode in locations list

Download from first healthy node

Reassemble chunks into output file

Verify integrity (optional: checksum match)

3. Heartbeats

Every DataNode sends:

{
  "id": "dn1",
  "chunks": ["chunkA", "chunkB"],
  "capacity": 0
}


NameNode updates:

LastSeen timestamp

Node’s chunk inventory

Cluster health

🚀 Quickstart
1. Clone the repo
git clone https://github.com/<your-username>/dfs-go
cd dfs-go

2. Start NameNode
go run ./cmd/namenode -addr=":8000" -meta-dir="./meta"

3. Start DataNodes

Run each in separate terminals:

DataNode 1:
go run ./cmd/datanode \
  -id=dn1 \
  -addr=":9000" \
  -self="http://localhost:9000" \
  -namenode="http://localhost:8000" \
  -data-dir="./data1"

DataNode 2:
go run ./cmd/datanode \
  -id=dn2 \
  -addr=":9001" \
  -self="http://localhost:9001" \
  -namenode="http://localhost:8000" \
  -data-dir="./data2"


Confirm cluster:

curl http://localhost:8000/nodes

4. Upload File
go run ./cmd/client upload ./test/big.bin user/test/big.bin

5. Download File Back
go run ./cmd/client download user/test/big.bin restored.bin


Verify:

sha256sum ./test/big.bin restored.bin

🛠 Directory Structure
dfs-go/
  cmd/
    namenode/    # NameNode binary
    datanode/    # DataNode binary
    client/      # CLI for upload/download
  internal/
    (optional packages: protocol, replication, config)
  meta/          # NameNode metadata (nodes.json, files.json)
  data1/         # DataNode 1 chunks
  data2/         # DataNode 2 chunks
  README.md
  go.mod

🛤 Roadmap (Future Updates)

These are planned features for upcoming releases:

✅ Short-term

Re-replication when a DataNode dies

Report “under-replicated” chunks in NameNode UI / API

Optional checksum validation during download

File/dir deletion (with cascading chunk deletion)

Improve client CLI (ls, rm, info commands)

🚀 Medium-term

Smarter chunk placement (load balancing, random, hash-based)

Background replication job inside NameNode

Configurable chunk size

HTTP/2 or gRPC transport mode

Simple web dashboard for cluster monitoring

🧪 Advanced / Research

Content addressing & deduplication

Multi-NameNode (leader election)

Raft or Etcd-backed metadata store

Access control (users, permissions)

Caching hot files on nearest DataNode

🤝 Contributing

Contributions are welcome!

Fork the repo

Create a feature branch

Describe your changes clearly

Submit a PR

Please open issues for bugs or feature requests.

📄 License

This project is licensed under the MIT License.
You are free to use, modify, distribute, and build on top of it.

⭐️ Support the Project

If you find this project useful or educational, consider giving it a star ⭐ on GitHub
and share it with others who are learning distributed systems!