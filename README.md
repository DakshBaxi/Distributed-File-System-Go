# DFS-Go

A lightweight, educational distributed file system implemented in Go. Inspired by GFS/HDFS, DFS-Go demonstrates chunked storage, a NameNode metadata service, multiple DataNodes, replication, and simple HTTP-based APIs for learning distributed-systems concepts.

## Features

- Chunk-based storage (default chunk size: 4 MiB)
- Streaming upload/download (no full-buffer)
- Replication for fault tolerance
- NameNode: metadata, placement planning, heartbeat handling
- DataNode: chunk storage, index, chunk serving, periodic heartbeats
- Client-side failover (tries replicas when a node is down)
- Persistent metadata on disk (JSON files)
- Pure Go, standard library only, HTTP + JSON APIs

## Architecture (high-level)

NameNode
- Tracks files → chunks → DataNode locations
- Stores metadata (files.json, nodes.json)
- Provides upload placement plans and receives heartbeats

DataNode
- Stores chunk files and maintains index
- Serves chunk PUT/GET endpoints
- Sends heartbeat with chunk inventory

Client
- Requests placement from NameNode
- Uploads chunks to assigned DataNodes
- Downloads by querying NameNode and contacting DataNodes

Simple ASCII flow:
```
Client <--> NameNode
    |          ^
    |          | (placement / metadata)
    v          |
DataNodes (store & serve chunks, send heartbeats)
```

## How it works

1. Upload
- Client splits file into chunks.
- POST /files { path, num_chunks, replication } to NameNode.
- NameNode creates chunk IDs and returns placement plan.
- Client uploads each chunk to the assigned DataNodes.

2. Download
- GET /files?path=... from NameNode to get chunk list and locations.
- Client tries DataNodes in order until a chunk is fetched.
- Reassemble chunks into the final file.

3. Heartbeats
- Each DataNode periodically POSTs inventory:
  {
     "id": "dn1",
     "chunks": ["chunkA","chunkB"],
     "capacity": 0
  }
- NameNode updates last-seen and cluster state.

## Quickstart

Clone:
```
git clone https://github.com/<your-username>/dfs-go
cd dfs-go
```

Start NameNode:
```
go run ./cmd/namenode -addr=":8000" -meta-dir="./meta"
```

Start DataNodes (separate terminals):
```
# DataNode 1
go run ./cmd/datanode \
  -id=dn1 \
  -addr=":9000" \
  -self="http://localhost:9000" \
  -namenode="http://localhost:8000" \
  -data-dir="./data1"

# DataNode 2
go run ./cmd/datanode \
  -id=dn2 \
  -addr=":9001" \
  -self="http://localhost:9001" \
  -namenode="http://localhost:8000" \
  -data-dir="./data2"
```

Verify nodes:
```
curl http://localhost:8000/nodes
```

Upload:
```
go run ./cmd/client upload ./test/big.bin user/test/big.bin
```

Download:
```
go run ./cmd/client download user/test/big.bin restored.bin
```

Verify integrity:
```
sha256sum ./test/big.bin restored.bin
```

## Directory layout
```
dfs-go/
  cmd/
     namenode/
     datanode/
     client/
  internal/      # packages (protocol, replication, etc.)
  meta/           # NameNode metadata (files.json, nodes.json)
  data1/ data2/   # DataNode storage
  README.md
  go.mod
```

## Roadmap (planned)
Short-term
- Re-replication when a DataNode dies
- Report under-replicated chunks
- Optional checksum validation
- File/dir deletion and CLI enhancements

Medium-term
- Smarter placement strategies
- Background replication jobs
- Configurable chunk size, HTTP/2 or gRPC transport
- Simple web dashboard

Research
- Content-addressing & deduplication
- Multi-NameNode / consensus (Raft/Etcd)
- Access control, caching strategies

## Contributing
- Fork, create a feature branch, and submit a PR
- Open issues for bugs or feature requests

## License
MIT — free to use, modify, and distribute.

Feel free to open issues or PRs for improvements.
