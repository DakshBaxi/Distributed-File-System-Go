package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// const (
// 	nodesFile = "./tmp/nodes.json"
// 	filesFile = "./tmp/files.json"
// 	addr      = ":8000"
// )




type Config struct {
    ListenAddr string
    MetaDir    string
}

func loadConfig() Config {
    var cfg Config
    flag.StringVar(&cfg.ListenAddr, "addr", ":8000", "listen address")
    flag.StringVar(&cfg.MetaDir, "meta-dir", "./meta", "directory for files/nodes metadata")
    flag.Parse()
    return cfg
}

// Datanode info tracked by Namenode
type DataNodeInfo struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	LastSeen int64    `json:"last_seen"`
	Chunks   []string `json:"chunks,omitempty"`
}

// Data structures for FIle metadata
type ChunkPlacement struct {
	ID        string   `json:"id"`
	Locations []string `json:"locations"`
}
type FileMeta struct {
	Path   string           `json:"path"`
	Chunks []ChunkPlacement `json:"chunks"`
	Replication int 		`json:"replication"`
}

// NameNode
type NameNode struct {
	mu sync.RWMutex
	nodes map[string]*DataNodeInfo // nodeId ->info
	files map[string]*FileMeta	// path -> meta
	nodeIDs	[]string	// helper for round-robin
	rrIdx int
	cfg Config
}

type UnderReplicatedChunk struct {
    FilePath  string   `json:"file_path"`
    ChunkID   string   `json:"chunk_id"`
    Want      int      `json:"want"`
    Have      int      `json:"have"`
    Locations []string `json:"locations"` // current healthy locations
}


func NewNameNode(cfg Config) *NameNode{
	return &NameNode{
		nodes : make(map[string]*DataNodeInfo),
		files : make(map[string]*FileMeta),
		cfg: cfg,
	}
}

// register datnodes handler
func (nn *NameNode) registerHandler(w http.ResponseWriter,r *http.Request){
	var req struct {
		ID string `json:"id"`
		Addr string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err!=nil{
		http.Error(w,"bad request",http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.Addr == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	nn.mu.Lock()
	defer nn.mu.Unlock()
	nn.nodes[req.ID] = &DataNodeInfo{
		ID: req.ID,
		Addr: req.Addr,
		LastSeen: time.Now().Unix(),
	}
	// update nodeIDs list
	found:=slices.Contains(nn.nodeIDs, req.ID)
	if !found{
		nn.nodeIDs = append(nn.nodeIDs, req.ID)
	}
	// writing nodesFile
	nn.persistNodes()
	json.NewEncoder(w).Encode(map[string]bool{"ok":true})
}

// heartbeat handler
func (nn *NameNode) heartbeatHandler(w http.ResponseWriter,r *http.Request){
	var req struct{
		ID string `json:"id"`
		Chunks []string `json:"chunks"`
		Capacity int64 `json:"capacity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err!= nil{
		http.Error(w,"bad request",http.StatusBadRequest)
		return
	}
	if req.ID == ""{
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	nn.mu.Lock()
	defer nn.mu.Unlock()
	 node, ok := nn.nodes[req.ID]; 
	 if !ok{
		http.Error(w, "node not registered", http.StatusBadRequest)
		return
	}
	node.LastSeen = time.Now().Unix()
	node.Chunks = req.Chunks
	nn.persistNodes()
	// update file->chunk->node location map (we're keeping file meta minimal for MVP)
	// For now: we will nor backfil files map from heartbeats.
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// persist nodes and files(simple atomic write)
func (nn *NameNode) persistNodes(){
	nodesFile := filepath.Join(nn.cfg.MetaDir, "nodes.json")
	f,err := os.Create(nodesFile+".tmp")
	if err != nil {
		log.Println("persistNodes create:", err)
		return
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(nn.nodes); err != nil {
		log.Println("persistNodes encode:", err)
		f.Close()
		return
	}
	f.Close()
	os.Rename(nodesFile+".tmp", nodesFile)
}

// helper: chose k distinct datanodes(round-robin)
func (nn *NameNode) chooseDataNodes(k int) []string {
    nn.mu.Lock()
    defer nn.mu.Unlock()

    if len(nn.nodeIDs) == 0 {
        return nil
    }

    if k > len(nn.nodeIDs) {
        k = len(nn.nodeIDs)
    }

    res := make([]string, 0, k)
    for len(res) < k {
        id := nn.nodeIDs[nn.rrIdx%len(nn.nodeIDs)]
        nn.rrIdx++
        if !slices.Contains(res, id) {
            res = append(res, id)
        }
    }
    return res
}

// createfile metadata and placements
func (nn *NameNode) createFileHandler(w http.ResponseWriter,r *http.Request){
	var req struct{
		Path string `json:"path"`
		NumChunks int `json:"num_chunks"`
		Replication int `json:"replication"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Path == "" || req.NumChunks <= 0 {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	if req.Replication <= 0 {
		req.Replication = 2 // default
	}
	placements := make([]ChunkPlacement,0,req.NumChunks)
	for i:=0 ; i<req.NumChunks;i++{
		chunkID := uuid.New().String()
		nodeIDs := nn.chooseDataNodes(req.Replication)
		// cinvert nodeIs-> addresses
		locs := []string{}
		nn.mu.RLock()
		// locs : location of datnodes
		for _,nid :=range nodeIDs{
			if node,ok := nn.nodes[nid]; ok{
				locs = append(locs, node.Addr)
			}
			}
			nn.mu.RUnlock()
		placements = append(placements, ChunkPlacement{ID: chunkID, Locations: locs})	
	}
	meta := &FileMeta{
		Path: req.Path,
		 Chunks: placements,
		 Replication: req.Replication, // store desiredreplication factor
		}
	nn.mu.Lock()
	nn.files[req.Path] = meta
	nn.mu.Unlock()
	nn.persistFiles()

	json.NewEncoder(w).Encode(map[string]interface{}{"chunks": placements})
}

// get file metadata
func (nn *NameNode) getFileHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	nn.mu.RLock()
	defer nn.mu.RUnlock()
	if meta, ok := nn.files[path]; ok {
		json.NewEncoder(w).Encode(meta)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// list nodes handler
func (nn *NameNode) listNodesHandler(w http.ResponseWriter, r *http.Request) {
    nn.mu.RLock()
    defer nn.mu.RUnlock()
    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(nn.nodes); err != nil {
        http.Error(w, "internal error", http.StatusInternalServerError)
    }
}

// underReplication
func (nn *NameNode) underReplicatedHandler(w http.ResponseWriter, r *http.Request) {
    // e.g. consider nodes dead if no heartbeat in last 15 seconds
    const timeout = 15 * time.Second

    chunks := nn.findUnderReplicated(timeout)

    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(chunks); err != nil {
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }
}

func (nn *NameNode) healHandler(w http.ResponseWriter,r *http.Request){
	if r.Method != http.MethodPost{
		   http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
	}
	const timeout = 15 * time.Second
    healed := nn.healUnderReplicated(timeout)
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(healed)
}


// persist files helper
func (nn *NameNode) persistFiles() {
	filesFile := 	filepath.Join(nn.cfg.MetaDir, "files.json")
	f, err := os.Create(filesFile + ".tmp")
	if err != nil {
		log.Println("persistFiles create:", err)
		return
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(nn.files); err != nil {
		log.Println("persistFiles encode:", err)
		f.Close()
		return
	}
	f.Close()
	os.Rename(filesFile+".tmp", filesFile)
}

func (nn *NameNode) loadState() {
	nodesFile := filepath.Join(nn.cfg.MetaDir, "nodes.json")
	filesFile := 	filepath.Join(nn.cfg.MetaDir, "files.json")
	if _, err := os.Stat(nodesFile); err == nil {
		if b, err := os.ReadFile(nodesFile); err == nil {
			var nodes map[string]*DataNodeInfo
			if err := json.Unmarshal(b, &nodes); err == nil {
				nn.nodes = nodes
				for id := range nodes {
					nn.nodeIDs = append(nn.nodeIDs, id)
				}
			}
		}
	}
	if _, err := os.Stat(filesFile); err == nil {
		if b, err := os.ReadFile(filesFile); err == nil {
			var files map[string]*FileMeta
			if err := json.Unmarshal(b, &files); err == nil {
				nn.files = files
			}
		}
	}
}



func (nn *NameNode) findUnderReplicated(timeout time.Duration) []UnderReplicatedChunk{

	nn.mu.RLock()
	defer nn.mu.RUnlock()
	// // 1) determine healthy nodes (recent heartbeat)
	// healthyNodes := make(map[string]*DataNodeInfo) 
	// for id,n := range nn.nodes{
	// 	if now - n.LastSeen<=int64(timeout.Seconds()){
	// 		healthyNodes[id]=n
	// 	}
	// }

	// // 2) build chunkId -> []nodeId map from healthy nodes
	// chunkToNodes := make(map[string][]string)
	// for id,n := range healthyNodes{
	// 	for _,cid := range n.Chunks{
	// 		chunkToNodes[cid] = append(chunkToNodes[cid], id)
	// 	}
	// }

	healthyNodes, chunkToNodes := nn.buildClusterView(timeout)


	var result []UnderReplicatedChunk
	for path,meta := range nn.files{
		// desired replication: from meta, or fallback if old data
		desired := meta.Replication
		  if desired <= 0 {
            if len(meta.Chunks) > 0 {
                desired = len(meta.Chunks[0].Locations)
            } else {
                desired = 1
            }
        }
		for _,ch := range meta.Chunks{
			nodeIDs := chunkToNodes[ch.ID]
			 have := len(nodeIDs)
			 if have >= desired{
				 continue	//fully replicated or over replicated
			 }
			   // convert nodeIDs -> addresses for debug
            locs := make([]string, 0, len(nodeIDs))
            for _, nid := range nodeIDs {
                if n, ok := healthyNodes[nid]; ok {
                    locs = append(locs, n.Addr)
                }
            }
			  result = append(result, UnderReplicatedChunk{
                FilePath:  path,
                ChunkID:   ch.ID,
                Want:      desired,
                Have:      have,
                Locations: locs,
            })
		}
	}
	return result
}

// returns: healthyNodes, chunkID -> []nodeID
func (nn *NameNode) buildClusterView(timeout time.Duration) (map[string]*DataNodeInfo, map[string][]string) {
    now := time.Now().Unix()

    healthy := make(map[string]*DataNodeInfo)
    chunkToNodes := make(map[string][]string)

    for id, n := range nn.nodes {
        if now-n.LastSeen <= int64(timeout.Seconds()) {
            healthy[id] = n
        }
    }
    for id, n := range healthy {
        for _, cid := range n.Chunks {
            chunkToNodes[cid] = append(chunkToNodes[cid], id)
        }
    }
    return healthy, chunkToNodes
}

func (nn *NameNode) healUnderReplicated(timeout time.Duration) []UnderReplicatedChunk{
	under := nn.findUnderReplicated(timeout)
	if len(under)==0{
		return nil
	}
	healthy,chunkToNodes := nn.buildClusterView(timeout)
	// Build nodeID -> *DataNodeInfo map already in `healthy`
    // Also build quick lookup: chunkID -> set of nodeIDs that have it
    // chunkToNodes already gives that.

	for _,ur := range under{
		nn.mu.Lock()
		meta,ok := nn.files[ur.FilePath]
		if !ok{
			nn.mu.Unlock()
			continue
		}
		// find chunk in meta
		var chunkIdx int = -1
		for i,ch := range meta.Chunks{
			if ch.ID == ur.ChunkID{
				chunkIdx = i
				break
			}
		}
		if chunkIdx == -1 {
            nn.mu.Unlock()
            continue
        }
		  // pick a source node (one that currently has this chunk)
        nodeIDsWithChunk := chunkToNodes[ur.ChunkID]
        if len(nodeIDsWithChunk) == 0 {
            nn.mu.Unlock()
            log.Printf("heal: no source nodes for chunk %s\n", ur.ChunkID)
            continue
        }
        srcID := nodeIDsWithChunk[0]
        srcNode, ok := healthy[srcID]
        if !ok {
            nn.mu.Unlock()
            continue
        }
        sourceAddr := srcNode.Addr

		// pick a target node : healthy node that does not have this chunk
		var target *DataNodeInfo
        for id, n := range healthy {
            // skip nodes that already have chunk
            has := false
            for _, cid := range chunkToNodes[ur.ChunkID] {
                if cidNodeID := cid; cidNodeID == id {
                    has = true
                    break
                }
            }
            if has {
                continue
            }
            target = n
            break
        }

        if target == nil {
            nn.mu.Unlock()
            log.Printf("heal: no target node available for chunk %s\n", ur.ChunkID)
            continue
        }

        targetAddr := target.Addr
        chunkID := ur.ChunkID
        filePath := ur.FilePath

        nn.mu.Unlock()

        // Call target datanode to replicate
        log.Printf("heal: replicating chunk %s of %s from %s to %s\n",
            chunkID, filePath, sourceAddr, targetAddr)

        body := map[string]string{
            "chunk_id": chunkID,
            "source":   sourceAddr,
        }
        buf := &bytes.Buffer{}
        if err := json.NewEncoder(buf).Encode(body); err != nil {
            log.Printf("heal: encode error: %v", err)
            continue
        }

        resp, err := http.Post(strings.TrimRight(targetAddr, "/")+"/internal/replicate",
            "application/json", buf)
        if err != nil {
            log.Printf("heal: replicate request failed: %v", err)
            continue
        }
        resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            log.Printf("heal: target returned %d for chunk %s\n", resp.StatusCode, chunkID)
            continue
        }

        // Update metadata: add targetAddr to chunk locations if not already there
        nn.mu.Lock()
        meta, ok = nn.files[filePath]
        if ok {
            ch := &meta.Chunks[chunkIdx]
            found := false
            for _, loc := range ch.Locations {
                if loc == targetAddr {
                    found = true
                    break
                }
            }
            if !found {
                ch.Locations = append(ch.Locations, targetAddr)
            }
        }
        nn.mu.Unlock()
        nn.persistFiles()
    }

    return under

	}

func (nn *NameNode) startAutoHeal() {
    const interval = 10 * time.Second
    const timeout = 15 * time.Second

    go func() {
        for {
            time.Sleep(interval)

            // Find & Fix under-replication
            ur := nn.healUnderReplicated(timeout)
            if len(ur) > 0 {
                log.Printf("AutoHeal: healed %d chunks\n", len(ur))
            }
        }
    }()
}




func main() {
	cfg := loadConfig()
	 if err := os.MkdirAll(cfg.MetaDir, 0755); err != nil {
        log.Fatal(err)
    }
	nn := NewNameNode(cfg)
	nn.loadState()
	nn.startAutoHeal()
	http.HandleFunc("/register", nn.registerHandler)   // POST
	http.HandleFunc("/heartbeat", nn.heartbeatHandler) // POST
	http.HandleFunc("/files", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			nn.createFileHandler(w, r)
		case http.MethodGet:
			nn.getFileHandler(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/under_replicated", nn.underReplicatedHandler)
	http.HandleFunc("/nodes", nn.listNodesHandler)
	http.HandleFunc("/heal", nn.healHandler)
	log.Printf("NameNode listening on %s\n", nn.cfg.ListenAddr)
	if err := http.ListenAndServe(nn.cfg.ListenAddr, nil); err != nil {
		log.Fatal(err)
	}
}