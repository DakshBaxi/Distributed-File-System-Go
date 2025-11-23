package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Config struct {
    DataDir      string
    IndexFile    string
    ListenAddr   string
    NodeID       string
    SelfAddr     string
    NameNodeAddr string
}

func loadConfig() Config {
    var cfg Config
    flag.StringVar(&cfg.DataDir, "data-dir", "data", "directory to store chunks")
    flag.StringVar(&cfg.IndexFile, "index-file", "index.json", "index file path")
    flag.StringVar(&cfg.ListenAddr, "addr", ":9000", "listen address")
    flag.StringVar(&cfg.NodeID, "id", "dn1", "data node ID")
    flag.StringVar(&cfg.SelfAddr, "self", "http://localhost:9000", "self public address")
    flag.StringVar(&cfg.NameNodeAddr, "namenode", "http://localhost:8000", "namenode address")
    flag.Parse()
    return cfg
}



// indexEntry holds minimal metadata for a stored chnk
type IndexEntry struct {
	Size    int64 `json:"size"`
	Created int64 `json:"created"`
	CheckSum string `json:"checksum"`
}

// Datanode holds the in-memory index and lock //  similar to class in java
type DataNode struct {
	mu    sync.RWMutex
	index map[string]IndexEntry
	cfg Config
}

// similar to construtor class in java
func NewDataNode(cfg Config) *DataNode {
	return &DataNode{
		index: make(map[string]IndexEntry),
		cfg: cfg,
	}
}

// loadIndex loads inddex.json into memory if it exists.
func (dn *DataNode) loadIndex() {
	f, err := os.Open(dn.cfg.IndexFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("index.json not found — starting fresh")
			return
		}
		log.Fatalf("failed to open index file: %v", err)
	}
	defer f.Close()
	// json decoding
	dec := json.NewDecoder(f)
	if err := dec.Decode(&dn.index); err != nil {
		log.Fatalf("failed to decode index.json: %v", err)
	}
	log.Printf("loaded index with %d entries\n", len(dn.index))
}

// saveIndex writes the in memory index to disk automatically
func (dn *DataNode) saveIndex() {
	// locking to protect the index map from concurrent access by multiple goroutines
	dn.mu.RLock()
	defer dn.mu.RUnlock()

	tmp := dn.cfg.IndexFile + ".tmp"
	// creating file
	f, err := os.Create(tmp)
	if err != nil {
		log.Fatalf("failed to create temp index file: %v", err)
		return
	}
	// Encoding json data to file
	enc := json.NewEncoder(f)
	enc.SetIndent("", " ")
	if err := enc.Encode(dn.index); err != nil {
		f.Close()
		os.Remove(tmp)
		log.Print("error in encoding index", err)
		return
	}
	f.Close()
	// renaming file
	if err := os.Rename(tmp, dn.cfg.IndexFile); err != nil {
		log.Printf("error in renaming index file: %v", err)
		return
	}
	log.Println("index saved")

}

// creating put handler func for datanode
func (dn *DataNode) putChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Only Put allowed", http.StatusMethodNotAllowed)
		return
	}
	chunkID := r.URL.Path[len("/chunks/"):]
	if chunkID == "" {
		http.Error(w, "Missing chunk ID", http.StatusBadRequest)
		return
	}
	// creating a file path
	tmpPath := filepath.Join(dn.cfg.DataDir, chunkID+".tmp")
	finalPath := filepath.Join(dn.cfg.DataDir, chunkID)

	// create temp file and stream into it
	tmpF, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		log.Printf("create tmp file error: %v", err)
		return
	}
	//copying data
	n, err := io.Copy(tmpF, r.Body)
	if err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		http.Error(w, "copy error", http.StatusInternalServerError)
		log.Printf("copy error: %v", err)
		return
	}
	tmpF.Close()
	// atomically move into final location
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		http.Error(w, "rename error", http.StatusInternalServerError)
		log.Printf("rename error: %v", err)
		return
	}
	
	// compute sha256
	hasher := sha256.New()
	f, err := os.Open(finalPath)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	if _, err := io.Copy(hasher, f); err != nil {
		f.Close()
		os.Remove(finalPath)
		http.Error(w, "hash error", http.StatusInternalServerError)
		log.Printf("hash error: %v", err)
		return
	}
	f.Close()
	hashInBytes := hasher.Sum(nil)
	hash := fmt.Sprintf("%x", hashInBytes)


	// update index
	dn.mu.Lock()
	dn.index[chunkID] = IndexEntry{
		Size:    n,
		Created: time.Now().Unix(),
		CheckSum: hash,
	}
	dn.mu.Unlock() // This was the missing unlock from our previous discussion

	// goroutine persist index asynchronously (fire and forgot)
	go dn.saveIndex()

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "stored %s (%d bytes)\n", chunkID, n)
	log.Printf("stored chunk %s (%d bytes)\n", chunkID, n)
}

// getting each chunk
func (dn *DataNode) getChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	chunkID := r.URL.Path[len("/chunks/"):]
	if chunkID == "" {
		http.Error(w, "Missing chunk ID", http.StatusBadRequest)
		return
	}
	// creating a file path
	filePath := dn.cfg.DataDir + "/" + chunkID
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		log.Println("Error opening chunk:", err)
		return
	}
	defer f.Close()

	// set helpful headers
	dn.mu.RLock()
	if e, ok := dn.index[chunkID]; ok {
		w.Header().Set("Content-Length", strconv.FormatInt(e.Size, 10))
	}
	dn.mu.RUnlock()
	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := io.Copy(w, f); err != nil {
		// client might disconnect — log but do not crash
		log.Printf("error streaming chunk %s: %v", chunkID, err)
		return
	}
	log.Printf("served chunk %s\n", chunkID)
}

// listHandler returns the index (debug).
func (dn *DataNode) listHandler(w http.ResponseWriter, r *http.Request) {
	dn.mu.RLock()
	defer dn.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	// writing index.json to response
	if err := json.NewEncoder(w).Encode(dn.index); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

// heartbeatLoop (placeholder) logs status; later will Post to NameNode
func (dn *DataNode) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			dn.sendHeartbeat()
		}
	}
}

func (dn *DataNode) sendHeartbeat(){
	 type heartbeatReq struct {
        ID       string   `json:"id"`
        Chunks   []string `json:"chunks"`
        Capacity int64    `json:"capacity"`
    }
	chunks := dn.snapshotChunkIds()
	  reqBody := heartbeatReq{
        ID:       dn.cfg.NodeID,
        Chunks:   chunks,
        Capacity: 0, // TODO: set real capacity later
    }
	  buf := &bytes.Buffer{}
    if err := json.NewEncoder(buf).Encode(&reqBody); err != nil {
        log.Printf("heartbeat: encode error: %v", err)
        return
    }
	   resp, err := http.Post(dn.cfg.NameNodeAddr+"/heartbeat", "application/json", buf)
    if err != nil {
        log.Printf("heartbeat: post error: %v", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        b, _ := io.ReadAll(resp.Body)
        log.Printf("heartbeat: namenode returned %d: %s", resp.StatusCode, string(b))
        return
    }

    log.Printf("heartbeat sent: %d chunks\n", len(chunks))
}

// snapshotChunkIds returns a slice of all chunks Ids currently in the index
func (dn *DataNode) snapshotChunkIds() [] string{
	dn.mu.RLock()
	defer dn.mu.RUnlock()
	ids := make([]string, 0, len(dn.index))
	for id := range dn.index{
		ids = append(ids,id)
	}
	return ids
}

func (dn *DataNode) registerWithNameNode() {
    type regReq struct {
        ID   string `json:"id"`
        Addr string `json:"addr"`
    }

    body := regReq{
        ID:   dn.cfg.NodeID,
        Addr: dn.cfg.SelfAddr,
    }

    buf := &bytes.Buffer{}
    if err := json.NewEncoder(buf).Encode(&body); err != nil {
        log.Printf("register: encode error: %v", err)
        return
    }

    resp, err := http.Post(dn.cfg.NameNodeAddr+"/register", "application/json", buf)
    if err != nil {
        log.Printf("register: post error: %v", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        b, _ := io.ReadAll(resp.Body)
        log.Printf("register: namenode returned %d: %s", resp.StatusCode, string(b))
        return
    }
    log.Printf("registered with NameNode as %s (%s)\n", dn.cfg.NodeID, dn.cfg.ListenAddr)
}


func main() {
	 cfg := loadConfig()
	  if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
        log.Fatal(err)
    }
    dn := NewDataNode(cfg)
	fmt.Println("DataNode starting on", dn.cfg.ListenAddr)
	dn.loadIndex()

	// ensure we register at startup
    dn.registerWithNameNode()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "DataNode is running")
	})
	// Requests
	mux.HandleFunc("/chunks/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			dn.putChunkHandler(w, r) // You had a bug here, I fixed it in a previous response
		case http.MethodGet:
			dn.getChunkHandler(w, r) // And here
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/list", dn.listHandler)
	// context for heartbeat and graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	go dn.heartbeatLoop(ctx)

	srv := &http.Server{
		Addr: dn.cfg.ListenAddr,
		Handler: mux,
	}
	// start server in goroutinea
	go func() {
		log.Printf("DataNode Listning on %s\n", dn.cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	// wait for signals for graceful shutown
	sig := make(chan os.Signal, 1)
	// sig:channel in os, SIGINT:Ctrl+C , SigTerm : termination 
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutdown signal received, saving index and shutting down...")

	// stop heartbeat
	cancel()

	// save index synchronously before exit
	dn.saveIndex()

	// shutdown http server with timeout
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err:=srv.Shutdown(shutCtx); err!=nil{
		log.Printf("server shutdown error :%v",err)
	}
	log.Println("DataNode stopped")
}
