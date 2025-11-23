package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
)

const (
	nameNodeAddr = "http://localhost:8000"
	chunkSize    = 4 * 1024 * 1024 // 4 MB per chunk for testing
)

type ChunkPlacement struct {
	ID        string   `json:"id"`
	Locations []string `json:"locations"`
}

type FileMeta struct {
	Path   string           `json:"path"`
	Chunks []ChunkPlacement `json:"chunks"`
}

func main() {
	if len(os.Args) < 4 {
	fmt.Println("Usage: client <upload|download> <src> <dst>")
		os.Exit(1)
	}
	method := os.Args[1]

	switch method {
	case "upload":
		upload()
	case "download":
		download()
	default:
		fmt.Print("This Method is not allowed")
	}

}

func download() {
	dfsPath := os.Args[2]
	outPath := os.Args[3]
	meta, err := fetchFileMeta(dfsPath)
	if err != nil {
		log.Fatalf("fetch file meta: %v", err)
	}
	if len(meta.Chunks) == 0 {
		log.Fatalf("no chunks for file %s", dfsPath)
	}
	// create/truncate output file
	outFile, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create output file: %v", err)
	}
	defer outFile.Close()
	for i, ch := range meta.Chunks {
		if len(ch.Locations) == 0 {
			log.Fatalf("no locations for chunk %s", ch.ID)
		}
		var lastErr error
    success := false

    for _, loc := range ch.Locations {
        base := trimTrailingSlash(loc)
        url := fmt.Sprintf("%s/chunks/%s", base, ch.ID)

        fmt.Printf("Downloading chunk %d/%d: id=%s from %s\n",
            i+1, len(meta.Chunks), ch.ID, url)

        if err := downloadChunk(url, outFile); err != nil {
            log.Printf("failed from %s: %v", url, err)
            lastErr = err
            continue
        }
        success = true
        break
    }
	 if !success {
        log.Fatalf("all replicas failed for chunk %s: last error: %v", ch.ID, lastErr)
    }
	}
	fmt.Println("Download complete:", outPath)
}

func fetchFileMeta(path string) (*FileMeta, error) {
	q := url.QueryEscape(path) // <-- important
  	url := nameNodeAddr + "/files?path=" + q
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("namenode returned %d: %s", resp.StatusCode, string(b))
	}
	var meta FileMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// stream chunk from Datanode into outfile (append mode via current offset)
func downloadChunk(url string, out *os.File) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("datanode returned %d: %s", resp.StatusCode, string(b))
	}

	// copy directly to output file, appending sequentially
	_, err = io.Copy(out, resp.Body)
	return err
}


// upload
func upload() {
	localPath := os.Args[2]
	dfsPath := os.Args[3]

	// 1) open fileand get size
	f, err := os.Open(localPath)
	if err != nil {
		log.Fatalf("open local file: %v", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("stat local file: %v", err)
	}
	size := fi.Size()
	if size == 0 {
		log.Fatal("empty file")
	}
	// 2) compute nummchunks
	numChunks := int(math.Ceil(float64(size) / float64(chunkSize)))
	fmt.Printf("File size: %d bytes, chunkSize: %d, numChunks: %d\n", size, chunkSize, numChunks)

	// 3) ask Namenode for chunk placement
	placements, err := requestPlacements(dfsPath, numChunks)
	if err != nil {
		log.Fatal("request Placements: %v", err)
	}
	if len(placements) != numChunks {
		log.Fatalf("namenode returned %d chunks, expected %d", len(placements), numChunks)
	}

	// 4) upload each chunk sequentially (can parallelize later)
	for i, pl := range placements {
		if len(pl.Locations) == 0 {
			log.Fatalf("no locations for chunk %d (%s)", i, pl.ID)
		}
	fmt.Printf("Preparing chunk %d/%d: id=%s to %v\n", i+1, numChunks, pl.ID, pl.Locations)
		// compute how many bytes to send for this chunk
		var toSend int64 = chunkSize
		// last chunk may be smaller
		if i == numChunks-1 {
			remaining := size - int64(i)*chunkSize
			toSend = remaining
		}
		// create a limited reader over the file
		// f's read position moves forward as we read itna hi size read kar
		lr := io.LimitReader(f, toSend)
		chunkBuf, err := io.ReadAll(lr)
    	if err != nil {
        log.Fatalf("read chunk %d (%s): %v", i, pl.ID, err)
   		 }
		// now upload this chunk to ALL locations (replicas)
    	for _, loc := range pl.Locations {
       	fmt.Printf("Uploading chunk %d/%d (replica) id=%s to %s\n", i+1, numChunks, pl.ID, loc)
        if err := uploadChunk(loc, pl.ID, bytes.NewReader(chunkBuf), int64(len(chunkBuf))); err != nil {
            log.Fatalf("upload chunk %d (%s) to %s: %v", i, pl.ID, loc, err)
        }
	}
	}
	fmt.Println("Upload completed successfully.")
}

func requestPlacements(path string, numChunks int) ([]ChunkPlacement, error) {

	body := map[string]any{
		"path":        path,
		"num_chunks":  numChunks,
		"replication": 2, // for now we only have one data node
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		return nil, err
	}
	//  post
	resp, err := http.Post(nameNodeAddr+"/files", "application/json", buf)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("namenode returned %d: %s", resp.StatusCode, string(b))
	}

	var res struct {
		Chunks []ChunkPlacement `json:"chunks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Chunks, nil
}

// uploadchunks streams data to dataNodes PUT chunks/{id}
func uploadChunk(baseURL, chunkID string, data io.Reader, size int64) error {
	baseURL = trimTrailingSlash(baseURL)
	url := baseURL + "/chunks/" + chunkID
	req, err := http.NewRequest(http.MethodPut, url, data)
	if err != nil {
		return err
	}
	req.ContentLength = size
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("datanode returned %d: %s", resp.StatusCode, string(b))
	}
	return nil
}


// helper
func trimTrailingSlash(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '/' || s[len(s)-1] == '\\') {
		s = s[:len(s)-1]
	}
	return s
}
