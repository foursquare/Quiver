// Copyright (C) 2015 Foursquare Labs Inc.

package hfile

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/foursquare/fsgo/report"
	"github.com/fujiwara/shapeio"
)

type LoadMethod int

const (
	CopiedToMem LoadMethod = iota
	MemlockFile
	OnDisk
)

type CollectionConfig struct {
	// The Name of the collection.
	Name string

	// The Hfile itself.
	SourcePath string

	// A local copy of SourcePath, if SourcePath is remote, otherwise the same as SourcePath.
	LocalPath string

	// If the local path has already been read, a cache to avoid re-reading.
	cachedContent *[]byte

	// If the collection data should be kept in-memory (via mlock).
	LoadMethod LoadMethod

	// Should operations on this collection emit verbose debug output.
	Debug bool

	// This "collection" may, in fact, be a partition (subset) of some larger (sharded) collection.
	ParentName      string
	ShardFunction   string
	Partition       string
	TotalPartitions string
}

type CollectionSet struct {
	Collections map[string]*Reader
	cache       string
}

func LoadCollections(collections []*CollectionConfig, cache string, downloadOnly bool, bytesLimit float64, stats *report.Recorder) (*CollectionSet, error) {
	cs := new(CollectionSet)
	cs.Collections = make(map[string]*Reader)

	if len(collections) < 1 {
		return nil, fmt.Errorf("no collections to load!")
	}

	if err := downloadCollections(collections, cache, stats, !downloadOnly, bytesLimit); err != nil {
		log.Println("[LoadCollections] Error fetching collections: ", err)
		return nil, err
	}

	if downloadOnly {
		return nil, nil
	}

	t := time.Now()
	for _, cfg := range collections {
		reader, err := NewReaderFromConfig(*cfg)
		if err != nil {
			return nil, err
		}

		cs.Collections[cfg.Name] = reader
	}
	if stats != nil {
		stats.TimeSince("startup.read", t)
	}

	return cs, nil
}

func downloadCollections(collections []*CollectionConfig, cache string, stats *report.Recorder, canBypassDisk bool, bytesLimit float64) error {
	if stats != nil {
		t := time.Now()
		defer stats.TimeSince("startup.download", t)
	}
	for _, cfg := range collections {
		if cfg.LocalPath == "" {
			cfg.LocalPath = cfg.SourcePath
		}

		remote := isRemote(cfg.SourcePath)
		if remote {
			cfg.LocalPath = localCache(cfg.SourcePath, cache)
			if _, err := os.Stat(cfg.LocalPath); err == nil {
				if cfg.Debug {
					log.Printf("[FetchRemote] %s already cached: %s.", cfg.Name, cfg.LocalPath)
				}
			} else if !os.IsNotExist(err) {
				return err
			} else {
				err = fetch(cfg, canBypassDisk, bytesLimit)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isRemote(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

func localCache(url, cache string) string {
	h := md5.Sum([]byte(url))
	name := hex.EncodeToString(h[:]) + ".hfile"
	return path.Join(cache, name)
}

func fetch(cfg *CollectionConfig, canBypassDisk bool, bytesLimit float64) error {
	log.Printf("[FetchRemote] Fetching %s: %s -> %s.", cfg.Name, cfg.SourcePath, cfg.LocalPath)

	destDir := filepath.Dir(cfg.LocalPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	canBypassDisk = canBypassDisk && cfg.LoadMethod == CopiedToMem
	prealloc := int64(0)

	if canBypassDisk && strings.Contains(cfg.SourcePath, "webhdfs") {
		statusUrl := strings.Replace(cfg.SourcePath, "op=open", "op=getfilestatus", 1)
		log.Println("[FetchRemote] Path appears to be webhdfs. Attempting to getfilestatus first for", cfg.Name, statusUrl)

		statResp, err := http.Get(statusUrl)

		if err != nil {
			log.Println("[FetchRemote] getfilestatus failed for", cfg.Name, statusUrl, err)
		} else {
			defer statResp.Body.Close()
			stat := struct{ FileStatus struct{ Length int64 } }{}

			statData, err := ioutil.ReadAll(statResp.Body)

			if err != nil {
				log.Println("[FetchRemote] Reading file status failed", cfg.Name, err)
			} else if err = json.Unmarshal(statData, &stat); err != nil {
				log.Println("[FetchRemote] Parsing file status failed", cfg.Name, err)
			} else {
				log.Println("[FetchRemote] Got file length for", cfg.Name, stat.FileStatus.Length)
				prealloc = stat.FileStatus.Length
			}
		}
	}

	resp, err := http.Get(cfg.SourcePath)
	if err != nil {
		return err
	} else if resp.StatusCode >= 400 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		return fmt.Errorf("HTTP error fetching (%s): %s\n", resp.Status, buf.String())
	}

	defer resp.Body.Close()
	var respBody io.Reader = resp.Body

	if bytesLimit > 0 {
		r := shapeio.NewReader(resp.Body)
		r.SetRateLimit(bytesLimit)
		respBody = r
	}

	fp, err := ioutil.TempFile(destDir, "hfile-downloading-")
	if err != nil {
		return err
	}
	defer fp.Close()

	sz := int64(0)

	if prealloc == 0 {
		prealloc = resp.ContentLength
	}

	if prealloc <= 0 {
		canBypassDisk = false
		log.Println("[FetchRemote] Cannot bypass writing to disk due to bad content length", prealloc)
	}

	if canBypassDisk {
		buf := offheapMalloc(prealloc)
		cfg.cachedContent = &buf

		if read, err := io.ReadFull(respBody, buf); err != nil {
			log.Println("[FetchRemote] Error fetching", cfg.Name, err)
			return err
		} else {
			sz = int64(read / (1024 * 1024))
		}

		log.Printf("[FetchRemote] Flushing %s (%dmb) to disk...\n", cfg.Name, sz)
		if _, err := fp.Write(buf); err != nil {
			log.Println("[FetchRemote] Error flushing ", cfg.Name, ": ", err)
			return err
		}
	} else {
		if sz, err = io.Copy(fp, respBody); err != nil {
			log.Fatal("[FetchRemote] Error fetching to disk", cfg.Name, err)
			return err
		}
		sz = sz / (1024 * 1024)
	}

	if err := fp.Close(); err != nil {
		log.Println("[FetchRemote] Error closing downloaded", cfg.Name, ": ", err)
		return err
	} else if err := os.Rename(fp.Name(), cfg.LocalPath); err != nil {
		log.Printf("[FetchRemote] Error moving downloaded %s to destination: %s\n", cfg.Name, err.Error())
		return err
	}

	log.Printf("[FetchRemote] Fetched %s (%dmb) and flushed to disk.\n", cfg.Name, sz)
	return nil
}

func (cs *CollectionSet) ReaderFor(name string) (*Reader, error) {
	c, ok := cs.Collections[name]
	if !ok {
		return nil, fmt.Errorf("not configured with reader for collection %s", name)
	}
	return c, nil
}
