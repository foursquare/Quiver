// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/foursquare/curator.go"
	"github.com/foursquare/fsgo/adminz"
	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/hfile"
)

type SettingDefs struct {
	port int

	downloadOnly bool

	debug bool

	mlock bool

	configJsonUrl string

	cachePath string

	zk            string
	discoveryPath string
}

var Settings SettingDefs

func readSettings() []string {
	s := SettingDefs{}
	flag.IntVar(&s.port, "port", 9999, "listen port")

	flag.BoolVar(&s.debug, "debug", false, "print more output")

	flag.BoolVar(&s.downloadOnly, "download-only", false, "exit after downloading remote files to local cache.")

	flag.BoolVar(&s.mlock, "mlock", false, "mlock mapped files in memory rather than copy to heap.")

	flag.StringVar(&s.configJsonUrl, "config-json", "", "URL of collection configuration json")

	flag.StringVar(&s.cachePath, "cache", os.TempDir(), "local path to write files fetched (*not* cleaned up automatically)")

	flag.StringVar(&s.zk, "zookeeper", "", "zookeeper")
	flag.StringVar(&s.discoveryPath, "discovery", "", "service discovery base path")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			`
Usage: %s [options] col1=path1 col2=path2 ...
`, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	Settings = s

	if (len(flag.Args()) > 0) == (Settings.configJsonUrl != "") {
		log.Println("Collections must be specified OR URL to configuration json.")
		flag.Usage()
		os.Exit(-1)
	}

	return flag.Args()
}

var zk curator.CuratorFramework

func main() {
	fmt.Println("max procs:", runtime.GOMAXPROCS(-1))
	t := time.Now()

	graphite := report.Flag()
	args := readSettings()

	stats := report.NewRecorder().
		EnableGCInfoCollection().
		MaybeReportTo(graphite).
		RegisterHttp().
		SetAsDefault()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	if Settings.zk != "" {
		retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)
		zk = curator.NewClient(Settings.zk, retryPolicy)

		if err := zk.Start(); err != nil {
			log.Fatal(err)
		}
		defer zk.Close()
	}

	registrations := NewNoopRegistrations()

	if Settings.discoveryPath != "" && !Settings.downloadOnly {
		registrations = NewRegistrations(zk)
		defer registrations.Close()
	}

	configs := getCollectionConfig(args)

	log.Println("Loading collections...")

	cs, err := hfile.LoadCollections(configs, Settings.cachePath, Settings.downloadOnly, stats)

	if err != nil {
		log.Fatal(err)
	}

	if Settings.downloadOnly {
		// TODO(davidt): stats.MaybeReportNowTo(graphite)
		return
	}

	log.Printf("Serving on http://%s:%d/ \n", hostname, Settings.port)

	http.Handle("/rpc/HFileService", NewHttpRpcHandler(cs, stats))

	admin := adminz.New()
	admin.KillfilePaths(adminz.Killfiles(Settings.port))

	admin.Servicez(func() interface{} {
		return struct {
			Collections map[string]*hfile.Reader `json:"collections"`
			Impl        string                   `json:"implementation"`
		}{
			cs.Collections,
			"quiver",
		}
	})

	admin.OnPause(registrations.Leave)
	admin.OnResume(func() {
		if Settings.discoveryPath != "" {
			registrations.Join(hostname, Settings.discoveryPath, configs, 0)
		}
	})

	http.HandleFunc("/hfilez", admin.ServicezHandler)
	http.HandleFunc("/", admin.ServicezHandler)

	admin.Start()
	stats.TimeSince("startup.total", t)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
