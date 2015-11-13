// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/foursquare/fsgo/adminz"
	"github.com/foursquare/fsgo/report"
	"github.com/foursquare/quiver/hfile"
)

var version string = "HEAD?"
var buildTime string = "unknown?"

type SettingDefs struct {
	port int

	downloadOnly bool

	debug bool

	bloom int

	detailedStats bool

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

	flag.IntVar(&s.bloom, "bloom", 0, "bloom filter wrong-positive % (or 0 to disable): lower numbers use more RAM but filter more queries.")

	flag.BoolVar(&s.detailedStats, "detailed-stats", false, "collect and report detailed metrics (time spent searching block, index, etc)")

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

func main() {
	log.Printf("Quiver version %s (built %s, %s).\n\n", version, buildTime, runtime.Version())
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

	registrations := new(Registrations)

	if Settings.discoveryPath != "" && !Settings.downloadOnly {
		registrations.Connect()
		defer registrations.Close()
	}

	configs := getCollectionConfig(args)

	log.Println("Loading collections...")

	cs, err := hfile.LoadCollections(configs, Settings.cachePath, Settings.downloadOnly, stats)

	if err != nil {
		log.Fatal(err)
	}

	if Settings.downloadOnly {
		stats.FlushNow()
		return
	}

	if Settings.detailedStats {
		log.Println("Enabling detailed metrics collection.")

		for _, c := range cs.Collections {
			c.ReportStats(stats)
		}
	}

	if Settings.bloom > 0 {
		beforeBloom := time.Now()
		for _, c := range cs.Collections {
			log.Println("Calculating bloom filter for", c.Name)
			c.CalculateBloom(float64(Settings.bloom) / 100)
		}
		stats.TimeSince("startup.bloom", beforeBloom)
	}

	log.Printf("Serving on http://%s:%d/ \n", hostname, Settings.port)

	http.Handle("/rpc/HFileService", WrapHttpRpcHandler(cs, stats))

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

	http.HandleFunc("/debug/bloom/enable", func(w http.ResponseWriter, r *http.Request) {
		for _, c := range cs.Collections {
			c.EnableBloom()
		}
	})

	http.HandleFunc("/debug/bloom/disable", func(w http.ResponseWriter, r *http.Request) {
		for _, c := range cs.Collections {
			c.DisableBloom()
		}
	})

	http.HandleFunc("/debug/bloom/calc", func(w http.ResponseWriter, r *http.Request) {
		if falsePos, err := strconv.Atoi(r.URL.Query().Get("err")); err != nil {
			http.Error(w, err.Error(), 400)
		} else if falsePos > 99 || falsePos < 1 {
			http.Error(w, "`err` param must be a false pos rate between 0 and 100", 400)
		} else {
			admin.Pause()
			defer admin.Resume()
			for _, c := range cs.Collections {
				fmt.Fprintln(w, "Recalculating bloom for", c.Name)
				c.CalculateBloom(float64(falsePos) / 100)
			}
		}
	})

	runtime.GC()
	stats.FlushNow()

	admin.Start()
	stats.TimeSince("startup.total", t)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Settings.port), nil))
}
