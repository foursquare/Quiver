// Copyright (C) 2015 Foursquare Labs Inc.

package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/foursquare/curator.go"
	"github.com/foursquare/fsgo/net/discovery"
	"github.com/foursquare/quiver/hfile"
)

type Registrations struct {
	existing []*discovery.ServiceDiscovery

	zk curator.CuratorFramework

	sync.Mutex
}

func NewNoopRegistrations() *Registrations {
	return new(Registrations)
}

func NewRegistrations(zk curator.CuratorFramework) *Registrations {
	r := new(Registrations)
	if zk == nil {
		log.Fatal("A zookeeper connection is required for service discovery.")
	}
	r.zk = zk
	return r
}

func (r *Registrations) Join(hostname, base string, configs []*hfile.CollectionConfig, wait time.Duration) {
	if r.zk == nil {
		log.Println("No-op registrations ignoring Join()")
		return
	}

	if hostname == "localhost" {
		log.Fatal("invalid hostname for service discovery registration:", hostname)
	}

	log.Println("Waiting to join service discovery", wait)
	time.Sleep(wait)
	log.Println("Joining service discovery...")

	r.Lock()
	defer r.Unlock()

	for _, i := range configs {
		sfunc := i.ShardFunction
		capacity := i.TotalPartitions

		if len(sfunc) < 1 {
			capacity = "1"
			sfunc = "_"
		}

		base := fmt.Sprintf("%s/%s/%s", i.ParentName, sfunc, capacity)

		disco := discovery.NewServiceDiscovery(r.zk, curator.JoinPath(Settings.discoveryPath, base))
		if err := disco.MaintainRegistrations(); err != nil {
			log.Fatal(err)
		}
		s := discovery.NewSimpleServiceInstance(i.Partition, hostname, Settings.port)
		disco.Register(s)
		r.existing = append(r.existing, disco)
	}
}

func (r *Registrations) Leave() {
	if r.zk == nil {
		log.Println("No-op registrations ignoring Leave()")
		return
	}
	r.Lock()
	defer r.Unlock()
	for _, reg := range r.existing {
		reg.UnregisterAll()
	}
}

func (r *Registrations) Close() {
	r.Leave()
}
