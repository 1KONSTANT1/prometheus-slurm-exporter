package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

func init() {
	// Metrics have to be registered to be exposed
	prometheus.MustRegister(NewNetworkCollector())
	prometheus.MustRegister(NewDiskCollector())
	prometheus.MustRegister(NewAssocCollector())
	prometheus.MustRegister(NewPrioCollector())
	prometheus.MustRegister(NewJobCollector())
	prometheus.MustRegister(NewNodeResCollector())
	prometheus.MustRegister(NewCPUsCollector())       // from cpus.go
	prometheus.MustRegister(NewPartitionsCollector()) // from partitions.go
}

var listenAddress = flag.String(
	"listen-address",
	":8080",
	"The address to listen on for HTTP requests.")

var gpuAcct = flag.Bool(
	"gpus-acct",
	false,
	"Enable GPUs accounting")

func main() {
	flag.Parse()

	// Turn on GPUs accounting only if the corresponding command line option is set to true.
	if *gpuAcct {
		prometheus.MustRegister(NewGPUsCollector()) // from gpus.go
	}
	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	log.Infof("Starting Server: %s", *listenAddress)
	log.Infof("GPUs Accounting: %t", *gpuAcct)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
