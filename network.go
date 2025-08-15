package main

import (
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type InterfaceStats struct {
	LinkType   string
	State      string
	MTU        string
	RX_Bytes   float64
	RX_Packets float64
	RX_Errors  float64
	RX_Dropped float64
	RX_Missed  float64
	RX_Mcast   float64
	TX_Bytes   float64
	TX_Packets float64
	TX_Errors  float64
	TX_Dropped float64
	TX_Carrier float64
	TX_Collsns float64
	hostname   string
}

func NetworkGetMetrics() map[string]*InterfaceStats {
	return ParseNetworkMetrics(NetworkData())
}

func ParseNetworkMetrics(input []byte) map[string]*InterfaceStats {
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}
	interfaces := make(map[string]*InterfaceStats, 5)
	lines := strings.Split(string(input), "\n")

	var currentInterface string
	var inRX, inTX bool

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.Contains(line, ": ") && strings.Contains(line, "<") {
			parts := strings.Fields(line)
			iface := parts[1][:len(parts[1])-1]
			currentInterface = iface

			interfaces[currentInterface] = &InterfaceStats{}

			stateRe := regexp.MustCompile(`state (\w+)`)
			mtuRe := regexp.MustCompile(`mtu (\d+)`)

			if matches := stateRe.FindStringSubmatch(line); len(matches) > 1 {
				interfaces[currentInterface].State = matches[1]
			}

			if matches := mtuRe.FindStringSubmatch(line); len(matches) > 1 {
				interfaces[currentInterface].MTU = matches[1]
			}

			continue
		}

		if strings.HasPrefix(line, "link/") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				interfaces[currentInterface].LinkType = strings.Split(parts[0], "/")[1]
			}
			continue
		}

		if strings.HasPrefix(line, "RX:") {
			inRX = true
			inTX = false
			continue
		}

		if strings.HasPrefix(line, "TX:") {
			inRX = false
			inTX = true
			continue
		}

		if inRX || inTX {
			cleaned := regexp.MustCompile(`\s+`).ReplaceAllString(line, " ")
			values := strings.Split(cleaned, " ")

			if len(values) < 6 {
				continue
			}

			if inRX {
				interfaces[currentInterface].RX_Bytes, _ = strconv.ParseFloat(values[0], 64)
				interfaces[currentInterface].RX_Packets, _ = strconv.ParseFloat(values[1], 64)
				interfaces[currentInterface].RX_Errors, _ = strconv.ParseFloat(values[2], 64)
				interfaces[currentInterface].RX_Dropped, _ = strconv.ParseFloat(values[3], 64)
				interfaces[currentInterface].RX_Missed, _ = strconv.ParseFloat(values[4], 64)
				interfaces[currentInterface].RX_Mcast, _ = strconv.ParseFloat(values[5], 64)
			} else if inTX {
				interfaces[currentInterface].TX_Bytes, _ = strconv.ParseFloat(values[0], 64)
				interfaces[currentInterface].TX_Packets, _ = strconv.ParseFloat(values[1], 64)
				interfaces[currentInterface].TX_Errors, _ = strconv.ParseFloat(values[2], 64)
				interfaces[currentInterface].TX_Dropped, _ = strconv.ParseFloat(values[3], 64)
				interfaces[currentInterface].TX_Carrier, _ = strconv.ParseFloat(values[4], 64)
				interfaces[currentInterface].TX_Collsns, _ = strconv.ParseFloat(values[5], 64)
			}
		}
		interfaces[currentInterface].hostname = hostname
	}

	return interfaces
}

func NetworkData() []byte {
	cmd := exec.Command("/bin/bash", "-c", "ip -s link")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing ip link command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing ip link command: %v", err)
		}
		return []byte("")
	}
	return out
}

type NetworkCollector struct {
	rx_bytes   *prometheus.Desc
	rx_packets *prometheus.Desc
	rx_errors  *prometheus.Desc
	rx_dropped *prometheus.Desc
	rx_missed  *prometheus.Desc
	rx_mcast   *prometheus.Desc
	tx_bytes   *prometheus.Desc
	tx_packets *prometheus.Desc
	tx_errors  *prometheus.Desc
	tx_dropped *prometheus.Desc
	tx_carrier *prometheus.Desc
	tx_collsns *prometheus.Desc
	iface_info *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewNetworkCollector() *NetworkCollector {
	iface_labels := []string{"LINK_NAME", "TYPE", "HOSTNAME"}
	iface_info_labels := []string{"LINK_NAME", "TYPE", "HOSTNAME", "MTU", "STATE"}
	return &NetworkCollector{
		rx_bytes:   prometheus.NewDesc("slurm_net_rx_bytes", "SLURM RX BYTES", iface_labels, nil),
		rx_packets: prometheus.NewDesc("slurm_net_rx_packets", "SLURM RX PACKETS", iface_labels, nil),
		rx_errors:  prometheus.NewDesc("slurm_net_rx_errors", "SLURM RX BYTES", iface_labels, nil),
		rx_dropped: prometheus.NewDesc("slurm_net_rx_dropped", "SLURM RX BYTES", iface_labels, nil),
		rx_missed:  prometheus.NewDesc("slurm_net_rx_missed", "SLURM RX BYTES", iface_labels, nil),
		rx_mcast:   prometheus.NewDesc("slurm_net_rx_mcast", "SLURM RX BYTES", iface_labels, nil),
		tx_bytes:   prometheus.NewDesc("slurm_net_tx_bytes", "SLURM RX BYTES", iface_labels, nil),
		tx_packets: prometheus.NewDesc("slurm_net_tx_packets", "SLURM RX PACKETS", iface_labels, nil),
		tx_errors:  prometheus.NewDesc("slurm_net_tx_errors", "SLURM RX BYTES", iface_labels, nil),
		tx_dropped: prometheus.NewDesc("slurm_net_tx_dropped", "SLURM RX BYTES", iface_labels, nil),
		tx_carrier: prometheus.NewDesc("slurm_net_tx_carrier", "SLURM RX BYTES", iface_labels, nil),
		tx_collsns: prometheus.NewDesc("slurm_net_tx_collsns", "SLURM RX BYTES", iface_labels, nil),
		iface_info: prometheus.NewDesc("slurm_net_info", "SLURM RX BYTES", iface_info_labels, nil),
	}
}

// Send all metric descriptions
func (nc *NetworkCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.rx_bytes
	ch <- nc.rx_packets
	ch <- nc.rx_errors
	ch <- nc.rx_dropped
	ch <- nc.rx_missed
	ch <- nc.rx_mcast
	ch <- nc.tx_bytes
	ch <- nc.tx_packets
	ch <- nc.tx_errors
	ch <- nc.tx_dropped
	ch <- nc.tx_carrier
	ch <- nc.tx_collsns
	ch <- nc.iface_info
}

func (nc *NetworkCollector) Collect(ch chan<- prometheus.Metric) {
	interfaces := NetworkGetMetrics()
	for iface := range interfaces {
		ch <- prometheus.MustNewConstMetric(nc.rx_bytes, prometheus.GaugeValue, interfaces[iface].RX_Bytes, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.rx_packets, prometheus.GaugeValue, interfaces[iface].RX_Packets, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.rx_errors, prometheus.GaugeValue, interfaces[iface].RX_Errors, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.rx_dropped, prometheus.GaugeValue, interfaces[iface].RX_Dropped, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.rx_missed, prometheus.GaugeValue, interfaces[iface].RX_Missed, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.rx_mcast, prometheus.GaugeValue, interfaces[iface].RX_Mcast, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_bytes, prometheus.GaugeValue, interfaces[iface].TX_Bytes, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_packets, prometheus.GaugeValue, interfaces[iface].TX_Packets, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_errors, prometheus.GaugeValue, interfaces[iface].TX_Errors, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_dropped, prometheus.GaugeValue, interfaces[iface].TX_Dropped, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_carrier, prometheus.GaugeValue, interfaces[iface].TX_Carrier, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.tx_collsns, prometheus.GaugeValue, interfaces[iface].TX_Collsns, iface, interfaces[iface].LinkType, interfaces[iface].hostname)
		ch <- prometheus.MustNewConstMetric(nc.iface_info, prometheus.GaugeValue, float64(0), iface, interfaces[iface].LinkType, interfaces[iface].hostname, interfaces[iface].MTU, interfaces[iface].State)
	}
}
