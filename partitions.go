package main

import (
	"log"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func NewPartitionsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o \"%R|%a|%D|%g|%G|%I|%N|%T|%E\"")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing sinfo partition command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing sinfo partition command: %v", err)
		}
		return []byte("")
	}
	return out
}

func GetDefaultData() []byte {
	cmd := exec.Command("scontrol", "-o", "show", "partition")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing scontrol show partition command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing scontrol show partition command: %v", err)
		}
		return []byte("")
	}
	return out
}

type NewPartitionMetrics struct {
	available           string
	node_count          string
	groups              string
	gres                string
	priority            string
	nodelist            string
	node_states         string
	reason              string
	priority_job_factor string
	priority_tier       string
}

func ParsePartitionsMetrics() map[string]*NewPartitionMetrics {
	partitions_info := make(map[string]*NewPartitionMetrics)
	lines := strings.Split(string(NewPartitionsData()), "\n")
	partition_scontrol := string(GetDefaultData())
	for _, line := range lines {
		if strings.Contains(line, "|") {
			split := strings.Split(line, "|")
			partition_name := split[0]
			partition_name = partition_name[2:]
			partitions_info[partition_name] = &NewPartitionMetrics{}
			partitions_info[partition_name].available = split[1]
			partitions_info[partition_name].node_count = split[2]
			partitions_info[partition_name].groups = split[3]
			partitions_info[partition_name].gres = split[4]
			partitions_info[partition_name].priority = split[5]
			partitions_info[partition_name].nodelist = split[6]
			partitions_info[partition_name].node_states = split[7]
			partitions_info[partition_name].reason = split[8][:len(split[8])-1]
		}
	}
	current_partition := ""
	for _, scontrol_line := range strings.Split(partition_scontrol, "\n") {
		for _, word := range strings.Fields(scontrol_line) {
			if strings.HasPrefix(word, "PartitionName") {
				current_partition = strings.Split(word, "=")[1]
				if _, exists := partitions_info[current_partition]; !exists {
					break
				}
				continue
			}
			if strings.HasPrefix(word, "PriorityJobFactor") {
				partitions_info[current_partition].priority_job_factor = strings.Split(word, "=")[1]
			}
			if strings.HasPrefix(word, "PriorityTier") {
				partitions_info[current_partition].priority_tier = strings.Split(word, "=")[1]
				break
			}
		}
	}

	return partitions_info
}

type PartitionsCollector struct {
	partitions *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
	partition_labels := []string{"PARTITION", "AVAILABLE", "NODE_COUNT", "GROUPS", "GRES", "PRIORITY", "NODELIST", "NODES_STATES", "REASON", "PriorityJobFactor", "PriorityTier"}
	return &PartitionsCollector{
		partitions: prometheus.NewDesc("slurm_partition_info", "Partitions info", partition_labels, nil),
	}
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.partitions
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
	partitions := ParsePartitionsMetrics()
	for partition := range partitions {
		ch <- prometheus.MustNewConstMetric(pc.partitions, prometheus.GaugeValue, float64(0), partition, partitions[partition].available, partitions[partition].node_count, partitions[partition].groups, partitions[partition].gres, partitions[partition].priority, partitions[partition].nodelist, partitions[partition].node_states, partitions[partition].reason, partitions[partition].priority_job_factor, partitions[partition].priority_tier)
	}
}
