/* Copyright 2020 Victor Penso

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func PartitionsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o%R,%C")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

func PartitionsPendingJobsData() []byte {
	cmd := exec.Command("squeue", "-a", "-r", "-h", "-o%P", "--states=PENDING")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

func NewPartitionsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o \"%R|%a|%D|%g|%G|%I|%N|%T|%E\"")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func GetDefaultData() []byte {
	cmd := exec.Command("scontrol", "-o", "show", "partition")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type PartitionMetrics struct {
	allocated float64
	idle      float64
	other     float64
	pending   float64
	total     float64
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
	if_default          string
	priority_job_factor string
	priority_tier       string
}

func ParsePartitionsMetrics() (map[string]*PartitionMetrics, map[string]*NewPartitionMetrics) {
	partitions := make(map[string]*PartitionMetrics)
	lines := strings.Split(string(PartitionsData()), "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			// name of a partition
			partition := strings.Split(line, ",")[0]
			_, key := partitions[partition]
			if !key {
				partitions[partition] = &PartitionMetrics{}
			}
			states := strings.Split(line, ",")[1]
			allocated, _ := strconv.ParseFloat(strings.Split(states, "/")[0], 64)
			idle, _ := strconv.ParseFloat(strings.Split(states, "/")[1], 64)
			other, _ := strconv.ParseFloat(strings.Split(states, "/")[2], 64)
			total, _ := strconv.ParseFloat(strings.Split(states, "/")[3], 64)
			partitions[partition].allocated = allocated
			partitions[partition].idle = idle
			partitions[partition].other = other
			partitions[partition].total = total
		}
	}
	// get list of pending jobs by partition name
	list := strings.Split(string(PartitionsPendingJobsData()), "\n")
	for _, partition := range list {
		// accumulate the number of pending jobs
		_, key := partitions[partition]
		if key {
			partitions[partition].pending += 1
		}
	}
	partitions_info := make(map[string]*NewPartitionMetrics)
	lines = strings.Split(string(NewPartitionsData()), "\n")
	partition_scontrol := string(GetDefaultData())
	flag := false
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
			flag = false
			for _, scontrol_line := range strings.Split(partition_scontrol, "\n") {
				for _, word := range strings.Fields(scontrol_line) {
					if strings.HasPrefix(word, "PartitionName") {
						if strings.Split(word, "=")[1] != partition_name {
							break
						}

					}
					if strings.HasPrefix(word, "PriorityJobFactor") {
						partitions_info[partition_name].priority_job_factor = strings.Split(word, "=")[1]

					}
					if strings.HasPrefix(word, "PriorityTier") {
						partitions_info[partition_name].priority_tier = strings.Split(word, "=")[1]
						flag = true
						break
					}
				}
				if flag {
					break
				}
			}
		}
	}

	return partitions, partitions_info
}

type PartitionsCollector struct {
	allocated  *prometheus.Desc
	idle       *prometheus.Desc
	other      *prometheus.Desc
	pending    *prometheus.Desc
	total      *prometheus.Desc
	partitions *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
	labels := []string{"partition"}
	new_labels := []string{"PARTITION", "AVAILABLE", "NODE_COUNT", "GROUPS", "GRES", "PRIORITY", "NODELIST", "NODES_STATES", "REASON", "PriorityJobFactor", "PriorityTier"}
	return &PartitionsCollector{
		allocated:  prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels, nil),
		idle:       prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels, nil),
		other:      prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels, nil),
		pending:    prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels, nil),
		total:      prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels, nil),
		partitions: prometheus.NewDesc("slurm_partition_info", "Partitions info", new_labels, nil),
	}
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.allocated
	ch <- pc.idle
	ch <- pc.other
	ch <- pc.pending
	ch <- pc.total
	ch <- pc.partitions
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
	pm, partitions := ParsePartitionsMetrics()
	for p := range pm {
		if pm[p].allocated > 0 {
			ch <- prometheus.MustNewConstMetric(pc.allocated, prometheus.GaugeValue, pm[p].allocated, p)
		}
		if pm[p].idle > 0 {
			ch <- prometheus.MustNewConstMetric(pc.idle, prometheus.GaugeValue, pm[p].idle, p)
		}
		if pm[p].other > 0 {
			ch <- prometheus.MustNewConstMetric(pc.other, prometheus.GaugeValue, pm[p].other, p)
		}
		if pm[p].pending > 0 {
			ch <- prometheus.MustNewConstMetric(pc.pending, prometheus.GaugeValue, pm[p].pending, p)
		}
		if pm[p].total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.total, prometheus.GaugeValue, pm[p].total, p)
		}
	}
	for partition := range partitions {
		ch <- prometheus.MustNewConstMetric(pc.partitions, prometheus.GaugeValue, float64(0), partition, partitions[partition].available, partitions[partition].node_count, partitions[partition].groups, partitions[partition].gres, partitions[partition].priority, partitions[partition].nodelist, partitions[partition].node_states, partitions[partition].reason, partitions[partition].priority_job_factor, partitions[partition].priority_tier)
	}
}
