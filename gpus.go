/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

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
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}

type GPUusage struct {
	gpu_usage    float64
	memory_usage float64
	hostname     string
}

func GPUsGetMetrics() (*GPUsMetrics, map[string]*GPUusage) {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-a", "-X", "--format=Allocgres", "--state=RUNNING", "--noheader", "--parsable2"}
	output := string(Execute("sacct", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				descriptor := strings.TrimPrefix(line, "gpu:")
				job_gpus, _ := strconv.ParseFloat(descriptor, 64)
				num_gpus += job_gpus
			}
		}
	}

	return num_gpus
}

func ParseTotalGPUs() float64 {
	var num_gpus = 0.0

	args := []string{"-h", "-o \"%n %G\""}
	output := string(Execute("sinfo", args))
	if len(output) > 0 {
		for _, line := range strings.Split(output, "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				descriptor := strings.Fields(line)[1]
				descriptor = strings.TrimPrefix(descriptor, "gpu:")
				descriptor = strings.Split(descriptor, "(")[0]
				node_gpus, _ := strconv.ParseFloat(descriptor, 64)
				num_gpus += node_gpus
			}
		}
	}

	return num_gpus
}

func ParseGPUsMetrics() (*GPUsMetrics, map[string]*GPUusage) {
	var gm GPUsMetrics
	total_gpus := ParseTotalGPUs()
	allocated_gpus := ParseAllocatedGPUs()
	gm.alloc = allocated_gpus
	gm.idle = total_gpus - allocated_gpus
	gm.total = total_gpus
	gm.utilization = allocated_gpus / total_gpus
	pidJobMap := make(map[string][]string)
	nvidia_pid := make(map[string]*GPUusage)
	pids_lines, err := ShowPids()
	if err == nil {
		lines := strings.Split(string(pids_lines), "\n")
		lines = lines[1 : len(lines)-1]
		for _, line := range lines {
			split := strings.Fields(line)
			jobid := split[1]
			pidJobMap[jobid] = append(pidJobMap[jobid], split[0])

		}
		lines = strings.Split(string(Nvidiamon()), "\n")
		lines = lines[2 : len(lines)-1]
		for _, line := range lines {
			split := strings.Fields(line)
			target_pid := split[1]
			sm := split[3]
			mem := split[4]
			for jobid, pids := range pidJobMap {
				for _, pid := range pids {
					if pid == target_pid {
						nvidia_sm, _ := strconv.ParseFloat(sm, 64)
						nvidia_mem, _ := strconv.ParseFloat(mem, 64)
						nvidia_pid[jobid] = &GPUusage{}
						nvidia_pid[jobid].gpu_usage = nvidia_pid[jobid].gpu_usage + nvidia_sm
						nvidia_pid[jobid].memory_usage = nvidia_pid[jobid].memory_usage + nvidia_mem
						hostname := string(GetHostName())
						if strings.Contains(hostname, ".") {
							hostname = strings.Split(hostname, ".")[0]
						}
						nvidia_pid[jobid].hostname = hostname
					}
				}
			}

		}

	}

	return &gm, nvidia_pid
}

func Nvidiamon() []byte {
	cmd := exec.Command("nvidia-smi", "pmon", "-c", "1")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
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

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	labels := []string{"JOBID", "HOSTNAME"}
	return &GPUsCollector{
		alloc:        prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:         prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total:        prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization:  prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
		gpu_usage:    prometheus.NewDesc("slurm_gpu_usage", "Job gpu usage", labels, nil),
		memory_usage: prometheus.NewDesc("slurm_gpu_usage", "Memory gpu usage", labels, nil),
	}
}

type GPUsCollector struct {
	alloc        *prometheus.Desc
	idle         *prometheus.Desc
	total        *prometheus.Desc
	utilization  *prometheus.Desc
	gpu_usage    *prometheus.Desc
	memory_usage *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm, nvidia := GPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
	for job := range nvidia {
		ch <- prometheus.MustNewConstMetric(cc.gpu_usage, prometheus.GaugeValue, nvidia[job].gpu_usage, job, nvidia[job].hostname)
		ch <- prometheus.MustNewConstMetric(cc.memory_usage, prometheus.GaugeValue, nvidia[job].memory_usage, job, nvidia[job].hostname)
	}
}
