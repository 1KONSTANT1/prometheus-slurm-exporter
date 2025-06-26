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
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type GPUsMetrics struct {
	name               string
	pstate             string
	driver_version     string
	vbios_version      string
	memory_total       float64
	memory_used        float64
	total_gpu_usage    float64
	total_memory_usage float64
	temperature        float64
	power_instant      float64
	power_limit        float64
	hostname           string
	index              string
}

type GPUusage struct {
	gpu_usage    float64
	memory_usage float64
	hostname     string
	index        string
}

func GPUsGetMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	return ParseGPUsMetrics()
}

func ParseGPUsMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}
	//pidJobMap := make(map[string][]string)
	nvidia_pid := make(map[string]*GPUusage)
	pids_lines, err := ShowPids()
	if err == nil {
		slurm_pid_lines := strings.Split(string(pids_lines), "\n")
		slurm_pid_lines = slurm_pid_lines[1 : len(slurm_pid_lines)-1]
		nvidia_lines := strings.Split(string(Nvidiamon()), "\n")
		nvidia_lines = nvidia_lines[2 : len(nvidia_lines)-1]
		for _, line := range nvidia_lines {
			split := strings.Fields(line)
			target_pid := split[1]
			sm := split[3]
			mem := split[4]
			index := split[0]

			for _, pid_line := range slurm_pid_lines {
				split := strings.Fields(pid_line)
				if split[0] == target_pid {
					nvidia_sm := float64(0)
					nvidia_mem := float64(0)

					if sm != "-" {
						nvidia_sm, _ = strconv.ParseFloat(sm, 64)
					}
					if mem != "-" {
						nvidia_mem, _ = strconv.ParseFloat(mem, 64)
					}

					nvidia_pid[split[1]] = &GPUusage{0, 0, "", ""}
					nvidia_pid[split[1]].gpu_usage = nvidia_pid[split[1]].gpu_usage + nvidia_sm
					nvidia_pid[split[1]].memory_usage = nvidia_pid[split[1]].memory_usage + nvidia_mem
					nvidia_pid[split[1]].hostname = hostname
					nvidia_pid[split[1]].index = index
				}
			}

		}

	}

	GpusMap := make(map[string]*GPUsMetrics)
	lines := strings.Split(string(Nvidiaquery()), "\n")
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		split := strings.Split(line, ",")
		gpu_uuid := strings.Fields(split[11])[0]
		GpusMap[gpu_uuid] = &GPUsMetrics{}
		GpusMap[gpu_uuid].name = split[0]
		GpusMap[gpu_uuid].driver_version = strings.Fields(split[1])[0]
		GpusMap[gpu_uuid].vbios_version = strings.Fields(split[2])[0]
		GpusMap[gpu_uuid].pstate = strings.Fields(split[3])[0]
		GpusMap[gpu_uuid].memory_total, _ = strconv.ParseFloat(strings.Fields(split[4])[0], 64)
		GpusMap[gpu_uuid].memory_used, _ = strconv.ParseFloat(strings.Fields(split[5])[0], 64)
		GpusMap[gpu_uuid].total_gpu_usage, _ = strconv.ParseFloat(strings.Fields(split[6])[0], 64)
		GpusMap[gpu_uuid].total_memory_usage, _ = strconv.ParseFloat(strings.Fields(split[7])[0], 64)
		GpusMap[gpu_uuid].temperature, _ = strconv.ParseFloat(strings.Fields(split[8])[0], 64)
		GpusMap[gpu_uuid].power_instant, _ = strconv.ParseFloat(strings.Fields(split[9])[0], 64)
		GpusMap[gpu_uuid].power_limit, _ = strconv.ParseFloat(strings.Fields(split[10])[0], 64)
		GpusMap[gpu_uuid].index = strings.Fields(split[12])[0]
		GpusMap[gpu_uuid].hostname = hostname
	}

	return GpusMap, nvidia_pid
}

func Nvidiamon() []byte {
	cmd := exec.Command("nvidia-smi", "pmon", "-c", "1")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi pmon command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi pmon command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

func Nvidiaquery() []byte {
	cmd := exec.Command("nvidia-smi", "--query-gpu=name,driver_version,vbios_version,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,power.draw.instant,power.limit,uuid,index", "--format=csv")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi --query-gpu command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi --query-gpu command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	labels := []string{"JOBID", "HOSTNAME", "IDX"}
	labels2 := []string{"NAME", "DRIVER_VERSION", "PSTATE", "VBIOS_VERSION", "HOSTNAME", "UUID", "IDX"}
	labels3 := []string{"HOSTNAME", "UUID", "IDX"}
	return &GPUsCollector{
		gpu_usage:          prometheus.NewDesc("slurm_gpu_usage", "Job gpu usage", labels, nil),
		memory_usage:       prometheus.NewDesc("slurm_gpu_memory_usage", "Memory gpu usage", labels, nil),
		gpu_info:           prometheus.NewDesc("slurm_gpu_info", "Slurm gpu info", labels2, nil),
		total_memory:       prometheus.NewDesc("slurm_gpu_total_memory", "Slurm gpu info", labels3, nil),
		used_memory:        prometheus.NewDesc("slurm_gpu_used_memory", "Slurm gpu info", labels3, nil),
		total_gpu_usage:    prometheus.NewDesc("slurm_gpu_total_usage", "Slurm gpu info", labels3, nil),
		total_memory_usage: prometheus.NewDesc("slurm_gpu_memory_total_usage", "Slurm gpu info", labels3, nil),
		gpu_temp:           prometheus.NewDesc("slurm_gpu_temperature", "Slurm gpu info", labels3, nil),
		gpu_power_instant:  prometheus.NewDesc("slurm_gpu_power_instant", "Slurm gpu info", labels3, nil),
		gpu_power_limit:    prometheus.NewDesc("slurm_gpu_power_limit", "Slurm gpu info", labels3, nil),
	}
}

type GPUsCollector struct {
	gpu_info           *prometheus.Desc
	gpu_usage          *prometheus.Desc
	memory_usage       *prometheus.Desc
	total_memory       *prometheus.Desc
	used_memory        *prometheus.Desc
	total_gpu_usage    *prometheus.Desc
	total_memory_usage *prometheus.Desc
	gpu_temp           *prometheus.Desc
	gpu_power_instant  *prometheus.Desc
	gpu_power_limit    *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.gpu_info
	ch <- cc.gpu_usage
	ch <- cc.memory_usage
	ch <- cc.total_memory
	ch <- cc.used_memory
	ch <- cc.total_gpu_usage
	ch <- cc.total_memory_usage
	ch <- cc.gpu_temp
	ch <- cc.gpu_power_instant
	ch <- cc.gpu_power_limit
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	gpus_info, nvidia := GPUsGetMetrics()
	for gpu := range gpus_info {
		ch <- prometheus.MustNewConstMetric(cc.gpu_info, prometheus.GaugeValue, float64(0), gpus_info[gpu].name, gpus_info[gpu].driver_version, gpus_info[gpu].pstate, gpus_info[gpu].vbios_version, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_memory, prometheus.GaugeValue, gpus_info[gpu].memory_total, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.used_memory, prometheus.GaugeValue, gpus_info[gpu].memory_used, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_gpu_usage, prometheus.GaugeValue, gpus_info[gpu].total_gpu_usage, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_memory_usage, prometheus.GaugeValue, gpus_info[gpu].total_memory_usage, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpu_temp, prometheus.GaugeValue, gpus_info[gpu].temperature, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpu_power_instant, prometheus.GaugeValue, gpus_info[gpu].power_instant, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpu_power_limit, prometheus.GaugeValue, gpus_info[gpu].power_limit, gpus_info[gpu].hostname, gpu, gpus_info[gpu].index)
	}
	for job := range nvidia {
		ch <- prometheus.MustNewConstMetric(cc.gpu_usage, prometheus.GaugeValue, nvidia[job].gpu_usage, job, nvidia[job].hostname, nvidia[job].index)
		ch <- prometheus.MustNewConstMetric(cc.memory_usage, prometheus.GaugeValue, nvidia[job].memory_usage, job, nvidia[job].hostname, nvidia[job].index)
	}
}
