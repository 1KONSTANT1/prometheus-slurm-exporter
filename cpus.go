/* Copyright 2017 Victor Penso, Matteo Dessalvi

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
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type CPUsMetrics struct {
	alloc float64
	idle  float64
	other float64
	total float64
}

type NewCPUsMetrics struct {
	architecture string
	cpu_mode     string
	byte_order   string
	cores        string
	vendorid     string
	model_name   string
	cpu_family   string
	model        string
	hostname     string
}

type pidmem struct {
	mem_usage float64
	hostname  string
}

type pidcpu struct {
	cpu_usage float64
	hostname  string
}

type jobpcpuram struct {
	cpu_usage  float64
	mem_usage  float64
	rss        float64
	vsz        float64
	hostname   string
	swap_usage float64
}

type RAMmetrics struct {
	total      float64
	used       float64
	free       float64
	shared     float64
	buff       float64
	available  float64
	hostname   string
	total_swap float64
	used_swap  float64
	free_swap  float64
}

func CPUsGetMetrics() (*CPUsMetrics, *NewCPUsMetrics, map[string]*pidcpu, map[string]*jobpcpuram, map[string]*pidmem, *RAMmetrics) {
	return ParseCPUsMetrics(CPUsData())
}

func managestring(str string) string {

	return strings.Join(strings.Fields(strings.Split(str, ":")[1]), " ")
}

func pscommand(pid string) []byte {
	if _, err := os.Stat("/proc/" + pid); os.IsNotExist(err) {
		log.Printf("No such PID directory: /proc/%s", pid)
		return []byte("0.0 0.0 0.0 0.0")
	} else if err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command("ps", "-p", pid, "--format=pcpu,pmem,rss,vsz", "--no-header")

	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func get_swap(pid string) []byte {
	if _, err := os.Stat("/proc/" + pid); os.IsNotExist(err) {
		log.Printf("No such PID directory: /proc/%s", pid)
		return []byte("VmSwap: 0 kB")
	} else if err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", "-c", "cat /proc/"+pid+"/status | grep VmSwap")

	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func ParseCPUsMetrics(input []byte) (*CPUsMetrics, *NewCPUsMetrics, map[string]*pidcpu, map[string]*jobpcpuram, map[string]*pidmem, *RAMmetrics) {
	var cm CPUsMetrics
	if strings.Contains(string(input), "/") {
		splitted := strings.Split(strings.TrimSpace(string(input)), "/")
		cm.alloc, _ = strconv.ParseFloat(splitted[0], 64)
		cm.idle, _ = strconv.ParseFloat(splitted[1], 64)
		cm.other, _ = strconv.ParseFloat(splitted[2], 64)
		cm.total, _ = strconv.ParseFloat(splitted[3], 64)
	}
	var ccm NewCPUsMetrics
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}
	lines := strings.Split(string(CPUquery()), "\n")
	ccm.architecture = managestring(lines[0])
	ccm.cpu_mode = managestring(lines[1])
	ccm.byte_order = managestring(lines[3])
	ccm.cores = managestring(lines[4])
	ccm.vendorid = managestring(lines[6])
	ccm.model_name = managestring(lines[7])
	ccm.cpu_family = managestring(lines[8])
	ccm.model = managestring(lines[9])
	ccm.hostname = hostname

	lines = strings.Split(string(CPUtop10()), "\n")
	lines = lines[1 : len(lines)-1]
	cpu_pids := make(map[string]*pidcpu, 10)
	for _, line := range lines {
		comm := strings.Fields(line)[10]
		cpu_pids[comm] = &pidcpu{}
		cpu_pids[comm].hostname = hostname
		cpu_pids[comm].cpu_usage, _ = strconv.ParseFloat(strings.Fields(line)[2], 64)
	}

	pids_lines, err := ShowPids()
	job_cpu_pids := make(map[string]*jobpcpuram)
	if err == nil {
		lines := strings.Split(string(pids_lines), "\n")
		lines = lines[1 : len(lines)-1]
		for _, line := range lines {
			split := strings.Fields(line)
			if _, exists := job_cpu_pids[split[1]]; !exists {
				// Если ключа нет, создаем новый элемент
				job_cpu_pids[split[1]] = &jobpcpuram{}
				out_line := string(pscommand(split[0]))
				job_cpu_pids[split[1]].cpu_usage, _ = strconv.ParseFloat(strings.Fields(out_line)[0], 64)
				job_cpu_pids[split[1]].mem_usage, _ = strconv.ParseFloat(strings.Fields(out_line)[1], 64)
				job_cpu_pids[split[1]].rss, _ = strconv.ParseFloat(strings.Fields(out_line)[2], 64)
				job_cpu_pids[split[1]].vsz, _ = strconv.ParseFloat(strings.Fields(out_line)[3], 64)
				job_cpu_pids[split[1]].hostname = hostname
				swap_line := string(get_swap(split[0]))
				job_cpu_pids[split[1]].swap_usage, _ = strconv.ParseFloat(strings.Fields(swap_line)[1], 64)
			} else {
				// Если ключ уже существует, суммируем значения
				out_line := string(pscommand(split[0]))
				cp_us, _ := strconv.ParseFloat(strings.Fields(out_line)[0], 64)
				job_cpu_pids[split[1]].cpu_usage += cp_us
				mem_us, _ := strconv.ParseFloat(strings.Fields(out_line)[1], 64)
				job_cpu_pids[split[1]].mem_usage += mem_us
				new_rss, _ := strconv.ParseFloat(strings.Fields(out_line)[2], 64)
				job_cpu_pids[split[1]].rss += new_rss
				new_vsz, _ := strconv.ParseFloat(strings.Fields(out_line)[3], 64)
				job_cpu_pids[split[1]].vsz += new_vsz
				swap_line := string(get_swap(split[0]))
				swap_us, _ := strconv.ParseFloat(strings.Fields(swap_line)[1], 64)
				job_cpu_pids[split[1]].swap_usage += swap_us
			}
		}

	}

	lines = strings.Split(string(MEMtop10()), "\n")
	lines = lines[1 : len(lines)-1]
	mem_pids := make(map[string]*pidmem, 10)
	for _, line := range lines {
		comm := strings.Fields(line)[10]
		mem_pids[comm] = &pidmem{}
		mem_pids[comm].hostname = hostname
		mem_pids[comm].mem_usage, _ = strconv.ParseFloat(strings.Fields(line)[3], 64)
	}

	var rrm RAMmetrics
	lines = strings.Split(string(RAMquery()), "\n")
	split := strings.Fields(lines[1])
	rrm.total, _ = strconv.ParseFloat(split[1], 64)
	rrm.used, _ = strconv.ParseFloat(split[2], 64)
	rrm.free, _ = strconv.ParseFloat(split[3], 64)
	rrm.shared, _ = strconv.ParseFloat(split[4], 64)
	rrm.buff, _ = strconv.ParseFloat(split[5], 64)
	rrm.available, _ = strconv.ParseFloat(split[6], 64)
	rrm.hostname = hostname
	split = strings.Fields(lines[2])
	rrm.total_swap, _ = strconv.ParseFloat(split[1], 64)
	rrm.used_swap, _ = strconv.ParseFloat(split[2], 64)
	rrm.free_swap, _ = strconv.ParseFloat(split[3], 64)

	return &cm, &ccm, cpu_pids, job_cpu_pids, mem_pids, &rrm
}

func CPUquery() []byte {
	cmd := exec.Command("/bin/bash", "-c", "lscpu")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func RAMquery() []byte {
	cmd := exec.Command("/bin/bash", "-c", "free -b")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func CPUtop10() []byte {
	cmd := exec.Command("/bin/bash", "-c", "ps aux --sort=-%cpu | head -n 11")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

func MEMtop10() []byte {
	cmd := exec.Command("/bin/bash", "-c", "ps aux --sort=-%mem | head -n 11")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

// Execute the sinfo command and return its output
func CPUsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o %C")
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

func NewCPUsCollector() *CPUsCollector {
	labels := []string{"Architecture", "OPMODE", "ByteOrder", "Cores", "NAME", "VENDORID", "CPUFamily", "MODEL", "HOSTNAME"}
	labels2 := []string{"Command", "HOSTNAME"}
	labels3 := []string{"JOBID", "HOSTNAME"}
	labels4 := []string{"HOSTNAME"}

	return &CPUsCollector{
		alloc:         prometheus.NewDesc("slurm_cpus_alloc", "Allocated CPUs", nil, nil),
		idle:          prometheus.NewDesc("slurm_cpus_idle", "Idle CPUs", nil, nil),
		other:         prometheus.NewDesc("slurm_cpus_other", "Mix CPUs", nil, nil),
		total:         prometheus.NewDesc("slurm_cpus_total", "Total CPUs", nil, nil),
		cpu_info:      prometheus.NewDesc("slurm_cpu_info", "Total CPUs info", labels, nil),
		top_cpu_usage: prometheus.NewDesc("slurm_cpu_top_usage", "Total CPUs info", labels2, nil),
		job_cpu_usage: prometheus.NewDesc("slurm_cpu_job_usage", "Total CPUs info", labels3, nil),
		job_mem_usage: prometheus.NewDesc("slurm_mem_job_usage", "Total CPUs info", labels3, nil),
		job_rss:       prometheus.NewDesc("slurm_mem_rss", "Total CPUs info", labels3, nil),
		job_vsz:       prometheus.NewDesc("slurm_mem_vsz", "Total CPUs info", labels3, nil),
		job_swap:      prometheus.NewDesc("slurm_mem_swap", "Total CPUs info", labels3, nil),
		top_mem_usage: prometheus.NewDesc("slurm_mem_top_usage", "Total CPUs info", labels2, nil),
		total_ram:     prometheus.NewDesc("slurm_ram_total", "Total CPUs info", labels4, nil),
		used_ram:      prometheus.NewDesc("slurm_ram_used", "Total CPUs info", labels4, nil),
		free_ram:      prometheus.NewDesc("slurm_ram_free", "Total CPUs info", labels4, nil),
		shared_ram:    prometheus.NewDesc("slurm_ram_shared", "Total CPUs info", labels4, nil),
		buff_ram:      prometheus.NewDesc("slurm_ram_buff", "Total CPUs info", labels4, nil),
		available_ram: prometheus.NewDesc("slurm_ram_available", "Total CPUs info", labels4, nil),
		total_swap:    prometheus.NewDesc("slurm_swap_total", "Total CPUs info", labels4, nil),
		used_swap:     prometheus.NewDesc("slurm_swap_used", "Total CPUs info", labels4, nil),
		free_swap:     prometheus.NewDesc("slurm_swap_free", "Total CPUs info", labels4, nil),
	}
}

type CPUsCollector struct {
	alloc         *prometheus.Desc
	idle          *prometheus.Desc
	other         *prometheus.Desc
	total         *prometheus.Desc
	cpu_info      *prometheus.Desc
	top_cpu_usage *prometheus.Desc
	job_cpu_usage *prometheus.Desc
	job_mem_usage *prometheus.Desc
	job_rss       *prometheus.Desc
	job_vsz       *prometheus.Desc
	job_swap      *prometheus.Desc
	top_mem_usage *prometheus.Desc
	total_ram     *prometheus.Desc
	used_ram      *prometheus.Desc
	free_ram      *prometheus.Desc
	shared_ram    *prometheus.Desc
	buff_ram      *prometheus.Desc
	available_ram *prometheus.Desc
	total_swap    *prometheus.Desc
	used_swap     *prometheus.Desc
	free_swap     *prometheus.Desc
}

// Send all metric descriptions
func (cc *CPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.other
	ch <- cc.total
	ch <- cc.cpu_info
	ch <- cc.top_cpu_usage
	ch <- cc.job_cpu_usage
	ch <- cc.job_mem_usage
	ch <- cc.job_rss
	ch <- cc.job_vsz
	ch <- cc.job_swap
	ch <- cc.top_mem_usage
	ch <- cc.total_ram
	ch <- cc.used_ram
	ch <- cc.free_ram
	ch <- cc.shared_ram
	ch <- cc.buff_ram
	ch <- cc.available_ram
}
func (cc *CPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm, ccm, top_cpu, job_metr, top_mem, rrm := CPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.other, prometheus.GaugeValue, cm.other)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.cpu_info, prometheus.GaugeValue, float64(0), ccm.architecture, ccm.cpu_mode, ccm.byte_order, ccm.cores, ccm.model_name, ccm.vendorid, ccm.cpu_family, ccm.model, ccm.hostname)
	for comm := range top_cpu {
		ch <- prometheus.MustNewConstMetric(cc.top_cpu_usage, prometheus.GaugeValue, top_cpu[comm].cpu_usage, comm, top_cpu[comm].hostname)
	}
	for job := range job_metr {
		ch <- prometheus.MustNewConstMetric(cc.job_cpu_usage, prometheus.GaugeValue, job_metr[job].cpu_usage, job, job_metr[job].hostname)
		ch <- prometheus.MustNewConstMetric(cc.job_mem_usage, prometheus.GaugeValue, job_metr[job].mem_usage, job, job_metr[job].hostname)
		ch <- prometheus.MustNewConstMetric(cc.job_rss, prometheus.GaugeValue, job_metr[job].rss, job, job_metr[job].hostname)
		ch <- prometheus.MustNewConstMetric(cc.job_vsz, prometheus.GaugeValue, job_metr[job].vsz, job, job_metr[job].hostname)
		ch <- prometheus.MustNewConstMetric(cc.job_swap, prometheus.GaugeValue, job_metr[job].swap_usage, job, job_metr[job].hostname)
	}
	for comm := range top_mem {
		ch <- prometheus.MustNewConstMetric(cc.top_mem_usage, prometheus.GaugeValue, top_mem[comm].mem_usage, comm, top_mem[comm].hostname)
	}
	ch <- prometheus.MustNewConstMetric(cc.total_ram, prometheus.GaugeValue, rrm.total, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.used_ram, prometheus.GaugeValue, rrm.used, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.free_ram, prometheus.GaugeValue, rrm.free, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.shared_ram, prometheus.GaugeValue, rrm.shared, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.buff_ram, prometheus.GaugeValue, rrm.buff, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.available_ram, prometheus.GaugeValue, rrm.available, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.total_swap, prometheus.GaugeValue, rrm.total_swap, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.used_swap, prometheus.GaugeValue, rrm.used_swap, rrm.hostname)
	ch <- prometheus.MustNewConstMetric(cc.free_swap, prometheus.GaugeValue, rrm.free_swap, rrm.hostname)
}
