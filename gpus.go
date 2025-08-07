package main

import (
	"bufio"
	"fmt"
	"log"
	"os/exec"
	"regexp"
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
	hostname           string
	index              string
}

type GPUusage struct {
	gpu_usage        float64
	allocated_memory float64
	hostname         string
	index            string
}

func GPUsGetMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	return ParseGPUsMetrics()
}

// ProcessInfo представляет информацию о процессе
type ProcessInfo struct {
	PID    string
	Type   string
	Name   string
	GPUMem string
}

// ParseNvidiaSMI парсит вывод nvidia-smi и возвращает map MIG-устройств

type ProcessNvidia struct {
	GPUID    string
	MemoryMB float64
}

func FindProcessByPID(nvidiaSMIOutput string, pid string) (*ProcessNvidia, error) {
	scanner := bufio.NewScanner(strings.NewReader(nvidiaSMIOutput))

	// Регулярные выражения для парсинга
	processHeaderRegex := regexp.MustCompile(`^\|\s+GPU\s+GI\s+CI\s+PID\s+Type\s+Process name\s+GPU Memory\s+\|`)
	processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+(\d+)MiB\s+\|`)

	inProcessSection := false

	for scanner.Scan() {
		line := scanner.Text()

		// Find start of the process section
		if processHeaderRegex.MatchString(line) {
			inProcessSection = true
			continue
		}

		if strings.HasPrefix(line, "===") || strings.TrimSpace(line) == "" {
			continue
		}

		// Parse process data
		if inProcessSection {
			matches := processDataRegex.FindStringSubmatch(line)
			if len(matches) == 4 && matches[2] == pid {
				gpuID := matches[1]
				memoryMB, _ := strconv.ParseFloat(matches[3], 64)

				return &ProcessNvidia{
					GPUID:    gpuID,
					MemoryMB: memoryMB,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("process with PID %s not found", pid)
}

func ParseGPUsMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}

	GpusMap := make(map[string]*GPUsMetrics)
	lines := strings.Split(string(Nvidiaquery()), "\n")
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		split := strings.Split(line, ",")
		gpu_uuid := strings.Fields(split[12])[0]
		GpusMap[gpu_uuid] = &GPUsMetrics{}
		GpusMap[gpu_uuid].name = split[0]
		GpusMap[gpu_uuid].driver_version = strings.Fields(split[1])[0]
		GpusMap[gpu_uuid].vbios_version = strings.Fields(split[2])[0]
		GpusMap[gpu_uuid].pstate = strings.Fields(split[3])[0]
		GpusMap[gpu_uuid].memory_total, _ = strconv.ParseFloat(strings.Fields(split[4])[0], 64)
		GpusMap[gpu_uuid].memory_used, _ = strconv.ParseFloat(strings.Fields(split[5])[0], 64)
		GpusMap[gpu_uuid].memory_used, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", GpusMap[gpu_uuid].memory_used/GpusMap[gpu_uuid].memory_total*100), 64)

		GpusMap[gpu_uuid].total_gpu_usage, _ = strconv.ParseFloat(strings.Fields(split[6])[0], 64)
		GpusMap[gpu_uuid].total_memory_usage, _ = strconv.ParseFloat(strings.Fields(split[7])[0], 64)

		GpusMap[gpu_uuid].temperature, _ = strconv.ParseFloat(strings.Fields(split[8])[0], 64)
		GpusMap[gpu_uuid].index = strings.Fields(split[12])[0]
		GpusMap[gpu_uuid].hostname = hostname
	}

	nvidia_pid := make(map[string]*GPUusage)
	nvidia_lines := strings.Split(string(Nvidiamon()), "\n")
	pids_lines, err := ShowPids()
	if err == nil && len(nvidia_lines) > 2 {
		slurm_pid_lines := strings.Split(string(pids_lines), "\n")
		slurm_pid_lines = slurm_pid_lines[1 : len(slurm_pid_lines)-1]
		nvidia_lines = nvidia_lines[2 : len(nvidia_lines)-1]
		for _, line := range nvidia_lines {
			split := strings.Fields(line)
			target_pid := split[1]
			sm := split[3]
			index := split[0]

			for _, pid_line := range slurm_pid_lines {
				split := strings.Fields(pid_line)
				if split[0] == target_pid {
					nvidia_sm := float64(0)

					if sm != "-" {
						nvidia_sm, _ = strconv.ParseFloat(sm, 64)
					}

					nvidia_pid[split[1]] = &GPUusage{}
					nvidia_pid[split[1]].gpu_usage = nvidia_pid[split[1]].gpu_usage + nvidia_sm
					nvidia_pid[split[1]].hostname = hostname
					nvidia_pid[split[1]].index = index
					nvidia_proc_info, _ := FindProcessByPID(string(NvidiSMI()), split[0])
					nvidia_pid[split[1]].allocated_memory, err = strconv.ParseFloat(fmt.Sprintf("%.1f", nvidia_proc_info.MemoryMB/GpusMap[nvidia_proc_info.GPUID].memory_total*100), 64)
					if err != nil {
						nvidia_pid[split[1]].allocated_memory = 0
					}
				}
			}

		}

	}

	return GpusMap, nvidia_pid

}

func Nvidiamon() []byte {
	cmd := exec.Command("nvidia-smi", "pmon", "-c", "1")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi pmon command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing nvidia-smi pmon command: %v", err)
		}
		return []byte("")
	}
	return out
}

func NvidiSMI() []byte {
	cmd := exec.Command("nvidia-smi")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing nvidia-smi command: %v", err)
		}
		return []byte("")
	}
	return out
}

func Nvidiaquery() []byte {
	cmd := exec.Command("nvidia-smi", "--query-gpu=name,driver_version,vbios_version,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,power.draw.instant,power.limit,uuid,index", "--format=csv")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi --query-gpu command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing nvidia-smi --query-gpu command: %v", err)
		}
		return []byte("")
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	job_gpu_labels := []string{"JOBID", "HOSTNAME", "IDX"}
	gpu_labels := []string{"NAME", "DRIVER_VERSION", "PSTATE", "VBIOS_VERSION", "HOSTNAME", "IDX"}
	gpu_metric_labels := []string{"HOSTNAME", "IDX"}
	return &GPUsCollector{
		gpu_usage:          prometheus.NewDesc("slurm_gpu_usage", "Job gpu usage", job_gpu_labels, nil),
		allocated_memory:   prometheus.NewDesc("slurm_gpu_memory_allocated", "Memory gpu usage", job_gpu_labels, nil),
		gpu_info:           prometheus.NewDesc("slurm_gpu_info", "Slurm gpu info", gpu_labels, nil),
		total_memory:       prometheus.NewDesc("slurm_gpu_total_memory", "Slurm gpu total memory", gpu_metric_labels, nil),
		used_memory:        prometheus.NewDesc("slurm_gpu_used_memory", "Slurm gpu used memory", gpu_metric_labels, nil),
		total_gpu_usage:    prometheus.NewDesc("slurm_gpu_total_usage", "Slurm gpu total usage", gpu_metric_labels, nil),
		total_memory_usage: prometheus.NewDesc("slurm_gpu_memory_total_usage", "Slurm gpu total memory usage", gpu_metric_labels, nil),
		gpu_temp:           prometheus.NewDesc("slurm_gpu_temperature", "Slurm gpu temperature", gpu_metric_labels, nil),
	}
}

type GPUsCollector struct {
	gpu_info           *prometheus.Desc
	gpu_usage          *prometheus.Desc
	allocated_memory   *prometheus.Desc
	total_memory       *prometheus.Desc
	used_memory        *prometheus.Desc
	total_gpu_usage    *prometheus.Desc
	total_memory_usage *prometheus.Desc
	gpu_temp           *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.gpu_info
	ch <- cc.gpu_usage
	ch <- cc.total_memory
	ch <- cc.used_memory
	ch <- cc.total_gpu_usage
	ch <- cc.total_memory_usage
	ch <- cc.gpu_temp
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	gpus_info, nvidia := GPUsGetMetrics()
	for gpu := range gpus_info {
		ch <- prometheus.MustNewConstMetric(cc.gpu_info, prometheus.GaugeValue, float64(0), gpus_info[gpu].name, gpus_info[gpu].driver_version, gpus_info[gpu].pstate, gpus_info[gpu].vbios_version, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_memory, prometheus.GaugeValue, gpus_info[gpu].memory_total, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.used_memory, prometheus.GaugeValue, gpus_info[gpu].memory_used, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_gpu_usage, prometheus.GaugeValue, gpus_info[gpu].total_gpu_usage, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_memory_usage, prometheus.GaugeValue, gpus_info[gpu].total_memory_usage, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpu_temp, prometheus.GaugeValue, gpus_info[gpu].temperature, gpus_info[gpu].hostname, gpus_info[gpu].index)
	}
	for job := range nvidia {
		ch <- prometheus.MustNewConstMetric(cc.gpu_usage, prometheus.GaugeValue, nvidia[job].gpu_usage, job, nvidia[job].hostname, nvidia[job].index)
		ch <- prometheus.MustNewConstMetric(cc.allocated_memory, prometheus.GaugeValue, nvidia[job].allocated_memory, job, nvidia[job].hostname, nvidia[job].index)
	}
}
