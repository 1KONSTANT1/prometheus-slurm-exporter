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
	"bufio"
	"fmt"
	"log"
	"os"
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
	mig_name     string
}

func GPUsGetMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	return ParseGPUsMetrics()
}

type MIGDevice struct {
	GPU           int
	GI            int
	CI            int
	MigDev        int
	InstanceUsage float64 // SMACT значение
	MemoryUsage   float64 // DRAMA значение
	ProfileID     string  // ID профиля
	ProfileName   string  // Название профиля
	Processes     []ProcessInfo
}

type MIGPROFILES struct {
	SM  float64
	MEM float64
}

// ProcessInfo представляет информацию о процессе
type ProcessInfo struct {
	PID    string
	Type   string
	Name   string
	GPUMem string
}

func ParseDcgmiDmon(output string, migDevices map[string]*MIGDevice) (float64, float64) {
	scanner := bufio.NewScanner(strings.NewReader(output))

	gpu_total_sm := 0.0
	gpu_total_mem := 0.0
	entityRegex := regexp.MustCompile(`^(GPU-I\s+(\d+)|GPU\s+0)\s+([\d.]+)\s+([\d.]+)`)

	for scanner.Scan() {
		line := scanner.Text()
		// Пропускаем заголовки и разделители
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		// Ищем совпадения с шаблоном данных
		matches := entityRegex.FindStringSubmatch(line)
		if len(matches) != 5 {
			continue
		}

		// Парсим значения SMACT и DRAMA
		smact, err := strconv.ParseFloat(matches[3], 64)
		if err != nil {
			//return fmt.Errorf("ошибка парсинга SMACT: %v", )
		}

		drama, err := strconv.ParseFloat(matches[4], 64)
		if err != nil {
			//return fmt.Errorf("ошибка парсинга DRAMA: %v", err)
		}

		// Обновляем метрики
		if matches[1] == "GPU 0" {
			// Это общие метрики GPU, можно сохранить куда-то если нужно
			gpu_total_sm = smact
			gpu_total_mem = drama
		} else {
			// Это MIG устройство
			migDev, err := strconv.Atoi(matches[2])
			if err != nil {
				//return fmt.Errorf("ошибка парсинга MigDev: %v", err)
			}

			// Находим соответствующее устройство в map
			for _, device := range migDevices {
				if device.MigDev == migDev {
					// Обновляем метрики
					device.InstanceUsage = smact
					device.MemoryUsage = drama
					break
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		//return fmt.Errorf("ошибка чтения вывода: %v", err)
	}
	return gpu_total_sm, gpu_total_mem
}

// ParseNvidiaSMI парсит вывод nvidia-smi и возвращает map MIG-устройств
func ParseNvidiaSMI(output string) (map[string]*MIGDevice, error) {
	migDevices := make(map[string]*MIGDevice)
	scanner := bufio.NewScanner(strings.NewReader(output))

	// Регулярные выражения для парсинга
	migHeaderRegex := regexp.MustCompile(`^\| GPU\s+GI\s+CI\s+MIG\s+\|`)
	migDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\|`)
	processHeaderRegex := regexp.MustCompile(`^\|\s+GPU\s+GI\s+CI\s+PID\s+Type\s+Process name\s+GPU Memory\s+\|`)
	processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+MiB)\s+\|`)

	// Флаги для определения текущей секции
	inMIGSection := false
	inProcessSection := false

	for scanner.Scan() {
		line := scanner.Text()

		// Проверяем начало секции MIG devices
		if migHeaderRegex.MatchString(line) {
			inMIGSection = true
			inProcessSection = false
			continue
		}

		// Проверяем начало секции Processes
		if processHeaderRegex.MatchString(line) {
			inMIGSection = false
			inProcessSection = true
			continue
		}

		// Парсим данные MIG устройств
		if inMIGSection {
			matches := migDataRegex.FindStringSubmatch(line)
			if len(matches) == 5 {
				gpu, _ := strconv.Atoi(matches[1])
				gi, _ := strconv.Atoi(matches[2])
				ci, _ := strconv.Atoi(matches[3])
				migDev, _ := strconv.Atoi(matches[4])
				if migDev == 2 {
					migDev = 0
				} else {
					if migDev == 0 {
						migDev = 2
					}
				}

				key := fmt.Sprintf("%d-%d-%d", gpu, gi, ci)
				migDevices[key] = &MIGDevice{
					GPU:    gpu,
					GI:     gi,
					CI:     ci,
					MigDev: migDev,
				}
			}
		}

		// Парсим данные процессов
		if inProcessSection {
			matches := processDataRegex.FindStringSubmatch(line)
			if len(matches) == 8 {
				gpu, _ := strconv.Atoi(matches[1])
				gi, _ := strconv.Atoi(matches[2])
				ci, _ := strconv.Atoi(matches[3])
				pid := matches[4]
				procType := matches[5]
				procName := strings.TrimSpace(matches[6])
				gpuMem := matches[7]

				key := fmt.Sprintf("%d-%d-%d", gpu, gi, ci)
				if device, exists := migDevices[key]; exists {
					device.Processes = append(device.Processes, ProcessInfo{
						PID:    pid,
						Type:   procType,
						Name:   procName,
						GPUMem: gpuMem,
					})
					//migDevices[key] = device
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return migDevices, nil
}
func ParseNvidiaSMIMIGLI(migDevices map[string]*MIGDevice) error {
	scanner := bufio.NewScanner(strings.NewReader(string(Nvidi_MIG_PROFILES())))

	// Регулярное выражение для парсинга строк с информацией об инстансах
	migInstanceRegex := regexp.MustCompile(`\|\s+(\d+)\s+MIG\s+([\w\.]+)\s+(\d+)\s+(\d+)\s+(\d+:\d+)\s+\|`)

	for scanner.Scan() {
		line := scanner.Text()

		matches := migInstanceRegex.FindStringSubmatch(line)
		if len(matches) != 6 { // Теперь 6 групп с учётом имени профиля
			continue
		}

		gpuID, _ := strconv.Atoi(matches[1])
		profileName := matches[2]
		profileID := matches[3]
		instanceID, _ := strconv.Atoi(matches[4])
		// placement := matches[5] // Не используется, но можно сохранить если нужно

		// Ищем устройство с таким GPU Instance ID
		for _, device := range migDevices {
			if device.GPU == gpuID && device.GI == instanceID {
				device.ProfileID = profileID
				device.ProfileName = profileName
				//migDevices[key] = device
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("ошибка чтения вывода: %v", err)
	}

	return nil
}

func FindPIDMetrics(pid string, migDevices map[string]*MIGDevice) (string, string, string) {
	for _, device := range migDevices {
		for _, process := range device.Processes {
			if process.PID == pid {
				sm := fmt.Sprintf("%.1f", device.InstanceUsage)
				mem := fmt.Sprintf("%.1f", device.MemoryUsage)
				return sm, mem, device.ProfileName
			}
		}
	}
	// PID не найден
	return "0", "0", ""
}

func ParseGPUsMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}
	migs := false

	migDevices, err := ParseNvidiaSMI(string(NvidiSMI()))

	mig_total_sm, mig_total_mem := 0.0, 0.0
	if migDevices != nil {
		migs = true
		migProfiles := make(map[string]*MIGPROFILES, 7)
		migProfiles["0"] = &MIGPROFILES{}
		migProfiles["0"].SM = 98.0
		migProfiles["0"].MEM = 39.38

		migProfiles["5"] = &MIGPROFILES{}
		migProfiles["5"].SM = 56.0
		migProfiles["5"].MEM = 19.62

		migProfiles["9"] = &MIGPROFILES{}
		migProfiles["9"].SM = 42.0
		migProfiles["9"].MEM = 19.62

		migProfiles["14"] = &MIGPROFILES{}
		migProfiles["14"].SM = 28.0
		migProfiles["14"].MEM = 9.75

		migProfiles["15"] = &MIGPROFILES{}
		migProfiles["15"].SM = 14.0
		migProfiles["15"].MEM = 9.75

		migProfiles["20"] = &MIGPROFILES{}
		migProfiles["20"].SM = 14.0
		migProfiles["20"].MEM = 4.75

		migProfiles["19"] = &MIGPROFILES{}
		migProfiles["19"].SM = 14.0
		migProfiles["19"].MEM = 4.75

		dcgmi_dmon_comm := "dcgmi dmon -e 1002,1005 -i 0"
		for _, device := range migDevices {
			dcgmi_dmon_comm = dcgmi_dmon_comm + fmt.Sprintf(",i:%d", device.MigDev)
		}
		dcgmi_dmon_comm = dcgmi_dmon_comm + " -c 1"
		mig_total_sm, mig_total_mem = ParseDcgmiDmon(string(DCGMIDMON(dcgmi_dmon_comm)), migDevices)

		ParseNvidiaSMIMIGLI(migDevices)
		for _, device := range migDevices {
			device.InstanceUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.InstanceUsage*(migProfiles[device.ProfileID].SM/migProfiles["0"].SM)*100), 64)
			device.MemoryUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.MemoryUsage*(migProfiles[device.ProfileID].MEM/migProfiles["0"].MEM)*100), 64)
			//migDevices[key] = device
		}

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
			sm := "0"
			mem := "0"
			index := "0"
			mig_name := ""
			if migs {
				sm, mem, mig_name = FindPIDMetrics(target_pid, migDevices)
			} else {
				sm = split[3]
				mem = split[4]
				index = split[0]
			}

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

					nvidia_pid[split[1]] = &GPUusage{0, 0, "", "", ""}
					nvidia_pid[split[1]].gpu_usage = nvidia_pid[split[1]].gpu_usage + nvidia_sm
					nvidia_pid[split[1]].memory_usage = nvidia_pid[split[1]].memory_usage + nvidia_mem
					nvidia_pid[split[1]].hostname = hostname
					nvidia_pid[split[1]].index = index
					if migs {
						nvidia_pid[split[1]].mig_name = mig_name
					}
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
		if migs {
			GpusMap[gpu_uuid].total_gpu_usage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", mig_total_sm*100), 64)
			GpusMap[gpu_uuid].total_memory_usage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", mig_total_mem*100), 64)
		} else {
			GpusMap[gpu_uuid].total_gpu_usage, _ = strconv.ParseFloat(strings.Fields(split[6])[0], 64)
			GpusMap[gpu_uuid].total_memory_usage, _ = strconv.ParseFloat(strings.Fields(split[7])[0], 64)
		}
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

func NvidiSMI() []byte {
	cmd := exec.Command("nvidia-smi")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

func Nvidi_MIG_PROFILES() []byte {
	cmd := exec.Command("nvidia-smi", "mig", "-lgi")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi mig command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi mig command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

func DCGMIDMON(comm string) []byte {
	cmd := exec.Command("/bin/bash", "-c", comm)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing dcgmi dmon command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing dcgmi dmon command: %v", err)
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
	labels := []string{"JOBID", "HOSTNAME", "IDX", "MIG_NAME"}
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
		ch <- prometheus.MustNewConstMetric(cc.gpu_usage, prometheus.GaugeValue, nvidia[job].gpu_usage, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].mig_name)
		ch <- prometheus.MustNewConstMetric(cc.memory_usage, prometheus.GaugeValue, nvidia[job].memory_usage, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].mig_name)
	}
}
