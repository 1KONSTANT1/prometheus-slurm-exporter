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
	hostname           string
	index              string
	mig_mode           string
}

type GPUusage struct {
	gpu_usage        float64
	allocated_memory float64
	hostname         string
	index            string
	mig_name         string
}

func GPUsGetMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	return ParseGPUsMetrics()
}

type MIGDevice struct {
	GPU           int
	GI            int
	CI            int
	MigDev        int
	InstanceUsage float64
	MemoryUsage   float64
	ProfileID     string
	ProfileName   string
	Processes     []ProcessInfo
}

type MIGPROFILES struct {
	SM  float64
	MEM float64
}

type ProcessInfo struct {
	PID    string
	Type   string
	Name   string
	GPUMem string
}

type ProcessNvidia struct {
	GPUID    string
	MemoryMB float64
}

func ParseDcgmiDmon(output string, migDevices map[string]*MIGDevice) map[string]*MIGPROFILES {
	scanner := bufio.NewScanner(strings.NewReader(output))
	total_gpus := make(map[string]*MIGPROFILES)

	entityRegex := regexp.MustCompile(`^(GPU-CI\s+(\d+)|GPU\s+(\d+))\s+([\d.]+|N/A)\s+([\d.]+|N/A)`)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}

		matches := entityRegex.FindStringSubmatch(line)
		if len(matches) != 6 {
			continue
		}

		smact, err := strconv.ParseFloat(matches[4], 64)
		if err != nil {
			smact = 0.0
		}

		drama, err := strconv.ParseFloat(matches[5], 64)
		if err != nil {
			drama = 0.0
		}

		if matches[3] != "" {
			total_gpus[matches[3]] = &MIGPROFILES{}
			total_gpus[matches[3]].SM = smact
			total_gpus[matches[3]].MEM = drama
		} else {
			migDev, err := strconv.Atoi(matches[2])
			if err != nil {
			}
			for _, device := range migDevices {
				if device.MigDev == migDev {
					device.InstanceUsage = smact
					device.MemoryUsage = drama
					break
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
	}
	return total_gpus
}

func FindProcessByPID(nvidiaSMIOutput string, pid string) (*ProcessNvidia, error) {
	scanner := bufio.NewScanner(strings.NewReader(nvidiaSMIOutput))

	processHeaderRegex := regexp.MustCompile(`^\|\s+GPU\s+GI\s+CI\s+PID\s+Type\s+Process name\s+GPU Memory\s+\|`)
	processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+\S+\s+\S+\s+(\d+)\s+\S+\s+\S+\s+(\d+)MiB\s+\|`)

	inProcessSection := false

	for scanner.Scan() {
		line := scanner.Text()
		if processHeaderRegex.MatchString(line) {
			inProcessSection = true
			continue
		}

		if strings.HasPrefix(line, "===") || strings.TrimSpace(line) == "" {
			continue
		}

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

func FindEntityID(gpu, gi, ci int) string {
	scanner := bufio.NewScanner(strings.NewReader(string(discovery())))

	targetPattern := fmt.Sprintf("CI %d/%d/%d", gpu, gi, ci)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, targetPattern) {

			re := regexp.MustCompile(`EntityID:\s*(\d+)`)

			matches := re.FindStringSubmatch(line)
			return matches[1]
		}
	}

	return ""
}

func ParseNvidiaSMI(output string) (map[string]*MIGDevice, error) {
	migDevices := make(map[string]*MIGDevice)
	scanner := bufio.NewScanner(strings.NewReader(output))

	migHeaderRegex := regexp.MustCompile(`^\| GPU\s+GI\s+CI\s+MIG\s+\|`)
	migDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\|`)
	processHeaderRegex := regexp.MustCompile(`^\|\s+GPU\s+GI\s+CI\s+PID\s+Type\s+Process name\s+GPU Memory\s+\|`)
	processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+MiB)\s+\|`)

	inMIGSection := false
	inProcessSection := false

	for scanner.Scan() {
		line := scanner.Text()

		if migHeaderRegex.MatchString(line) {
			inMIGSection = true
			inProcessSection = false
			continue
		}

		if processHeaderRegex.MatchString(line) {
			inMIGSection = false
			inProcessSection = true
			continue
		}

		if inMIGSection {
			matches := migDataRegex.FindStringSubmatch(line)
			if len(matches) == 5 {
				gpu, _ := strconv.Atoi(matches[1])
				gi, _ := strconv.Atoi(matches[2])
				ci, _ := strconv.Atoi(matches[3])
				migDev, _ := strconv.Atoi(FindEntityID(gpu, gi, ci))

				key := fmt.Sprintf("%d-%d-%d", gpu, gi, ci)
				migDevices[key] = &MIGDevice{
					GPU:    gpu,
					GI:     gi,
					CI:     ci,
					MigDev: migDev,
				}
			}
		}

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
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return migDevices, nil
}

func SetMigProfileInfo(migDevices map[string]*MIGDevice) error {
	scanner := bufio.NewScanner(strings.NewReader(string(Nvidi_MIG_PROFILES())))

	migInstanceRegex := regexp.MustCompile(`\|\s+(\d+)\s+MIG\s+([\w\.]+)\s+(\d+)\s+(\d+)\s+(\d+:\d+)\s+\|`)

	for scanner.Scan() {
		line := scanner.Text()

		matches := migInstanceRegex.FindStringSubmatch(line)
		if len(matches) != 6 {
			continue
		}

		gpuID, _ := strconv.Atoi(matches[1])
		profileName := matches[2]
		profileID := matches[3]
		instanceID, _ := strconv.Atoi(matches[4])

		for _, device := range migDevices {
			if device.GPU == gpuID && device.GI == instanceID {
				device.ProfileID = profileID
				device.ProfileName = profileName
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
				return sm, device.ProfileName, strconv.Itoa(device.GPU)
			}
		}
	}
	return "0", "", "0"
}

func parseMIGProfiles() map[string]map[string]*MIGPROFILES {
	result := make(map[string]map[string]*MIGPROFILES)

	lines := strings.Split(string(MIG_LGIP()), "\n")

	profileRegex := regexp.MustCompile(`^\|\s+(\d+)\s+MIG\s+[\w\.\+]+\s+(\d+)\s+\d+/\d+\s+([\d\.]+)\s+\w+\s+(\d+)`)

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.Contains(line, "====") || strings.Contains(line, "GPU instance profiles:") ||
			strings.Contains(line, "GPU   Name") || line == "" {
			continue
		}

		matches := profileRegex.FindStringSubmatch(line)
		if len(matches) == 5 {
			gpuID := matches[1]
			profileID := matches[2]
			memory, _ := strconv.ParseFloat(matches[3], 64)
			sm, _ := strconv.ParseFloat(matches[4], 64)

			if result[gpuID] == nil {
				result[gpuID] = make(map[string]*MIGPROFILES, 7)
			}

			result[gpuID][profileID] = &MIGPROFILES{
				SM:  sm,
				MEM: memory,
			}

		}
	}

	return result
}

func ParseGPUsMetrics() (map[string]*GPUsMetrics, map[string]*GPUusage) {
	hostname := string(GetHostName())
	hostname = strings.ReplaceAll(hostname, "\n", "")
	if strings.Contains(hostname, ".") {
		hostname = strings.Split(hostname, ".")[0]
	}
	migs := false

	migDevices, err := ParseNvidiaSMI(string(NvidiSMI()))

	mig_totals := make(map[string]*MIGPROFILES)
	if migDevices != nil {
		migs = true

		gpu_mig_profiles := parseMIGProfiles()
		gpu_count := len(gpu_mig_profiles)

		dcgmi_dmon_comm := "dcgmi dmon -e 1002,1005 -i "
		gpu_str := ""

		for i := 0; i < gpu_count; i++ {
			if i == gpu_count-1 {
				gpu_str = gpu_str + fmt.Sprintf("%d", i)
			} else {
				gpu_str = gpu_str + fmt.Sprintf("%d", i) + ","
			}
		}
		dcgmi_dmon_comm = dcgmi_dmon_comm + gpu_str
		for _, device := range migDevices {
			dcgmi_dmon_comm = dcgmi_dmon_comm + fmt.Sprintf(",ci:%d", device.MigDev)
		}
		dcgmi_dmon_comm = dcgmi_dmon_comm + " -c 1"
		mig_totals = ParseDcgmiDmon(string(DCGMIDMON(dcgmi_dmon_comm)), migDevices)

		SetMigProfileInfo(migDevices)

		for _, device := range migDevices {
			device.InstanceUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.InstanceUsage*(gpu_mig_profiles[strconv.Itoa(device.GPU)][device.ProfileID].SM/gpu_mig_profiles[strconv.Itoa(device.GPU)]["0"].SM)*100), 64)
			device.MemoryUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.MemoryUsage*(gpu_mig_profiles[strconv.Itoa(device.GPU)][device.ProfileID].MEM/gpu_mig_profiles[strconv.Itoa(device.GPU)]["0"].MEM)*100), 64)
		}

	}

	GpusMap := make(map[string]*GPUsMetrics)
	lines := strings.Split(string(Nvidiaquery()), "\n")
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		split := strings.Split(line, ",")
		gpu_index := strings.Fields(split[12])[0]
		GpusMap[gpu_index] = &GPUsMetrics{}
		GpusMap[gpu_index].name = split[0]
		GpusMap[gpu_index].driver_version = strings.Fields(split[1])[0]
		GpusMap[gpu_index].vbios_version = strings.Fields(split[2])[0]
		GpusMap[gpu_index].pstate = strings.Fields(split[3])[0]
		GpusMap[gpu_index].memory_total, _ = strconv.ParseFloat(strings.Fields(split[4])[0], 64)
		GpusMap[gpu_index].memory_used, _ = strconv.ParseFloat(strings.Fields(split[5])[0], 64)
		GpusMap[gpu_index].memory_used, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", GpusMap[gpu_index].memory_used/GpusMap[gpu_index].memory_total*100), 64)

		if migs {
			GpusMap[gpu_index].total_gpu_usage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", mig_totals[gpu_index].SM*100), 64)
			GpusMap[gpu_index].total_memory_usage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", mig_totals[gpu_index].MEM*100), 64)
		} else {
			GpusMap[gpu_index].total_gpu_usage, _ = strconv.ParseFloat(strings.Fields(split[6])[0], 64)
			GpusMap[gpu_index].total_memory_usage, _ = strconv.ParseFloat(strings.Fields(split[7])[0], 64)
		}
		GpusMap[gpu_index].temperature, _ = strconv.ParseFloat(strings.Fields(split[8])[0], 64)
		GpusMap[gpu_index].index = strings.Fields(split[12])[0]
		GpusMap[gpu_index].mig_mode = strings.Fields(split[13])[0]
		GpusMap[gpu_index].hostname = hostname
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
			sm := "0"
			index := "0"
			mig_name := ""
			if migs {
				sm, mig_name, index = FindPIDMetrics(target_pid, migDevices)
			} else {
				sm = split[3]
				index = split[0]
			}

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
					if nvidia_proc_info != nil {
						nvidia_pid[split[1]].allocated_memory, err = strconv.ParseFloat(fmt.Sprintf("%.1f", nvidia_proc_info.MemoryMB/GpusMap[nvidia_proc_info.GPUID].memory_total*100), 64)
						if err != nil {
							nvidia_pid[split[1]].allocated_memory = 0
						}
					} else {
						nvidia_pid[split[1]].allocated_memory = 0
					}

					if migs {
						nvidia_pid[split[1]].mig_name = mig_name
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

func discovery() []byte {
	cmd := exec.Command("/bin/bash", "-c", "dcgmi discovery -c")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing dcgmi discovery command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing dcgmi discovery command: %v", err)
		}
		return []byte("")
	}
	return out
}

func Nvidi_MIG_PROFILES() []byte {
	cmd := exec.Command("nvidia-smi", "mig", "-lgi")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi mig lgi command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi mig lgi command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

func MIG_LGIP() []byte {
	cmd := exec.Command("nvidia-smi", "mig", "-lgip")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing nvidia-smi mig lgip command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing nvidia-smi mig lgip command: %v", err)
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
	cmd := exec.Command("nvidia-smi", "--query-gpu=name,driver_version,vbios_version,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,power.draw.instant,power.limit,uuid,index,mig.mode.current", "--format=csv")
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
	job_gpu_labels := []string{"JOBID", "HOSTNAME", "IDX", "MIG_NAME"}
	gpu_labels := []string{"NAME", "DRIVER_VERSION", "PSTATE", "VBIOS_VERSION", "HOSTNAME", "IDX", "MIG_MODE"}
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
		ch <- prometheus.MustNewConstMetric(cc.gpu_info, prometheus.GaugeValue, float64(0), gpus_info[gpu].name, gpus_info[gpu].driver_version, gpus_info[gpu].pstate, gpus_info[gpu].vbios_version, gpus_info[gpu].hostname, gpus_info[gpu].index, gpus_info[gpu].mig_mode)
		ch <- prometheus.MustNewConstMetric(cc.total_memory, prometheus.GaugeValue, gpus_info[gpu].memory_total, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.used_memory, prometheus.GaugeValue, gpus_info[gpu].memory_used, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_gpu_usage, prometheus.GaugeValue, gpus_info[gpu].total_gpu_usage, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.total_memory_usage, prometheus.GaugeValue, gpus_info[gpu].total_memory_usage, gpus_info[gpu].hostname, gpus_info[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpu_temp, prometheus.GaugeValue, gpus_info[gpu].temperature, gpus_info[gpu].hostname, gpus_info[gpu].index)
	}
	for job := range nvidia {
		ch <- prometheus.MustNewConstMetric(cc.gpu_usage, prometheus.GaugeValue, nvidia[job].gpu_usage, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].mig_name)
		ch <- prometheus.MustNewConstMetric(cc.allocated_memory, prometheus.GaugeValue, nvidia[job].allocated_memory, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].mig_name)
	}
}
