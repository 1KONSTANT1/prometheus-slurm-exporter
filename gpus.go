package main

import (
	"bufio"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type GPUsMetrics struct {
	name             string
	pstate           string
	driverVersion    string
	vbiosVersion     string
	memoryTotal      float64
	memoryUsed       float64
	totalGPUUsage    float64
	totalMemoryUsage float64
	temperature      float64
	hostname         string
	index            string
	migMode          string
}

type GPUUsage struct {
	gpuUsage        float64
	allocatedMemory float64
	hostname        string
	index           string
	migName         string
}

func GPUsGetMetrics() (map[string]*GPUsMetrics, map[string]*GPUUsage) {
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
	Processes     []string
}

type MIGProfiles struct {
	SM  float64
	MEM float64
}

type ProcessInfo struct {
	GPUID   string
	GPUMem  float64
	MigName string
	sm      string
}

func ParseDcgmiDmon(output string, migDevices map[string]*MIGDevice) map[string]*MIGProfiles {
	scanner := bufio.NewScanner(strings.NewReader(output))
	totalGpus := make(map[string]*MIGProfiles)

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

		smAct, err := strconv.ParseFloat(matches[4], 64)
		if err != nil {
			smAct = 0.0
		}

		dramA, err := strconv.ParseFloat(matches[5], 64)
		if err != nil {
			dramA = 0.0
		}

		if matches[3] != "" {
			totalGpus[matches[3]] = &MIGProfiles{}
			totalGpus[matches[3]].SM = smAct
			totalGpus[matches[3]].MEM = dramA
		} else {
			migDev, _ := strconv.Atoi(matches[2])
			for _, device := range migDevices {
				if device.MigDev == migDev {
					device.InstanceUsage = smAct
					device.MemoryUsage = dramA
					break
				}
			}
		}
	}

	return totalGpus
}

func FindEntityID(gpu, gi, ci int) string {
	scanner := bufio.NewScanner(strings.NewReader(string(ExecuteCommand(DCGMI_DISCOVERY))))

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

func ParseNvidiaSMI(output string) (map[string]*MIGDevice, map[string]*ProcessInfo, error) {
	migDevices := make(map[string]*MIGDevice)
	processes := make(map[string]*ProcessInfo)
	scanner := bufio.NewScanner(strings.NewReader(output))

	migHeaderRegex := regexp.MustCompile(`^\| GPU\s+GI\s+CI\s+MIG\s+\|`)
	migDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\|`)
	processHeaderRegex := regexp.MustCompile(`^\|\s+GPU\s+GI\s+CI\s+.+PID\s+Type\s+Process name\s+GPU Memory\s+\|`)
	//processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)MiB\s+\|`)
	processDataRegex := regexp.MustCompile(`^\|\s+(\d+)\s+([\w\/]+)\s+([\w\/]+)\s+(\d+)\s+(\w+)\s+(\w+)\s+(\d+)MiB\s+\|`)

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
				gi := matches[2]
				ci := matches[3]
				pid := matches[4]
				gpuMem, _ := strconv.ParseFloat(matches[7], 64)

				if gi != "N/A" && ci != "N/A" {
					giInt, _ := strconv.Atoi(gi)
					ciInt, _ := strconv.Atoi(ci)
					key := fmt.Sprintf("%d-%d-%d", gpu, giInt, ciInt)
					if device, exists := migDevices[key]; exists {
						device.Processes = append(device.Processes, pid)
					}
				}

				processes[pid] = &ProcessInfo{
					GPUID:  matches[1],
					GPUMem: gpuMem,
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return migDevices, processes, nil
}

func SetMigProfileInfo(migDevices map[string]*MIGDevice) error {
	scanner := bufio.NewScanner(strings.NewReader(string(ExecuteCommand(NVIDIA_SMI_MIG_LGI))))

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
		return fmt.Errorf("error reading the output: %v", err)
	}

	return nil
}

func parseMIGProfiles() map[string]map[string]*MIGProfiles {
	result := make(map[string]map[string]*MIGProfiles)

	lines := strings.Split(string(ExecuteCommand(NVIDIA_SMI_MIG_LGIP)), "\n")

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

			if _, exists := result[gpuID]; !exists {
				result[gpuID] = make(map[string]*MIGProfiles, 7)
			}

			result[gpuID][profileID] = &MIGProfiles{
				SM:  sm,
				MEM: memory,
			}

		}
	}

	return result
}

func ParseGPUsMetrics() (map[string]*GPUsMetrics, map[string]*GPUUsage) {
	hostname := string(ExecuteCommand(HOSTNAME))
	hostname = strings.ReplaceAll(hostname, "\n", "")

	migDevices, processes, _ := ParseNvidiaSMI(string(ExecuteCommand(NVIDIA_SMI)))

	migTotals := make(map[string]*MIGProfiles)
	if len(migDevices) != 0 {

		gpuMigProfiles := parseMIGProfiles()
		gpuCount := len(gpuMigProfiles)

		var builder strings.Builder

		builder.WriteString("dcgmi dmon -e 1002,1005 -i ")

		for i := 0; i < gpuCount; i++ {
			if i > 0 {
				builder.WriteString(",")
			}
			builder.WriteString(strconv.Itoa(i))
		}

		for _, device := range migDevices {
			builder.WriteString(fmt.Sprintf(",ci:%d", device.MigDev))
		}

		builder.WriteString(" -c 1")

		migTotals = ParseDcgmiDmon(string(ExecuteCommand(builder.String())), migDevices)

		SetMigProfileInfo(migDevices)

		for _, device := range migDevices {
			device.InstanceUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.InstanceUsage*(gpuMigProfiles[strconv.Itoa(device.GPU)][device.ProfileID].SM/gpuMigProfiles[strconv.Itoa(device.GPU)]["0"].SM)*100), 64)
			device.MemoryUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", device.MemoryUsage*(gpuMigProfiles[strconv.Itoa(device.GPU)][device.ProfileID].MEM/gpuMigProfiles[strconv.Itoa(device.GPU)]["0"].MEM)*100), 64)
			for _, process := range device.Processes {
				processes[process].MigName = device.ProfileName
				processes[process].sm = fmt.Sprintf("%.1f", device.InstanceUsage)
			}
		}

	}

	gpusMap := make(map[string]*GPUsMetrics)

	lines := strings.Split(string(ExecuteCommand(NVDIA_QUERY)), "\n")
	lines = lines[1 : len(lines)-1]
	for _, line := range lines {
		split := strings.Split(line, ",")
		gpuIndex := strings.Fields(split[12])[0]
		gpusMap[gpuIndex] = &GPUsMetrics{}
		gpusMap[gpuIndex].name = split[0]
		gpusMap[gpuIndex].driverVersion = strings.Fields(split[1])[0]
		gpusMap[gpuIndex].vbiosVersion = strings.Fields(split[2])[0]
		gpusMap[gpuIndex].pstate = strings.Fields(split[3])[0]
		gpusMap[gpuIndex].memoryTotal, _ = strconv.ParseFloat(strings.Fields(split[4])[0], 64)
		gpusMap[gpuIndex].memoryUsed, _ = strconv.ParseFloat(strings.Fields(split[5])[0], 64)
		gpusMap[gpuIndex].memoryUsed, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", gpusMap[gpuIndex].memoryUsed/gpusMap[gpuIndex].memoryTotal*100), 64)

		gpusMap[gpuIndex].temperature, _ = strconv.ParseFloat(strings.Fields(split[8])[0], 64)
		gpusMap[gpuIndex].index = strings.Fields(split[12])[0]
		gpusMap[gpuIndex].migMode = strings.Fields(split[13])[0]

		if gpusMap[gpuIndex].migMode == "Enabled" {
			gpusMap[gpuIndex].totalGPUUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", migTotals[gpuIndex].SM*100), 64)
			gpusMap[gpuIndex].totalMemoryUsage, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", migTotals[gpuIndex].MEM*100), 64)
		} else {
			gpusMap[gpuIndex].totalGPUUsage, _ = strconv.ParseFloat(strings.Fields(split[6])[0], 64)
			gpusMap[gpuIndex].totalMemoryUsage, _ = strconv.ParseFloat(strings.Fields(split[7])[0], 64)
		}
		gpusMap[gpuIndex].hostname = hostname
	}

	nvidiaPid := make(map[string]*GPUUsage)
	nvidiaLines := strings.Split(string(ExecuteCommand(NVIDIA_SMI_PMON)), "\n")
	pidsLines, err := ShowPids()
	if err == nil && len(nvidiaLines) > 2 {
		slurmPidLines := strings.Split(string(pidsLines), "\n")
		slurmPidLines = slurmPidLines[1 : len(slurmPidLines)-1]
		nvidiaLines = nvidiaLines[2 : len(nvidiaLines)-1]
		for _, line := range nvidiaLines {
			split := strings.Fields(line)
			targetPid := split[1]
			sm := split[3]
			index := split[0]

			for _, pidLine := range slurmPidLines {
				split := strings.Fields(pidLine)
				if split[0] == targetPid {
					nvidiaSm := float64(0)

					if sm != "-" {
						nvidiaSm, _ = strconv.ParseFloat(sm, 64)
					}

					nvidiaPid[split[1]] = &GPUUsage{}
					if gpusMap[index].migMode != "Enabled" {
						nvidiaPid[split[1]].gpuUsage = nvidiaPid[split[1]].gpuUsage + nvidiaSm
					} else {
						nvidiaPid[split[1]].gpuUsage, _ = strconv.ParseFloat(processes[targetPid].sm, 64)
						nvidiaPid[split[1]].migName = processes[targetPid].MigName
					}
					nvidiaPid[split[1]].hostname = hostname
					nvidiaPid[split[1]].index = index

					if _, exists := processes[split[0]]; exists {

						nvidiaAllocMem, err := strconv.ParseFloat(fmt.Sprintf("%.1f", processes[split[0]].GPUMem/gpusMap[processes[split[0]].GPUID].memoryTotal*100), 64)

						if err != nil {
							nvidiaPid[split[1]].allocatedMemory = 0
						}
						nvidiaPid[split[1]].allocatedMemory = nvidiaPid[split[1]].allocatedMemory + nvidiaAllocMem

					} else {
						nvidiaPid[split[1]].allocatedMemory = 0
					}
				}
			}

		}

	}

	return gpusMap, nvidiaPid

}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	jobGPULabels := []string{"JOBID", "HOSTNAME", "IDX", "MIG_NAME"}
	gpuLabels := []string{"NAME", "DRIVER_VERSION", "PSTATE", "VBIOS_VERSION", "HOSTNAME", "IDX", "MIG_MODE"}
	gpuMetricLabels := []string{"HOSTNAME", "IDX"}
	return &GPUsCollector{
		gpuUsage:         prometheus.NewDesc("slurm_gpu_usage", "Job gpu usage", jobGPULabels, nil),
		allocatedMemory:  prometheus.NewDesc("slurm_gpu_memory_allocated", "Memory gpu usage", jobGPULabels, nil),
		gpuInfo:          prometheus.NewDesc("slurm_gpu_info", "Slurm gpu info", gpuLabels, nil),
		totalMemory:      prometheus.NewDesc("slurm_gpu_total_memory", "Slurm gpu total memory", gpuMetricLabels, nil),
		usedMemory:       prometheus.NewDesc("slurm_gpu_used_memory", "Slurm gpu used memory", gpuMetricLabels, nil),
		totalGPUUsage:    prometheus.NewDesc("slurm_gpu_total_usage", "Slurm gpu total usage", gpuMetricLabels, nil),
		totalMemoryUsage: prometheus.NewDesc("slurm_gpu_memory_total_usage", "Slurm gpu total memory usage", gpuMetricLabels, nil),
		gpuTemp:          prometheus.NewDesc("slurm_gpu_temperature", "Slurm gpu temperature", gpuMetricLabels, nil),
	}
}

type GPUsCollector struct {
	gpuInfo          *prometheus.Desc
	gpuUsage         *prometheus.Desc
	allocatedMemory  *prometheus.Desc
	totalMemory      *prometheus.Desc
	usedMemory       *prometheus.Desc
	totalGPUUsage    *prometheus.Desc
	totalMemoryUsage *prometheus.Desc
	gpuTemp          *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.gpuInfo
	ch <- cc.gpuUsage
	ch <- cc.totalMemory
	ch <- cc.usedMemory
	ch <- cc.totalGPUUsage
	ch <- cc.totalMemoryUsage
	ch <- cc.gpuTemp
}

func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	gpusInfo, nvidia := GPUsGetMetrics()
	for gpu := range gpusInfo {
		ch <- prometheus.MustNewConstMetric(cc.gpuInfo, prometheus.GaugeValue, float64(0), gpusInfo[gpu].name, gpusInfo[gpu].driverVersion, gpusInfo[gpu].pstate, gpusInfo[gpu].vbiosVersion, gpusInfo[gpu].hostname, gpusInfo[gpu].index, gpusInfo[gpu].migMode)
		ch <- prometheus.MustNewConstMetric(cc.totalMemory, prometheus.GaugeValue, gpusInfo[gpu].memoryTotal, gpusInfo[gpu].hostname, gpusInfo[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.usedMemory, prometheus.GaugeValue, gpusInfo[gpu].memoryUsed, gpusInfo[gpu].hostname, gpusInfo[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.totalGPUUsage, prometheus.GaugeValue, gpusInfo[gpu].totalGPUUsage, gpusInfo[gpu].hostname, gpusInfo[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.totalMemoryUsage, prometheus.GaugeValue, gpusInfo[gpu].totalMemoryUsage, gpusInfo[gpu].hostname, gpusInfo[gpu].index)
		ch <- prometheus.MustNewConstMetric(cc.gpuTemp, prometheus.GaugeValue, gpusInfo[gpu].temperature, gpusInfo[gpu].hostname, gpusInfo[gpu].index)
	}
	for job := range nvidia {
		ch <- prometheus.MustNewConstMetric(cc.gpuUsage, prometheus.GaugeValue, nvidia[job].gpuUsage, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].migName)
		ch <- prometheus.MustNewConstMetric(cc.allocatedMemory, prometheus.GaugeValue, nvidia[job].allocatedMemory, job, nvidia[job].hostname, nvidia[job].index, nvidia[job].migName)
	}
}
