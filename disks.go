package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobsMetrics stores metrics for each node
type DiskMetrics struct {
	fsize       float64
	size_avail  float64
	size_used   float64
	size        float64
	hostname    string
	device_type string
	parent_name string
	disk_total  string
	mountpoints string
}

type DiskStats struct {
	R_IOPS   float64
	W_IOPS   float64
	hostname string
}

type Jobio struct {
	read     float64
	write    float64
	hostname string
}

func DiskGetMetrics() (map[string]*DiskMetrics, map[string]*Jobio, map[string]*DiskStats) {
	return ParseDiskMetrics(ExecuteCommand(LSBLK))
}

func readProcIO(pid string) string {
	// Make path to /proc/{{pid}}/io
	procIOPath := fmt.Sprintf("/proc/%s/io", pid)

	// Read file data
	data, err := ioutil.ReadFile(procIOPath)
	if err != nil {
		if _, err := os.Stat(procIOPath); os.IsNotExist(err) {
			return "rchar: 0\nwchar: 0"
		} else if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				log.Printf("Error reading from %s pid: %v, stderr: %s", pid, err, exitErr.Stderr)
			} else {
				log.Printf("Error reading from %s pid: %v", pid, err)
			}
			return ""
		}
	}

	return string(data)
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseDiskMetrics(input []byte) (map[string]*DiskMetrics, map[string]*Jobio, map[string]*DiskStats) {
	disk_info := make(map[string]*DiskMetrics)
	hostname := string(ExecuteCommand(HOSTNAME))
	hostname = strings.ReplaceAll(hostname, "\n", "")

	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if len(line) < 2 {
			continue
		}
		split := strings.Split(line, " ")
		disk_name := strings.Split(split[0], "=")[1]
		disk_name = disk_name[1 : len(disk_name)-1]
		disk_info[disk_name] = &DiskMetrics{}
		if strings.Split(split[1], "=")[1] != "\"\"" {
			disk_info[disk_name].size_avail, _ = strconv.ParseFloat(strings.Split(split[1], "=")[1][1:len(strings.Split(split[1], "=")[1])-1], 64)
		} else {
			disk_info[disk_name].size_avail = -1
		}
		if strings.Split(split[2], "=")[1] != "\"\"" {
			disk_info[disk_name].fsize, _ = strconv.ParseFloat(strings.Split(split[2], "=")[1][1:len(strings.Split(split[2], "=")[1])-1], 64)
		} else {
			disk_info[disk_name].fsize = -10
		}
		disk_info[disk_name].size, _ = strconv.ParseFloat(strings.Split(split[3], "=")[1][1:len(strings.Split(split[3], "=")[1])-1], 64)
		disk_info[disk_name].size_used = disk_info[disk_name].fsize - disk_info[disk_name].size_avail
		disk_info[disk_name].hostname = hostname
		disk_info[disk_name].device_type = strings.Split(split[4], "=")[1][1 : len(strings.Split(split[4], "=")[1])-1]
		if strings.Split(split[5], "=")[1] != "\"\"" {
			disk_info[disk_name].parent_name = strings.Split(split[5], "=")[1][1 : len(strings.Split(split[5], "=")[1])-1]
		} else {
			disk_info[disk_name].parent_name = disk_name
		}

		if disk_info[disk_name].parent_name == disk_name {
			disk_info[disk_name].disk_total = disk_name
		} else {
			str := disk_info[disk_name].parent_name
			for {
				if disk_info[str].parent_name != str {
					str = disk_info[disk_info[str].parent_name].parent_name
				} else {
					disk_info[disk_name].disk_total = str
					break
				}
			}
		}

		if strings.Split(split[6], "=")[1] != "\"\"" {
			disk_info[disk_name].mountpoints = strings.Split(split[6], "=")[1][1 : len(strings.Split(split[6], "=")[1])-1]
		} else {
			disk_info[disk_name].mountpoints = "None"
		}

	}

	pids_lines, err := ShowPids()
	jobs_io := make(map[string]*Jobio)
	if err == nil {
		lines = strings.Split(string(pids_lines), "\n")
		lines = lines[1 : len(lines)-1]
		for _, line := range lines {
			split := strings.Fields(line)
			pid_io_lines := strings.Split(readProcIO(split[0]), "\n")
			if _, exists := jobs_io[split[1]]; !exists {
				// If no key, make new value
				jobs_io[split[1]] = &Jobio{}
				jobs_io[split[1]].read, _ = strconv.ParseFloat(strings.Split(pid_io_lines[0], " ")[1], 64)
				jobs_io[split[1]].write, _ = strconv.ParseFloat(strings.Split(pid_io_lines[1], " ")[1], 64)
				jobs_io[split[1]].hostname = hostname
			} else {
				// If key exists, add to it
				read_sum, _ := strconv.ParseFloat(strings.Split(pid_io_lines[0], " ")[1], 64)
				jobs_io[split[1]].read = jobs_io[split[1]].read + read_sum
				write_sum, _ := strconv.ParseFloat(strings.Split(pid_io_lines[1], " ")[1], 64)
				jobs_io[split[1]].write = jobs_io[split[1]].write + write_sum
			}

		}
	}

	disk_ops := make(map[string]*DiskStats)
	lines = strings.Split(GetDiskstatsAsString(), "\n")
	//lines = lines[3 : len(lines)-1]
	lines = RemoveDuplicates(lines)
	for _, line := range lines {
		fields := strings.Fields(line)
		disk_ops[fields[2]] = &DiskStats{}
		disk_ops[fields[2]].hostname = hostname
		disk_ops[fields[2]].R_IOPS, err = strconv.ParseFloat(fields[3], 64)
		if err != nil {
			log.Printf("Error parsing  r/s for %s device: %v", fields[2], err)
		}
		disk_ops[fields[2]].W_IOPS, err = strconv.ParseFloat(fields[7], 64)
		if err != nil {
			log.Printf("Error parsing  w/s for %s device: %v", fields[2], err)
		}
	}

	return disk_info, jobs_io, disk_ops
}

func GetDiskstatsAsString() string {
	data, err := ioutil.ReadFile("/proc/diskstats")
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error opening /proc/diskstats file: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error opening /proc/diskstats file: %v", err)
		}
		return ""
	}
	return string(data)
}

type DiskCollector struct {
	disk_fsize      *prometheus.Desc
	disk_size_avail *prometheus.Desc
	disk_size_used  *prometheus.Desc
	disk_size       *prometheus.Desc
	jobs_read_disk  *prometheus.Desc
	jobs_write_disk *prometheus.Desc
	disk_write_iops *prometheus.Desc
	disk_read_iops  *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewDiskCollector() *DiskCollector {
	disk_labels := []string{"DISK", "HOSTNAME", "TYPE", "PARENT", "DISK_TOTAL", "MOUNTPOINTS"}
	job_disk_labels := []string{"JOBID", "HOSTNAME"}
	iops_disk_labels := []string{"DISK", "HOSTNAME"}
	return &DiskCollector{
		disk_fsize:      prometheus.NewDesc("slurm_disk_filesystemsize", "DISK fsize", disk_labels, nil),
		disk_size_avail: prometheus.NewDesc("slurm_disk_size_avail", "DISK size avail", disk_labels, nil),
		disk_size_used:  prometheus.NewDesc("slurm_disk_size_used", "DISK size used", disk_labels, nil),
		disk_size:       prometheus.NewDesc("slurm_disk_size", "DISK size", disk_labels, nil),
		jobs_read_disk:  prometheus.NewDesc("slurm_disk_jobs_read", "SLURM JOBS READ FROM DISK", job_disk_labels, nil),
		jobs_write_disk: prometheus.NewDesc("slurm_disk_jobs_write", "SLURM JOBS WRITE TO DISK", job_disk_labels, nil),
		disk_write_iops: prometheus.NewDesc("slurm_disk_write_iops", "DiSK write iops", iops_disk_labels, nil),
		disk_read_iops:  prometheus.NewDesc("slurm_disk_read_iops", "DiSK read iops", iops_disk_labels, nil),
	}
}

// Send all metric descriptions
func (nc *DiskCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.disk_fsize
	ch <- nc.disk_size_avail
	ch <- nc.disk_size_used
	ch <- nc.disk_size
	ch <- nc.jobs_read_disk
	ch <- nc.jobs_write_disk
	ch <- nc.disk_write_iops
	ch <- nc.disk_read_iops
}

func (nc *DiskCollector) Collect(ch chan<- prometheus.Metric) {
	disks, jobs_io, iops_disk := DiskGetMetrics()
	for disk := range disks {
		ch <- prometheus.MustNewConstMetric(nc.disk_fsize, prometheus.GaugeValue, disks[disk].fsize, disk, disks[disk].hostname, disks[disk].device_type, disks[disk].parent_name, disks[disk].disk_total, disks[disk].mountpoints)
		ch <- prometheus.MustNewConstMetric(nc.disk_size, prometheus.GaugeValue, disks[disk].size, disk, disks[disk].hostname, disks[disk].device_type, disks[disk].parent_name, disks[disk].disk_total, disks[disk].mountpoints)
		ch <- prometheus.MustNewConstMetric(nc.disk_size_avail, prometheus.GaugeValue, disks[disk].size_avail, disk, disks[disk].hostname, disks[disk].device_type, disks[disk].parent_name, disks[disk].disk_total, disks[disk].mountpoints)
		ch <- prometheus.MustNewConstMetric(nc.disk_size_used, prometheus.GaugeValue, disks[disk].size_used, disk, disks[disk].hostname, disks[disk].device_type, disks[disk].parent_name, disks[disk].disk_total, disks[disk].mountpoints)
	}
	for job := range jobs_io {
		ch <- prometheus.MustNewConstMetric(nc.jobs_read_disk, prometheus.GaugeValue, jobs_io[job].read, job, jobs_io[job].hostname)
		ch <- prometheus.MustNewConstMetric(nc.jobs_write_disk, prometheus.GaugeValue, jobs_io[job].write, job, jobs_io[job].hostname)
	}
	for disk := range iops_disk {
		ch <- prometheus.MustNewConstMetric(nc.disk_write_iops, prometheus.GaugeValue, iops_disk[disk].W_IOPS, disk, iops_disk[disk].hostname)
		ch <- prometheus.MustNewConstMetric(nc.disk_read_iops, prometheus.GaugeValue, iops_disk[disk].R_IOPS, disk, iops_disk[disk].hostname)
	}
}
