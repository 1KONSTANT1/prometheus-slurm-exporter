package main

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobsMetrics stores metrics for each node
type DiskMetrics struct {
	fsize      float64
	size_avail float64
	size_used  float64
	size       float64
	hostname   string
}

func DiskGetMetrics() map[string]*DiskMetrics {
	return ParseDiskMetrics(DiskData())
}

func ConvertToBytes(sizeStr string) int64 {
	// Trim any whitespace from the input
	sizeStr = strings.TrimSpace(sizeStr)

	// Get the last character to determine the unit
	unit := sizeStr[len(sizeStr)-1]
	// Get the numeric part of the string
	valueStr := sizeStr[:len(sizeStr)-1]

	// Parse the numeric part
	value, _ := strconv.ParseFloat(valueStr, 64)

	// Convert to bytes based on the unit
	var bytes int64
	switch unit {
	case 'K':
		bytes = int64(value * 1024) // 1 KB = 1024 bytes
	case 'M':
		bytes = int64(value * 1024 * 1024) // 1 MB = 1024 * 1024 bytes
	case 'G':
		bytes = int64(value * 1024 * 1024 * 1024) // 1 GB = 1024 * 1024 * 1024 bytes
	default:
		return -1
	}

	return bytes
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseDiskMetrics(input []byte) map[string]*DiskMetrics {
	disk_info := make(map[string]*DiskMetrics)
	lines := strings.Split(string(input), "\n")
	fmt.Println(lines)

	for _, line := range lines {
		if len(line) < 2 {
			continue
		}
		split := strings.Split(line, " ")
		disk_name := strings.Split(split[0], "=")[1]
		disk_name = disk_name[1 : len(disk_name)-1]
		disk_info[disk_name] = &DiskMetrics{0, 0, 0, 0, ""}
		fmt.Println("YO")
		if strings.Split(split[1], "=")[1] != "\"\"" {
			disk_info[disk_name].size_avail, _ = strconv.ParseFloat(strings.Split(split[1], "=")[1][1:len(strings.Split(split[1], "=")[1])-1], 64)
		} else {
			disk_info[disk_name].size_avail = -1
		}
		fmt.Println("YOOOOOOOOOOOOOOO")
		if strings.Split(split[2], "=")[1] != "\"\"" {
			disk_info[disk_name].fsize, _ = strconv.ParseFloat(strings.Split(split[2], "=")[1][1:len(strings.Split(split[2], "=")[1])-1], 64)
		} else {
			disk_info[disk_name].fsize = -10
		}
		disk_info[disk_name].size, _ = strconv.ParseFloat(strings.Split(split[3], "=")[1][1:len(strings.Split(split[3], "=")[1])-1], 64)
		disk_info[disk_name].size_used = disk_info[disk_name].fsize - disk_info[disk_name].size_avail
		hostname := string(PartitionsData())
		if strings.Contains(hostname, ".") {
			hostname = strings.Split(hostname, ".")[0]
		}
		disk_info[disk_name].hostname = hostname

	}

	return disk_info
}

// NodeData executes the sinfo command to get data for each node
// It returns the output of the sinfo command
func DiskData() []byte {
	fmt.Println("WTFkvpowvwevwe")
	cmd := exec.Command("lsblk", "-Pb", "-o", "NAME,FSAVAIL,FSSIZE,SIZE")
	out, err := cmd.Output()
	fmt.Println(out)
	if err != nil {
		log.Fatal(err)
	}
	return out
}
func GetHostName() []byte {
	cmd := exec.Command("hostname")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type DiskCollector struct {
	disk_fsize      *prometheus.Desc
	disk_size_avail *prometheus.Desc
	disk_size_used  *prometheus.Desc
	disk_size       *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewDiskCollector() *DiskCollector {
	labels := []string{"DISK", "HOSTNAME"}
	return &DiskCollector{
		disk_fsize:      prometheus.NewDesc("slurm_disk_filesystemsize", "DISK INFOf", labels, nil),
		disk_size_avail: prometheus.NewDesc("slurm_disk_size_avail", "DISK INFOd", labels, nil),
		disk_size_used:  prometheus.NewDesc("slurm_disk_size_used", "DISK INFOs", labels, nil),
		disk_size:       prometheus.NewDesc("slurm_disk_size", "DISK INFOf", labels, nil),
	}
}

// Send all metric descriptions
func (nc *DiskCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.disk_fsize
	ch <- nc.disk_size_avail
	ch <- nc.disk_size_used
	ch <- nc.disk_size
}

func (nc *DiskCollector) Collect(ch chan<- prometheus.Metric) {
	disks := DiskGetMetrics()
	for disk := range disks {
		ch <- prometheus.MustNewConstMetric(nc.disk_fsize, prometheus.GaugeValue, disks[disk].fsize, disk, disks[disk].hostname)
		ch <- prometheus.MustNewConstMetric(nc.disk_size, prometheus.GaugeValue, disks[disk].size, disk, disks[disk].hostname)
		ch <- prometheus.MustNewConstMetric(nc.disk_size_avail, prometheus.GaugeValue, disks[disk].size_avail, disk, disks[disk].hostname)
		ch <- prometheus.MustNewConstMetric(nc.disk_size_used, prometheus.GaugeValue, disks[disk].size_used, disk, disks[disk].hostname)
	}
}
