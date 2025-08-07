package main

import (
	"log"
	"os/exec"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobsMetrics stores metrics for each node
type NodeResMetrics struct {
	cpu_alloc         string
	cpu_total         string
	cpu_load          string
	real_mem          string
	alloc_mem         string
	free_mem          string
	state             string
	partitions        string
	reason            string
	last_busy_time    string
	boot_time         string
	slurmd_start_time string
	ip                string
}

func NodeResGetMetrics() map[string]*NodeResMetrics {
	return ParseNodeResMetrics(NodeResData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseNodeResMetrics(input []byte, input2 []byte) map[string]*NodeResMetrics {
	nodes := make(map[string]*NodeResMetrics)
	lines := strings.Split(string(input), "\n")
	lines_etc := strings.Split(string(input2), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		node_info := strings.Split(line, " ")
		nodeid := strings.Split(node_info[0], "=")[1]
		nodes[nodeid] = &NodeResMetrics{}
		for i := range node_info {
			if strings.Contains(node_info[i], "CPUAlloc") {
				nodes[nodeid].cpu_alloc = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "CPUTot") {
				nodes[nodeid].cpu_total = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "CPULoad") {
				nodes[nodeid].cpu_load = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "RealMemory") {
				nodes[nodeid].real_mem = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "AllocMem") {
				nodes[nodeid].alloc_mem = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "FreeMem") {
				nodes[nodeid].free_mem = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "State") {
				nodes[nodeid].state = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "Partitions") {
				nodes[nodeid].partitions = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "LastBusyTime") {
				nodes[nodeid].last_busy_time = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "BootTime") {
				nodes[nodeid].boot_time = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "SlurmdStartTime") {
				nodes[nodeid].slurmd_start_time = strings.Split(node_info[i], "=")[1]
			}
			if strings.Contains(node_info[i], "Reason") {
				res := strings.Split(node_info[i], "=")[1]
				if i < len(node_info) {
					for j := i + 1; j < len(node_info); j++ {
						res = res + " " + node_info[j]
					}

					nodes[nodeid].reason = res
				} else {
					nodes[nodeid].reason = res
				}
			}
		}
		if nodes[nodeid].reason == "" {
			nodes[nodeid].reason = "OK"
		}
		for _, line := range lines_etc {
			if strings.Contains(line, nodeid) {
				nodes[nodeid].ip = strings.Split(line, " ")[0]
			}
		}
	}

	return nodes
}

// NodeData executes the sinfo command to get data for each node
// It returns the output of the sinfo command
func NodeResData() ([]byte, []byte) {
	cmd := exec.Command("scontrol", "show", "nodes", "-d", "-o")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing scontrol show nodes command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing scontrol show nodes command: %v", err)
		}
		out = []byte("")
	}
	cmd2 := exec.Command("cat", "/etc/hosts")
	out2, err := cmd2.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing cat /etc/hosts command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing cat /etc/hosts command: %v", err)
		}
		out2 = []byte("")
	}
	return out, out2
}

type NodeResCollector struct {
	node_res *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewNodeResCollector() *NodeResCollector {
	node_res_labels := []string{"NODE_NAME", "CPUAlloc", "CPUTot", "CPULoad", "RealMemory", "AllocMem", "FreeMem", "STATE", "PARTITIONS", "LastBusyTime", "BootTime", "SlurmdStartTime", "Reason", "IP"}

	return &NodeResCollector{
		node_res: prometheus.NewDesc("slurm_node_resources", "NODE RESOURCES", node_res_labels, nil),
	}
}

// Send all metric descriptions
func (nc *NodeResCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.node_res
}

func (nc *NodeResCollector) Collect(ch chan<- prometheus.Metric) {
	nodes := NodeResGetMetrics()
	for node := range nodes {
		ch <- prometheus.MustNewConstMetric(nc.node_res, prometheus.GaugeValue, float64(0), node, nodes[node].cpu_alloc, nodes[node].cpu_total, nodes[node].cpu_load, nodes[node].real_mem, nodes[node].alloc_mem, nodes[node].free_mem, nodes[node].state, nodes[node].partitions, nodes[node].last_busy_time, nodes[node].boot_time, nodes[node].slurmd_start_time, nodes[node].reason, nodes[node].ip)
	}
}
