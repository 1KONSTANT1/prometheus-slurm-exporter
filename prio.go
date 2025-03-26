package main

import (
	"log"
	"os/exec"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobsMetrics stores metrics for each node
type PrioMetrics struct {
	priority         string
	age_factor       string
	assoc_factor     string
	partition_factor string
	jobsize_factor   string
	qos_name         string
	nice_factor      string
	account          string
	qos_factor       string
	partition        string
	tres_factor      string
	user             string
}

func PrioGetMetrics() map[string]*PrioMetrics {
	return ParsePrioMetrics(PrioData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParsePrioMetrics(input []byte) map[string]*PrioMetrics {
	priorities := make(map[string]*PrioMetrics)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		if strings.Contains(line, "|") {
			split := strings.Split(line, "|")
			jobid := split[0]
			jobid = jobid[2:]
			priorities[jobid] = &PrioMetrics{"", "", "", "", "", "", "", "", "", "", "", ""}
			priorities[jobid].priority = split[1]
			priorities[jobid].age_factor = split[2]
			priorities[jobid].assoc_factor = split[3]
			priorities[jobid].partition_factor = split[4]
			priorities[jobid].jobsize_factor = split[5]
			priorities[jobid].qos_name = split[6]
			priorities[jobid].nice_factor = split[7]
			priorities[jobid].account = split[8]

			priorities[jobid].qos_factor = split[9]
			priorities[jobid].partition = split[10]
			priorities[jobid].tres_factor = split[11]
			priorities[jobid].user = split[12][:len(split[12])-1]

		}
	}

	return priorities
}

// NodeData executes the sinfo command to get data for each node
// It returns the output of the sinfo command
func PrioData() []byte {
	cmd := exec.Command("sprio", "-h", "-o \"%i|%Y|%A|%B|%P|%J|%n|%N|%o|%Q|%r|%T|%u\"")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type PrioCollector struct {
	prio *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewPrioCollector() *PrioCollector {
	labels := []string{"JOBID", "PRIORITY", "AGE_FACT", "ASSOC_FACT", "PARTITION_FACT", "JOBSIZE_FACT", "QOS", "NICE_FACT", "ACCOUNT", "QOS_FACT", "PARTITION", "TRES_FACT", "USER"}

	return &PrioCollector{
		prio: prometheus.NewDesc("slurm_prio", "JOB's priority", labels, nil),
	}
}

// Send all metric descriptions
func (nc *PrioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.prio
}

func (nc *PrioCollector) Collect(ch chan<- prometheus.Metric) {
	priorities := PrioGetMetrics()
	for job := range priorities {
		ch <- prometheus.MustNewConstMetric(nc.prio, prometheus.GaugeValue, float64(0), job, priorities[job].priority, priorities[job].age_factor, priorities[job].assoc_factor, priorities[job].partition_factor, priorities[job].jobsize_factor, priorities[job].qos_name, priorities[job].nice_factor, priorities[job].account, priorities[job].qos_factor, priorities[job].partition, priorities[job].tres_factor, priorities[job].user)
	}
}
