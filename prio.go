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

type PriorityConfigs struct {
	PriorityParameters           string
	PrioritySiteFactorParameters string
	PrioritySiteFactorPlugin     string
	PriorityDecayHalfLife        string
	PriorityCalcPeriod           string
	PriorityFavorSmall           string
	PriorityFlags                string
	PriorityMaxAge               string
	PriorityUsageResetPeriod     string
	PriorityType                 string
	PriorityWeightAge            string
	PriorityWeightAssoc          string
	PriorityWeightFairShare      string
	PriorityWeightJobSize        string
	PriorityWeightPartition      string
	PriorityWeightQOS            string
	PriorityWeightTRES           string
}

func PrioGetMetrics() (map[string]*PrioMetrics, PriorityConfigs) {
	return ParsePrioMetrics(PrioData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParsePrioMetrics(input []byte) (map[string]*PrioMetrics, PriorityConfigs) {
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
	config := PriorityConfigs{
		PriorityParameters:           "",
		PrioritySiteFactorParameters: "",
		PrioritySiteFactorPlugin:     "",
		PriorityDecayHalfLife:        "",
		PriorityCalcPeriod:           "",
		PriorityFavorSmall:           "",
		PriorityFlags:                "",
		PriorityMaxAge:               "",
		PriorityUsageResetPeriod:     "",
		PriorityType:                 "",
		PriorityWeightAge:            "",
		PriorityWeightAssoc:          "",
		PriorityWeightFairShare:      "",
		PriorityWeightJobSize:        "",
		PriorityWeightPartition:      "",
		PriorityWeightQOS:            "",
		PriorityWeightTRES:           "",
	}
	lines = strings.Split(string(PrioConfig()), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "PriorityWeightTRES") {
			config.PriorityWeightTRES = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityParameters") {
			config.PriorityParameters = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PrioritySiteFactorParameters") {
			config.PrioritySiteFactorParameters = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PrioritySiteFactorPlugin") {
			config.PrioritySiteFactorPlugin = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityDecayHalfLife") {
			config.PriorityDecayHalfLife = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityCalcPeriod") {
			config.PriorityCalcPeriod = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityFavorSmall") {
			config.PriorityFavorSmall = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityFlags") {
			if len(strings.Fields(line)) == 2 {
				config.PriorityFlags = "(null)"
			} else {
				config.PriorityFlags = strings.Fields(line)[2]
			}
		}
		if strings.HasPrefix(line, "PriorityMaxAge") {
			config.PriorityMaxAge = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityUsageResetPeriod") {
			config.PriorityUsageResetPeriod = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityType") {
			config.PriorityType = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightAge") {
			config.PriorityWeightAge = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightAssoc") {
			config.PriorityWeightAssoc = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightFairShare") {
			config.PriorityWeightFairShare = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightJobSize") {
			config.PriorityWeightJobSize = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightPartition") {
			config.PriorityWeightPartition = strings.Fields(line)[2]
		}
		if strings.HasPrefix(line, "PriorityWeightQOS") {
			config.PriorityWeightQOS = strings.Fields(line)[2]
		}

	}

	return priorities, config
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

func PrioConfig() []byte {
	cmd := exec.Command("scontrol", "show", "conf")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type PrioCollector struct {
	prio     *prometheus.Desc
	prioconf *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewPrioCollector() *PrioCollector {
	labels := []string{"JOBID", "PRIORITY", "AGE_FACT", "ASSOC_FACT", "PARTITION_FACT", "JOBSIZE_FACT", "QOS", "NICE_FACT", "ACCOUNT", "QOS_FACT", "PARTITION", "TRES_FACT", "USER"}

	conf_labels := []string{"PriorityParameters", "PrioritySiteFactorParameters", "PrioritySiteFactorPlugin", "PriorityDecayHalfLife", "PriorityCalcPeriod", "PriorityFavorSmall", "PriorityFlags", "PriorityMaxAge", "PriorityUsageResetPeriod", "PriorityType", "PriorityWeightAge", "PriorityWeightAssoc", "PriorityWeightFairShare", "PriorityWeightJobSize", "PriorityWeightPartition", "PriorityWeightQOS", "PriorityWeightTRES"}

	return &PrioCollector{
		prio: prometheus.NewDesc("slurm_prio", "JOB's priority", labels, nil),

		prioconf: prometheus.NewDesc("slurm_prio_conf", "SLurm Priority Configuration", conf_labels, nil),
	}

}

// Send all metric Descriptions
func (nc *PrioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.prio
}

func (nc *PrioCollector) Collect(ch chan<- prometheus.Metric) {
	priorities, conf := PrioGetMetrics()
	for job := range priorities {
		ch <- prometheus.MustNewConstMetric(nc.prio, prometheus.GaugeValue, float64(0), job, priorities[job].priority, priorities[job].age_factor, priorities[job].assoc_factor, priorities[job].partition_factor, priorities[job].jobsize_factor, priorities[job].qos_name, priorities[job].nice_factor, priorities[job].account, priorities[job].qos_factor, priorities[job].partition, priorities[job].tres_factor, priorities[job].user)
	}
	ch <- prometheus.MustNewConstMetric(nc.prioconf, prometheus.GaugeValue, float64(0), conf.PriorityParameters, conf.PrioritySiteFactorParameters, conf.PrioritySiteFactorPlugin, conf.PriorityDecayHalfLife, conf.PriorityCalcPeriod, conf.PriorityFavorSmall, conf.PriorityFlags, conf.PriorityMaxAge, conf.PriorityUsageResetPeriod, conf.PriorityType, conf.PriorityWeightAge, conf.PriorityWeightAssoc, conf.PriorityWeightFairShare, conf.PriorityWeightJobSize, conf.PriorityWeightPartition, conf.PriorityWeightQOS, conf.PriorityWeightTRES)
}
