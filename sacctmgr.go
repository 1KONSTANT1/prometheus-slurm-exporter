package main

import (
	"log"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func NewAcctData() []byte {
	cmd := exec.Command("sacctmgr", "-n", "-p", "show", "assoc")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing sacctmgr assoc command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing sacctmgr assoc command: %v", err)
		}
		return []byte("")
	}
	return out
}

func GetQOSData() []byte {
	cmd := exec.Command("sacctmgr", "-n", "-p", "show", "qos")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing sacctmgr qos command: %v, stderr: %s", err, exitErr.Stderr)
		} else {
			log.Printf("Error executing sacctmgr qos command: %v", err)
		}
		return []byte("")
	}
	return out
}

type AcctMetrics struct {
	cluster        string
	account        string
	user           string
	partition      string
	share          string
	priority       string
	grpjobs        string
	grptres        string
	grpsubmit      string
	grpwall        string
	grptresmins    string
	maxjobs        string
	maxtres        string
	maxtrespernode string
	maxsubmit      string
	maxwall        string
	maxtresmins    string
	qos            string
	defqos         string
	grptresrunmin  string
}

type QOSMetrics struct {
	priority         string
	gracetime        string
	preemt           string
	preemtexempttime string
	preemtmode       string
	flags            string
	usagethres       string
	usagefactor      string
	grptres          string
	grptresmins      string
	grptresrunmin    string
	grpjobs          string
	grpsubmit        string
	grpwall          string
	maxtres          string
	maxtrespernode   string
	maxtresmins      string
	maxwall          string
	maxtrespu        string
	maxjobspu        string
	maxsubmitpu      string
	maxtrespa        string
	maxjobspa        string
	maxsubmitpa      string
	mintres          string
}

func ParseAcctMetrics() (map[int]*AcctMetrics, map[string]*QOSMetrics) {
	assocs := make(map[int]*AcctMetrics)
	lines := strings.Split(string(NewAcctData()), "\n")
	i := 0

	for _, line := range lines {
		if strings.Contains(line, "|") {
			split := strings.Split(line, "|")
			assocs[i] = &AcctMetrics{}
			assocs[i].cluster = split[0]
			assocs[i].account = split[1]
			if split[2] == "" {
				assocs[i].user = "None"
			} else {
				assocs[i].user = split[2]
			}
			if split[3] == "" {
				assocs[i].partition = "None"
			} else {
				assocs[i].partition = split[3]
			}
			if split[4] == "" {
				assocs[i].share = "None"
			} else {
				assocs[i].share = split[4]
			}
			if split[5] == "" {
				assocs[i].priority = "None"
			} else {
				assocs[i].priority = split[5]
			}
			if split[6] == "" {
				assocs[i].grpjobs = "None"
			} else {
				assocs[i].grpjobs = split[6]
			}
			if split[7] == "" {
				assocs[i].grptres = "None"
			} else {
				assocs[i].grptres = split[7]
			}
			if split[8] == "" {
				assocs[i].grpsubmit = "None"
			} else {
				assocs[i].grpsubmit = split[8]
			}
			if split[9] == "" {
				assocs[i].grpwall = "None"
			} else {
				assocs[i].grpwall = split[9]
			}
			if split[10] == "" {
				assocs[i].grptresmins = "None"
			} else {
				assocs[i].grptresmins = split[10]
			}
			if split[11] == "" {
				assocs[i].maxjobs = "None"
			} else {
				assocs[i].maxjobs = split[11]
			}
			if split[12] == "" {
				assocs[i].maxtres = "None"
			} else {
				assocs[i].maxtres = split[12]
			}
			if split[13] == "" {
				assocs[i].maxtrespernode = "None"
			} else {
				assocs[i].maxtrespernode = split[13]
			}
			if split[14] == "" {
				assocs[i].maxsubmit = "None"
			} else {
				assocs[i].maxsubmit = split[14]
			}
			if split[15] == "" {
				assocs[i].maxwall = "None"
			} else {
				assocs[i].maxwall = split[15]
			}
			if split[16] == "" {
				assocs[i].maxtresmins = "None"
			} else {
				assocs[i].maxtresmins = split[16]
			}
			if split[17] == "" {
				assocs[i].qos = "None"
			} else {
				assocs[i].qos = split[17]
			}
			if split[18] == "" {
				assocs[i].defqos = "None"
			} else {
				assocs[i].defqos = split[18]
			}
			if split[19] == "" {
				assocs[i].grptresrunmin = "None"
			} else {
				assocs[i].grptresrunmin = split[19]
			}

		}
		i = i + 1
	}
	qoss := make(map[string]*QOSMetrics)
	lines = strings.Split(string(GetQOSData()), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {
			split := strings.Split(line, "|")
			qos := split[0]
			qoss[qos] = &QOSMetrics{}
			if split[1] == "" {
				qoss[qos].priority = "None"
			} else {
				qoss[qos].priority = split[1]
			}
			if split[2] == "" {
				qoss[qos].gracetime = "None"
			} else {
				qoss[qos].gracetime = split[2]
			}
			if split[3] == "" {
				qoss[qos].preemt = "None"
			} else {
				qoss[qos].preemt = split[3]
			}
			if split[4] == "" {
				qoss[qos].preemtexempttime = "None"
			} else {
				qoss[qos].preemtexempttime = split[4]
			}
			if split[5] == "" {
				qoss[qos].preemtmode = "None"
			} else {
				qoss[qos].preemtmode = split[5]
			}
			if split[6] == "" {
				qoss[qos].flags = "None"
			} else {
				qoss[qos].flags = split[6]
			}
			if split[7] == "" {
				qoss[qos].usagethres = "None"
			} else {
				qoss[qos].usagefactor = split[7]
			}
			if split[8] == "" {
				qoss[qos].grptres = "None"
			} else {
				qoss[qos].grptres = split[8]
			}
			if split[9] == "" {
				qoss[qos].grptresmins = "None"
			} else {
				qoss[qos].grptresmins = split[9]
			}
			if split[10] == "" {
				qoss[qos].grptresrunmin = "None"
			} else {
				qoss[qos].grptresrunmin = split[10]
			}
			if split[11] == "" {
				qoss[qos].grpjobs = "None"
			} else {
				qoss[qos].grpjobs = split[11]
			}
			if split[12] == "" {
				qoss[qos].grpsubmit = "None"
			} else {
				qoss[qos].grpsubmit = split[12]
			}
			if split[13] == "" {
				qoss[qos].grpwall = "None"
			} else {
				qoss[qos].grpwall = split[13]
			}
			if split[14] == "" {
				qoss[qos].maxtres = "None"
			} else {
				qoss[qos].maxtres = split[14]
			}
			if split[15] == "" {
				qoss[qos].maxtrespernode = "None"
			} else {
				qoss[qos].maxtrespernode = split[15]
			}
			if split[16] == "" {
				qoss[qos].maxtresmins = "None"
			} else {
				qoss[qos].maxtresmins = split[16]
			}
			if split[17] == "" {
				qoss[qos].maxtresmins = "None"
			} else {
				qoss[qos].maxtresmins = split[17]
			}
			if split[18] == "" {
				qoss[qos].maxwall = "None"
			} else {
				qoss[qos].maxwall = split[18]
			}
			if split[19] == "" {
				qoss[qos].maxtrespu = "None"
			} else {
				qoss[qos].maxtrespu = split[19]
			}
			if split[20] == "" {
				qoss[qos].maxjobspu = "None"
			} else {
				qoss[qos].maxjobspu = split[20]
			}
			if split[21] == "" {
				qoss[qos].maxsubmitpu = "None"
			} else {
				qoss[qos].maxsubmitpu = split[21]
			}
			if split[22] == "" {
				qoss[qos].maxtrespa = "None"
			} else {
				qoss[qos].maxtrespa = split[22]
			}
			if split[23] == "" {
				qoss[qos].maxjobspa = "None"
			} else {
				qoss[qos].maxjobspa = split[23]
			}
			if split[24] == "" {
				qoss[qos].maxsubmitpa = "None"
			} else {
				qoss[qos].maxsubmitpa = split[24]
			}
			if split[25] == "" {
				qoss[qos].mintres = "None"
			} else {
				qoss[qos].mintres = split[25]
			}

		}
	}

	return assocs, qoss
}

type AcctCollector struct {
	assoc *prometheus.Desc
	qos   *prometheus.Desc
}

func NewAssocCollector() *AcctCollector {
	acc_labels := []string{"Cluster", "Account", "User", "Partition", "Share", "Priority", "GrpJobs", "GrpTRES", "GrpSubmit", "GrpWall", "GrpTRESMins", "MaxJobs", "MaxTRES", "MaxTRESPerNode", "MaxSubmit", "MaxWall", "MaxTRESMins", "QOS", "Def_QOS", "GrpTRESRunMin"}
	qos_labels := []string{"Name", "Priority", "GraceTime", "Preempt", "PreemptExemptTime", "PreemptMode", "Flags", "UsageThres", "UsageFactor", "GrpTRES", "GrpTRESMins", "GrpTRESRunMin", "GrpJobs", "GrpSubmit", "GrpWall", "MaxTRES", "MaxTRESPerNode", "MaxTRESMins", "MaxWall", "MaxTRESPU", "MaxJobsPU", "MaxSubmitPU", "MaxTRESPA", "MaxJobsPA", "MaxSubmitPA", "MinTRES"}
	return &AcctCollector{
		assoc: prometheus.NewDesc("slurm_sacct_assoc", "Info about slurm accounts", acc_labels, nil),
		qos:   prometheus.NewDesc("slurm_sacct_qos", "Info about qos", qos_labels, nil),
	}
}

func (pc *AcctCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.assoc
	ch <- pc.qos
}

func (pc *AcctCollector) Collect(ch chan<- prometheus.Metric) {
	assocs, qoss := ParseAcctMetrics()
	for assoc := range assocs {
		ch <- prometheus.MustNewConstMetric(pc.assoc, prometheus.GaugeValue, float64(0), assocs[assoc].cluster, assocs[assoc].account, assocs[assoc].user, assocs[assoc].partition, assocs[assoc].share, assocs[assoc].priority, assocs[assoc].grpjobs, assocs[assoc].grptres, assocs[assoc].grpsubmit, assocs[assoc].grpwall, assocs[assoc].grptresmins, assocs[assoc].maxjobs, assocs[assoc].maxtres, assocs[assoc].maxtrespernode, assocs[assoc].maxsubmit, assocs[assoc].maxwall, assocs[assoc].maxtresmins, assocs[assoc].qos, assocs[assoc].defqos, assocs[assoc].grptresrunmin)
	}
	for qos := range qoss {
		ch <- prometheus.MustNewConstMetric(pc.qos, prometheus.GaugeValue, float64(0), qos, qoss[qos].priority, qoss[qos].gracetime, qoss[qos].preemt, qoss[qos].preemtexempttime, qoss[qos].preemtmode, qoss[qos].flags, qoss[qos].usagefactor, qoss[qos].grptres, qoss[qos].grptresmins, qoss[qos].grptresrunmin, qoss[qos].grpjobs, qoss[qos].grpsubmit, qoss[qos].priority, qoss[qos].grpwall, qoss[qos].maxtres, qoss[qos].maxtrespernode, qoss[qos].maxtresmins, qoss[qos].maxwall, qoss[qos].maxtrespu, qoss[qos].maxjobspu, qoss[qos].maxsubmitpu, qoss[qos].maxtrespa, qoss[qos].maxjobspa, qoss[qos].maxsubmitpa, qoss[qos].mintres)
	}
}
