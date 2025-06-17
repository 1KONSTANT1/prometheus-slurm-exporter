package main

import (
	"log"
	"os/exec"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// JobsMetrics stores metrics for each node
type JobsMetrics struct {
	sub_time      string
	start_time    string
	end_time      string
	time_limit    string
	status        string
	user          string
	group         string
	priority      string
	run_time      string
	nodes         string
	cpus          string
	min_mem       string
	partition     string
	account       string
	reason        string
	min_tmp_disk  string
	tres_per_node string
}

func JobGetMetrics() map[string]*JobsMetrics {
	return ParseJobMetrics(JobData())
}

// ParseNodeMetrics takes the output of sinfo with node data
// It returns a map of metrics per node
func ParseJobMetrics(input []byte) map[string]*JobsMetrics {
	jobs := make(map[string]*JobsMetrics, 15)
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	linesUniq := RemoveDuplicates(lines)

	for _, line := range linesUniq {
		if strings.Contains(line, "|") {
			split := strings.Split(line, "|")
			jobid := strings.Fields(split[0])[0]
			jobs[jobid] = &JobsMetrics{}
			jobs[jobid].sub_time = split[1]
			jobs[jobid].start_time = split[2]
			jobs[jobid].end_time = split[3]
			jobs[jobid].time_limit = split[4]
			jobs[jobid].status = split[7]
			jobs[jobid].user = split[9]
			jobs[jobid].group = split[10]
			jobs[jobid].priority = split[11]

			jobs[jobid].run_time = split[6]
			jobs[jobid].nodes = split[12]
			jobs[jobid].cpus = split[13]
			jobs[jobid].min_mem = split[14]
			jobs[jobid].account = split[15]
			jobs[jobid].reason = split[16]
			jobs[jobid].min_tmp_disk = split[17]
			jobs[jobid].tres_per_node = split[18]
			jobs[jobid].partition = strings.Fields(split[19])[0]

		}
	}

	return jobs
}

func JobData() []byte {
	cmd := exec.Command("/bin/bash", "-c", "squeue -a -r -h -O \"JOBID:|,SubmitTime:|,STARTTIME:|,ENDTIME:|,TIMELIMIT:|,TIMELEFT:|,TIMEUSED:|,STATE:|,REASON:|,USERNAME:|,GroupNAME:|,PRIORITYLONG:|,NODELIST:|,NumCPUs:|,MinMemory:|,ACCOUNT:|,ReasonList:|,MinTmpDisk:|,tres-per-node:|,PARTITION\"")
	out, err := cmd.Output()
	if err != nil {
		log.Fatal(err)
	}
	return out
}

type JobCollector struct {
	time *prometheus.Desc
}

// NewNodeCollector creates a Prometheus collector to keep all our stats in
// It returns a set of collections for consumption
func NewJobCollector() *JobCollector {
	labels := []string{"JOBID", "SUBMIT_TIME", "START_TIME", "END_TIME", "TIME_LIMIT", "STATUS", "USER", "GROUP", "PRIORITY", "RUN_TIME", "NODELIST", "CPUS", "MIN_MEM_REQUSTED", "ACCOUNT", "PARTITION", "REASON", "MIN_TMP_DISK", "TRES_PER_NODE"}

	return &JobCollector{
		time: prometheus.NewDesc("slurm_job_time", "TIME FOR JOB", labels, nil),
	}
}

// Send all metric descriptions
func (nc *JobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.time
}

func (nc *JobCollector) Collect(ch chan<- prometheus.Metric) {
	jobs := JobGetMetrics()
	for job := range jobs {
		ch <- prometheus.MustNewConstMetric(nc.time, prometheus.GaugeValue, float64(0), job, jobs[job].sub_time, jobs[job].start_time, jobs[job].end_time, jobs[job].time_limit, jobs[job].status, jobs[job].user, jobs[job].group, jobs[job].priority, jobs[job].run_time, jobs[job].nodes, jobs[job].cpus, jobs[job].min_mem, jobs[job].account, jobs[job].partition, jobs[job].reason, jobs[job].min_tmp_disk, jobs[job].tres_per_node)
	}
}
