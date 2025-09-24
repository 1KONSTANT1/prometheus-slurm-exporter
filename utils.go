package main

import (
	"log"
	"os/exec"
)

const (
	NVDIA_QUERY             string = "nvidia-smi --query-gpu=name,driver_version,vbios_version,pstate,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,power.draw.instant,power.limit,uuid,index,mig.mode.current --format=csv"
	NVIDIA_SMI_MIG_LGIP     string = "nvidia-smi mig -lgip"
	NVIDIA_SMI_MIG_LGI      string = "nvidia-smi mig -lgi"
	DCGMI_DISCOVERY         string = "dcgmi discovery -c"
	NVIDIA_SMI              string = "nvidia-smi"
	NVIDIA_SMI_PMON         string = "nvidia-smi pmon -c 1"
	SACCT_SHOW_ASSOC        string = "sacctmgr -n -p show assoc"
	SACCT_SHOW_QOS          string = "sacctmgr -n -p show qos"
	HOSTNAME                string = "hostname -s"
	SPRIO                   string = "sprio -h -o \"%i|%Y|%A|%B|%P|%J|%n|%N|%o|%Q|%r|%T|%u\""
	SCONTROL_SHOW_CONF      string = "scontrol show conf"
	SINFO_PARTITIONS        string = "sinfo -h -o \"%R|%a|%D|%g|%G|%I|%N|%T|%E\""
	SCONTROL_SHOW_PARTITION string = "scontrol -o show partition"
	SCONTROL_SHOW_NODES     string = "scontrol show nodes -d -o"
	SHOW_HOSTS              string = "cat /etc/hosts"
	SHOW_LINKS              string = "ip -s link"
	SQUEUE                  string = "squeue -a -r -h -O \"JOBID:|,SubmitTime:|,STARTTIME:|,ENDTIME:|,TIMELIMIT:|,TIMELEFT:|,TIMEUSED:|,STATE:|,REASON:|,USERNAME:|,GroupNAME:|,PRIORITYLONG:|,NODELIST:|,NumCPUs:|,MinMemory:|,ACCOUNT:|,ReasonList:|,MinTmpDisk:|,tres-per-node:|,QOS:|,tres-alloc:|,PARTITION\""
	LSBLK                   string = "lsblk -Pb -o NAME,FSAVAIL,FSSIZE,SIZE,TYPE,PKNAME,MOUNTPOINTS"
	CPU_INFO                string = "lscpu"
	RAM_INFO                string = "free -b"
)

func EXECUTE_COMMAND(comm string) []byte {
	cmd := exec.Command("/bin/bash", "-c", comm)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing %s command: %v, stderr: %s", comm, err, exitErr.Stderr)
		} else {
			log.Printf("Error executing %s command: %v", comm, err)
		}
		return []byte("")
	}
	return out
}

func ShowPids() ([]byte, error) {
	cmd := exec.Command("scontrol", "listpids")
	out, err := cmd.Output()
	return out, err
}

func RemoveDuplicates(s []string) []string {
	m := make(map[string]struct{})
	t := []string{}

	for _, v := range s {
		if _, seen := m[v]; !seen && v != "" {
			t = append(t, v)
			m[v] = struct{}{}
		}
	}

	return t
}
