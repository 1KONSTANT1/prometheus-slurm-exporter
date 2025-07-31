package main

import (
	"log"
	"os"
	"os/exec"
)

func ShowPids() ([]byte, error) {
	cmd := exec.Command("scontrol", "listpids")
	out, err := cmd.Output()
	return out, err
}

func GetHostName() []byte {
	cmd := exec.Command("hostname")
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Printf("Error executing hostname command: %v, stderr: %s", err, exitErr.Stderr)
			os.Exit(1)
		} else {
			log.Printf("Error executing hostname command: %v", err)
			os.Exit(1)
		}
	}
	return out
}

func RemoveDuplicates(s []string) []string {
	m := map[string]bool{}
	t := []string{}

	// Walk through the slice 's' and for each value we haven't seen so far, append it to 't'.
	for _, v := range s {
		if _, seen := m[v]; !seen {
			if len(v) > 0 {
				t = append(t, v)
				m[v] = true
			}
		}
	}

	return t
}
