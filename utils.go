package main

import (
	"log"
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
		} else {
			log.Printf("Error executing hostname command: %v", err)
		}
		return []byte("")
	}
	return out
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
