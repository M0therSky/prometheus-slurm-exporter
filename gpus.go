/* Copyright 2020 Joeri Hermans, Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"io/ioutil"
	"os/exec"
	"strings"
	"strconv"
)

type GPUsMetrics struct {
	alloc       float64
	idle        float64
	total       float64
	utilization float64
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() float64 {
	var num_gpus float64

	args := []string{"-a", "-X", "--format=AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	output := Execute("sacct", args) // Ensure Execute returns a []byte
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.Trim(line, "\"")
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "gres/gpu=") {
				descriptor := strings.TrimPrefix(part, "gres/gpu=")
				job_gpus, err := strconv.ParseFloat(descriptor, 64)
				if err == nil {
					num_gpus += job_gpus
				}
			}
		}
	}

	return num_gpus
}

func ParseTotalGPUs() float64 {
	var num_gpus float64

	args := []string{"-h", "-o", "%n %G"} // Corrected arguments
	output := Execute("sinfo", args)      // Ensure Execute returns []byte

	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue // Skip lines without enough data
		}
		gpuField := fields[1] // Second field is the GPU info (e.g., "gpu:a100:4")
		if !strings.HasPrefix(gpuField, "gpu:") {
			continue // Skip lines without GPU data
		}
		// Split into parts: ["gpu", "a100", "4"]
		parts := strings.Split(gpuField, ":")
		if len(parts) < 3 {
			continue // Malformed GPU field
		}
		countStr := parts[2] // Third part is the count
		// Parse the count (e.g., "4" -> 4.0)
		count, err := strconv.ParseFloat(countStr, 64)
		if err != nil {
			continue // Invalid number, skip
		}
		num_gpus += count
	}

	return num_gpus
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	total_gpus := ParseTotalGPUs()
	allocated_gpus := ParseAllocatedGPUs()
	gm.alloc = allocated_gpus
	gm.idle = total_gpus - allocated_gpus
	gm.total = total_gpus
	gm.utilization = allocated_gpus / total_gpus
	return &gm
}

// Execute the sinfo command and return its output
func Execute(command string, arguments []string) []byte {
	cmd := exec.Command(command, arguments...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc: prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total: prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
}

// Send all metric descriptions
func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
}
func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
}
