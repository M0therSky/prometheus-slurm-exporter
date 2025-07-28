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
	userAlloc   map[string]float64
}

func GPUsGetMetrics() *GPUsMetrics {
	return ParseGPUsMetrics()
}

func ParseAllocatedGPUs() (float64, map[string]float64) {
	var totalGpus float64
	userGpus := make(map[string]float64)

	args := []string{"-a", "-X", "--format=User,AllocTRES", "--state=RUNNING", "--noheader", "--parsable2"}
	output := Execute("sacct", args)
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.Trim(line, "\"")
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) < 2 {
			continue
		}
		user := strings.TrimSpace(parts[0])
		tres := strings.TrimSpace(parts[1])
		if user == "" || tres == "" {
			continue
		}
		for _, part := range strings.Split(tres, ",") {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(part, "gres/gpu=") {
				descriptor := strings.TrimPrefix(part, "gres/gpu=")
				jobGpus, err := strconv.ParseFloat(descriptor, 64)
				if err == nil {
					userGpus[user] += jobGpus
					totalGpus += jobGpus
				}
			}
		}
	}

	return totalGpus, userGpus
}

func ParseTotalGPUs() float64 {
	var numGpus float64

	args := []string{"-h", "-o", "%n %G"}
	output := Execute("sinfo", args)

	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		gpuField := fields[1]
		if !strings.HasPrefix(gpuField, "gpu:") {
			continue
		}
		parts := strings.Split(gpuField, ":")
		if len(parts) < 3 {
			continue
		}
		countStr := parts[2]
		count, err := strconv.ParseFloat(countStr, 64)
		if err != nil {
			continue
		}
		numGpus += count
	}

	return numGpus
}

func ParseGPUsMetrics() *GPUsMetrics {
	var gm GPUsMetrics
	totalGpus := ParseTotalGPUs()
	allocatedGpus, userAlloc := ParseAllocatedGPUs()
	gm.alloc = allocatedGpus
	gm.idle = totalGpus - allocatedGpus
	gm.total = totalGpus
	if totalGpus > 0 {
		gm.utilization = allocatedGpus / totalGpus
	} else {
		gm.utilization = 0
	}
	gm.userAlloc = userAlloc
	return &gm
}

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

func NewGPUsCollector() *GPUsCollector {
	return &GPUsCollector{
		alloc:       prometheus.NewDesc("slurm_gpus_alloc", "Allocated GPUs", nil, nil),
		idle:        prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		total:       prometheus.NewDesc("slurm_gpus_total", "Total GPUs", nil, nil),
		utilization: prometheus.NewDesc("slurm_gpus_utilization", "Total GPU utilization", nil, nil),
		userAlloc:   prometheus.NewDesc("slurm_user_gpus_running", "GPUs allocated per user for running jobs", []string{"user"}, nil),
	}
}

type GPUsCollector struct {
	alloc       *prometheus.Desc
	idle        *prometheus.Desc
	total       *prometheus.Desc
	utilization *prometheus.Desc
	userAlloc   *prometheus.Desc
}

func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.alloc
	ch <- cc.idle
	ch <- cc.total
	ch <- cc.utilization
	ch <- cc.userAlloc
}

func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
	cm := GPUsGetMetrics()
	ch <- prometheus.MustNewConstMetric(cc.alloc, prometheus.GaugeValue, cm.alloc)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.total, prometheus.GaugeValue, cm.total)
	ch <- prometheus.MustNewConstMetric(cc.utilization, prometheus.GaugeValue, cm.utilization)
	for user, alloc := range cm.userAlloc {
		ch <- prometheus.MustNewConstMetric(cc.userAlloc, prometheus.GaugeValue, alloc, user)
	}
}
