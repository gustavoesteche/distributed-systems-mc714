package main

import (
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRoundRobinAssignmentsAreCyclic(t *testing.T) {
	result, _, err := RunSimulation(SimulationConfig{
		Lambda:       20,
		ServiceRates: [serverCount]float64{1, 1, 1},
		Duration:     10,
		Warmup:       1,
		Strategy:     StrategyRoundRobin,
		Seed:         42,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < serverCount; i++ {
		difference := result.Assigned[0] - result.Assigned[i]
		if difference < 0 || difference > 1 {
			t.Fatalf("round-robin assignments are not balanced: %v", result.Assigned)
		}
	}
}

func TestSimulationIsDeterministicForASeed(t *testing.T) {
	config := SimulationConfig{
		Lambda:        1.8,
		ServiceRates:  [serverCount]float64{1, 1, 1},
		Duration:      100,
		Warmup:        10,
		Strategy:      StrategyShortestQueue,
		Seed:          714,
		TraceInterval: 1,
	}

	firstResult, firstTrace, err := RunSimulation(config)
	if err != nil {
		t.Fatal(err)
	}
	secondResult, secondTrace, err := RunSimulation(config)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(firstResult, secondResult) {
		t.Fatalf("results differ for the same seed:\nfirst:  %+v\nsecond: %+v", firstResult, secondResult)
	}
	if !reflect.DeepEqual(firstTrace, secondTrace) {
		t.Fatal("traces differ for the same seed")
	}
}

func TestRandomPolicyConvergesToMM1AnalyticalModel(t *testing.T) {
	const (
		lambda = 1.2
		mu     = 1.0
	)
	result, _, err := RunSimulation(SimulationConfig{
		Lambda:       lambda,
		ServiceRates: [serverCount]float64{mu, mu, mu},
		Duration:     200_000,
		Warmup:       20_000,
		Strategy:     StrategyRandom,
		Seed:         256223,
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedResponse := 1 / (mu - lambda/serverCount)
	expectedNumber := lambda * expectedResponse
	expectedUtilization := lambda / (serverCount * mu)

	assertClose(t, "throughput", result.Throughput, lambda, 0.015)
	assertClose(t, "mean response", result.MeanResponse, expectedResponse, 0.04)
	assertClose(t, "mean number", result.MeanNumber, expectedNumber, 0.06)
	for i, utilization := range result.Utilization {
		assertClose(t, "utilization server "+string(rune('1'+i)), utilization, expectedUtilization, 0.01)
	}
}

func TestServersCanHaveDifferentServiceRates(t *testing.T) {
	const lambda = 0.6
	rates := [serverCount]float64{1.5, 1, 0.5}
	result, _, err := RunSimulation(SimulationConfig{
		Lambda:       lambda,
		ServiceRates: rates,
		Duration:     200_000,
		Warmup:       20_000,
		Strategy:     StrategyRandom,
		Seed:         252615,
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedResponse := 0.0
	for i, rate := range rates {
		arrivalRate := lambda / serverCount
		expectedResponse += 1 / (rate - arrivalRate) / serverCount
		expectedUtilization := arrivalRate / rate
		assertClose(t, "heterogeneous utilization", result.Utilization[i], expectedUtilization, 0.01)
	}
	assertClose(t, "heterogeneous mean response", result.MeanResponse, expectedResponse, 0.05)
}

func TestTraceUsesSimulatedTime(t *testing.T) {
	_, trace, err := RunSimulation(SimulationConfig{
		Lambda:        3.3,
		ServiceRates:  [serverCount]float64{1, 1, 1},
		Duration:      5,
		Warmup:        1,
		Strategy:      StrategyRandom,
		Seed:          1,
		TraceInterval: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(trace) != 6 {
		t.Fatalf("got %d trace points, want 6", len(trace))
	}
	for i, point := range trace {
		if point.Time != float64(i) {
			t.Fatalf("trace[%d].Time = %g, want %d", i, point.Time, i)
		}
	}
}

func TestInvalidConfiguration(t *testing.T) {
	_, _, err := RunSimulation(SimulationConfig{
		Lambda:       1,
		ServiceRates: [serverCount]float64{1, 1, 1},
		Duration:     10,
		Warmup:       10,
		Strategy:     StrategyRandom,
	})
	if err == nil {
		t.Fatal("expected warmup validation error")
	}
}

func TestLittleLawGapIsAbsolute(t *testing.T) {
	result := SimulationResult{
		Throughput:   2,
		MeanResponse: 1,
		MeanNumber:   1.5,
	}
	if gap := littleLawGap(result); gap != 0.5 {
		t.Fatalf("littleLawGap() = %g, want 0.5", gap)
	}
}

func TestParseDifferentServiceRates(t *testing.T) {
	rates, err := parseServiceRates(1, "1.5,1,0.5")
	if err != nil {
		t.Fatal(err)
	}
	expected := [serverCount]float64{1.5, 1, 0.5}
	if rates != expected {
		t.Fatalf("parseServiceRates() = %v, want %v", rates, expected)
	}
	if formatted := formatServiceRates(rates); formatted != "1.5;1;0.5" {
		t.Fatalf("formatServiceRates() = %q", formatted)
	}
}

func TestExecuteWritesAllSimulationCSVs(t *testing.T) {
	outputDir := t.TempDir()
	err := execute(executionConfig{
		strategy:      "all",
		lambdas:       "1.2,3.3",
		mu:            1,
		duration:      50,
		warmup:        5,
		runs:          2,
		seed:          714,
		outputDir:     outputDir,
		traceLambda:   3.3,
		traceInterval: 5,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{
		"runs.csv",
		"queue_trace.csv",
		"results_random.csv",
		"results_round_robin.csv",
		"results_shortest_queue.csv",
	} {
		info, statErr := os.Stat(filepath.Join(outputDir, name))
		if statErr != nil {
			t.Fatalf("%s was not created: %v", name, statErr)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is empty", name)
		}
	}

	expectedHeader := []string{"X", "ER", "EN", "ETq", "U1", "U2", "U3", "lambda", "mu"}
	for _, name := range []string{
		"results_random.csv",
		"results_round_robin.csv",
		"results_shortest_queue.csv",
	} {
		file, openErr := os.Open(filepath.Join(outputDir, name))
		if openErr != nil {
			t.Fatal(openErr)
		}
		header, readErr := csv.NewReader(file).Read()
		_ = file.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !reflect.DeepEqual(header, expectedHeader) {
			t.Fatalf("%s header = %v, want %v", name, header, expectedHeader)
		}
	}
}

func assertClose(t *testing.T, name string, actual, expected, tolerance float64) {
	t.Helper()
	if math.Abs(actual-expected) > tolerance {
		t.Fatalf("%s = %g, want %g ± %g", name, actual, expected, tolerance)
	}
}
