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

	if _, statErr := os.Stat(filepath.Join(outputDir, "results_weighted_shortest_queue.csv")); !os.IsNotExist(statErr) {
		t.Fatal("weighted shortest-queue CSV should be absent unless the flag is set")
	}
}

func TestWeightedShortestQueueMatchesJSQWhenRatesAreEqual(t *testing.T) {
	base := SimulationConfig{
		Lambda:       1.8,
		ServiceRates: [serverCount]float64{1, 1, 1},
		Duration:     200,
		Warmup:       20,
		Seed:         714,
	}

	jsq := base
	jsq.Strategy = StrategyShortestQueue
	weighted := base
	weighted.Strategy = StrategyWeightedShortestQueue

	jsqResult, _, err := RunSimulation(jsq)
	if err != nil {
		t.Fatal(err)
	}
	weightedResult, _, err := RunSimulation(weighted)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(jsqResult, weightedResult) {
		t.Fatalf("homogeneous results differ:\njsq:      %+v\nweighted: %+v", jsqResult, weightedResult)
	}
}

func TestWeightedShortestQueuePrefersFasterServers(t *testing.T) {
	rates := [serverCount]float64{1.5, 1, 0.5}
	base := SimulationConfig{
		Lambda:       1.8,
		ServiceRates: rates,
		Duration:     20_000,
		Warmup:       2_000,
		Seed:         714,
	}

	jsq := base
	jsq.Strategy = StrategyShortestQueue
	weighted := base
	weighted.Strategy = StrategyWeightedShortestQueue

	jsqResult, _, err := RunSimulation(jsq)
	if err != nil {
		t.Fatal(err)
	}
	weightedResult, _, err := RunSimulation(weighted)
	if err != nil {
		t.Fatal(err)
	}

	if weightedResult.Assigned[0] <= jsqResult.Assigned[0] {
		t.Fatalf("fast server should receive more work under 1/mu weighting: weighted=%d jsq=%d", weightedResult.Assigned[0], jsqResult.Assigned[0])
	}
	if weightedResult.Assigned[2] >= jsqResult.Assigned[2] {
		t.Fatalf("slow server should receive less work under 1/mu weighting: weighted=%d jsq=%d", weightedResult.Assigned[2], jsqResult.Assigned[2])
	}
	if !(weightedResult.Assigned[0] > weightedResult.Assigned[1] && weightedResult.Assigned[1] > weightedResult.Assigned[2]) {
		t.Fatalf("assignments should follow service rates: %v", weightedResult.Assigned)
	}
}

func TestParseStrategiesAddsWeightedWhenFlagSet(t *testing.T) {
	got, err := parseStrategies("all", true)
	if err != nil {
		t.Fatal(err)
	}
	want := []Strategy{StrategyRandom, StrategyRoundRobin, StrategyShortestQueue, StrategyWeightedShortestQueue}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseStrategies(all, true) = %v, want %v", got, want)
	}

	got, err = parseStrategies("shortest-queue", true)
	if err != nil {
		t.Fatal(err)
	}
	want = []Strategy{StrategyShortestQueue, StrategyWeightedShortestQueue}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseStrategies(shortest-queue, true) = %v, want %v", got, want)
	}

	got, err = parseStrategies("weighted-shortest-queue", false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []Strategy{StrategyWeightedShortestQueue}) {
		t.Fatalf("parseStrategies(weighted-shortest-queue, false) = %v", got)
	}
}

func TestResolveOutputDir(t *testing.T) {
	got, err := resolveOutputDir("out", "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "out" {
		t.Fatalf("resolveOutputDir(out, empty) = %q, want out", got)
	}

	got, err = resolveOutputDir("out", "different_mu")
	if err != nil {
		t.Fatal(err)
	}
	if got != "different_mu" {
		t.Fatalf("resolveOutputDir() = %q, want different_mu", got)
	}

	got, err = resolveOutputDir("out", "  named  ")
	if err != nil {
		t.Fatal(err)
	}
	if got != "named" {
		t.Fatalf("resolveOutputDir() = %q, want named", got)
	}

	if _, err = resolveOutputDir("", ""); err == nil {
		t.Fatal("expected empty output directory error")
	}
}

func TestExecuteUsesNameForResultsFolder(t *testing.T) {
	parent := t.TempDir()
	named := filepath.Join(parent, "different_mu")
	err := execute(executionConfig{
		strategy:      "random",
		lambdas:       "1.2",
		mu:            1,
		duration:      20,
		warmup:        2,
		runs:          1,
		seed:          1,
		outputDir:     filepath.Join(parent, "should_not_exist"),
		name:          named,
		traceInterval: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, statErr := os.Stat(filepath.Join(named, "runs.csv")); statErr != nil {
		t.Fatalf("results were not written to named folder: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(parent, "should_not_exist")); !os.IsNotExist(statErr) {
		t.Fatal("-name should override -output")
	}
}

func TestExecuteWritesWeightedShortestQueueCSV(t *testing.T) {
	outputDir := t.TempDir()
	err := execute(executionConfig{
		strategy:              "all",
		lambdas:               "1.2",
		mus:                   "1.5,1,0.5",
		weightedShortestQueue: true,
		duration:              50,
		warmup:                5,
		runs:                  1,
		seed:                  714,
		outputDir:             outputDir,
		traceInterval:         0,
	})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(outputDir, "results_weighted_shortest_queue.csv")
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("weighted shortest-queue CSV was not created: %v", statErr)
	}
}

func assertClose(t *testing.T, name string, actual, expected, tolerance float64) {
	t.Helper()
	if math.Abs(actual-expected) > tolerance {
		t.Fatalf("%s = %g, want %g ± %g", name, actual, expected, tolerance)
	}
}
