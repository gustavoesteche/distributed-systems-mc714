package main

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// runRecord pairs one replication's configuration with its metrics so the
// CSV writers and the aggregation step share the same data.
type runRecord struct {
	Run    int
	Seed   int64
	Config SimulationConfig
	Result SimulationResult
}

type aggregateKey struct {
	Strategy Strategy
	Lambda   float64
}

func main() {
	log.SetFlags(0)

	strategyFlag := flag.String("strategy", "all", "random, round-robin, shortest-queue, weighted-shortest-queue, or all")
	lambdaFlag := flag.String("lambdas", "0.6,1.2,1.8,2.4,2.7,3.3", "comma-separated arrival rates")
	mu := flag.Float64("mu", 1, "service rate used by all servers")
	mus := flag.String("mus", "", "optional service rates mu1,mu2,mu3; overrides -mu")
	weightedShortestQueue := flag.Bool("weighted-shortest-queue", false, "also simulate shortest-queue weighted by 1/mu; faster servers keep receiving work even when fuller")
	duration := flag.Float64("duration", 5000, "simulated duration of each run")
	warmup := flag.Float64("warmup", 500, "simulated warm-up period to discard")
	runs := flag.Int("runs", 10, "independent replications per configuration")
	seed := flag.Int64("seed", 0, "base random seed; 0 draws a fresh seed each execution")
	outputDir := flag.String("output", "out", "directory for CSV results")
	name := flag.String("name", "", "name of the results folder; overrides -output")
	traceLambda := flag.Float64("trace-lambda", 3.3, "lambda for the queue-size trace")
	traceInterval := flag.Float64("trace-interval", 1, "simulated time between queue trace samples; zero disables it")
	flag.Parse()

	if err := execute(executionConfig{
		strategy:              *strategyFlag,
		lambdas:               *lambdaFlag,
		mu:                    *mu,
		mus:                   *mus,
		weightedShortestQueue: *weightedShortestQueue,
		duration:              *duration,
		warmup:                *warmup,
		runs:                  *runs,
		seed:                  *seed,
		outputDir:             *outputDir,
		name:                  *name,
		traceLambda:           *traceLambda,
		traceInterval:         *traceInterval,
	}); err != nil {
		log.Fatal(err)
	}
}

type executionConfig struct {
	strategy              string
	lambdas               string
	mu                    float64
	mus                   string
	weightedShortestQueue bool
	duration              float64
	warmup                float64
	runs                  int
	seed                  int64
	outputDir             string
	name                  string
	traceLambda           float64
	traceInterval         float64
}

// execute runs every (lambda, strategy) configuration for the requested
// number of replications and writes the per-run, summary, and trace CSVs.
func execute(cfg executionConfig) error {
	strategies, err := parseStrategies(cfg.strategy, cfg.weightedShortestQueue)
	if err != nil {
		return err
	}
	lambdas, err := parseLambdas(cfg.lambdas)
	if err != nil {
		return err
	}
	serviceRates, err := parseServiceRates(cfg.mu, cfg.mus)
	if err != nil {
		return err
	}
	if cfg.runs <= 0 {
		return errors.New("runs must be greater than zero")
	}
	if cfg.traceInterval < 0 {
		return errors.New("trace interval cannot be negative")
	}
	outputDir, err := resolveOutputDir(cfg.outputDir, cfg.name)
	if err != nil {
		return err
	}
	cfg.outputDir = outputDir
	// Seed 0 means "not pinned": draw a fresh seed so consecutive executions
	// produce different samples. The seed is logged (and stored per run in
	// runs.csv) so any execution can still be reproduced with -seed.
	if cfg.seed == 0 {
		cfg.seed = freshSeed()
		log.Printf("base seed: %d (rerun with -seed %d to reproduce)", cfg.seed, cfg.seed)
	}
	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	runsFile, runsWriter, err := createCSV(
		filepath.Join(cfg.outputDir, "runs.csv"),
		[]string{
			"strategy", "lambda", "mu", "run", "seed", "arrivals", "completions",
			"assigned_1", "assigned_2", "assigned_3", "X", "ER", "EN", "ETq",
			"U1", "U2", "U3", "N_warmup", "N_final", "queue_growth_rate",
			"little_law_gap_abs",
		},
	)
	if err != nil {
		return err
	}
	defer runsFile.Close()

	traceFile, traceWriter, err := createCSV(
		filepath.Join(cfg.outputDir, "queue_trace.csv"),
		[]string{
			"strategy", "lambda", "mu", "run", "seed", "time",
			"N1", "N2", "N3", "Q1", "Q2", "Q3", "N",
		},
	)
	if err != nil {
		return err
	}
	defer traceFile.Close()

	records := make(map[aggregateKey][]runRecord)
	hasTrace := false
	for lambdaIndex, lambda := range lambdas {
		for _, strategy := range strategies {
			key := aggregateKey{Strategy: strategy, Lambda: lambda}
			for run := 1; run <= cfg.runs; run++ {
				experimentSeed := deriveRunSeed(cfg.seed, lambdaIndex, run)
				interval := 0.0
				if cfg.traceInterval > 0 && nearlyEqual(lambda, cfg.traceLambda) {
					interval = cfg.traceInterval
				}
				simulationConfig := SimulationConfig{
					Lambda:        lambda,
					ServiceRates:  serviceRates,
					Duration:      cfg.duration,
					Warmup:        cfg.warmup,
					Strategy:      strategy,
					Seed:          experimentSeed,
					TraceInterval: interval,
				}

				result, trace, runErr := RunSimulation(simulationConfig)
				if runErr != nil {
					return fmt.Errorf("simulate strategy=%s lambda=%g run=%d: %w", strategy, lambda, run, runErr)
				}
				record := runRecord{
					Run:    run,
					Seed:   experimentSeed,
					Config: simulationConfig,
					Result: result,
				}
				records[key] = append(records[key], record)
				if len(trace) > 0 {
					hasTrace = true
				}

				if err := writeRun(runsWriter, record); err != nil {
					return err
				}
				if err := writeTrace(traceWriter, record, trace); err != nil {
					return err
				}
			}
		}
	}

	runsWriter.Flush()
	if err := runsWriter.Error(); err != nil {
		return fmt.Errorf("write runs CSV: %w", err)
	}
	traceWriter.Flush()
	if err := traceWriter.Error(); err != nil {
		return fmt.Errorf("write trace CSV: %w", err)
	}

	if err := writeSummaries(cfg.outputDir, strategies, lambdas, records); err != nil {
		return err
	}

	log.Printf("simulated %d configurations (%d runs each) without wall-clock delays", len(strategies)*len(lambdas), cfg.runs)
	log.Printf("results: %s", filepath.Join(cfg.outputDir, "runs.csv"))
	log.Printf("metric summaries: %s", filepath.Join(cfg.outputDir, "results_<strategy>.csv"))
	if hasTrace {
		log.Printf("queue trace for lambda=%g: %s", cfg.traceLambda, filepath.Join(cfg.outputDir, "queue_trace.csv"))
	}
	return nil
}

func parseStrategies(value string, includeWeighted bool) ([]Strategy, error) {
	var strategies []Strategy
	if value == "all" {
		strategies = []Strategy{StrategyRandom, StrategyRoundRobin, StrategyShortestQueue}
	} else {
		strategy := Strategy(value)
		if !validStrategy(strategy) {
			return nil, fmt.Errorf("invalid strategy %q", value)
		}
		strategies = []Strategy{strategy}
	}
	if includeWeighted && !containsStrategy(strategies, StrategyWeightedShortestQueue) {
		strategies = append(strategies, StrategyWeightedShortestQueue)
	}
	return strategies, nil
}

func validStrategy(strategy Strategy) bool {
	switch strategy {
	case StrategyRandom, StrategyRoundRobin, StrategyShortestQueue, StrategyWeightedShortestQueue:
		return true
	default:
		return false
	}
}

func containsStrategy(strategies []Strategy, target Strategy) bool {
	for _, strategy := range strategies {
		if strategy == target {
			return true
		}
	}
	return false
}

func resolveOutputDir(output, name string) (string, error) {
	resolved := strings.TrimSpace(output)
	if trimmedName := strings.TrimSpace(name); trimmedName != "" {
		resolved = trimmedName
	}
	if resolved == "" {
		return "", errors.New("output directory cannot be empty")
	}
	return resolved, nil
}

func parseLambdas(value string) ([]float64, error) {
	parts := strings.Split(value, ",")
	lambdas := make([]float64, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			return nil, errors.New("lambda list contains an empty value")
		}
		lambda, err := strconv.ParseFloat(trimmed, 64)
		if err != nil || lambda <= 0 {
			return nil, fmt.Errorf("invalid lambda %q", trimmed)
		}
		lambdas = append(lambdas, lambda)
	}
	if len(lambdas) == 0 {
		return nil, errors.New("at least one lambda is required")
	}
	return lambdas, nil
}

func parseServiceRates(defaultRate float64, value string) ([serverCount]float64, error) {
	if value == "" {
		if defaultRate <= 0 {
			return [serverCount]float64{}, errors.New("mu must be greater than zero")
		}
		return [serverCount]float64{defaultRate, defaultRate, defaultRate}, nil
	}

	parts := strings.Split(value, ",")
	if len(parts) != serverCount {
		return [serverCount]float64{}, fmt.Errorf("mus must contain exactly %d values", serverCount)
	}

	var rates [serverCount]float64
	for i, part := range parts {
		rate, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || rate <= 0 {
			return [serverCount]float64{}, fmt.Errorf("invalid mu%d %q", i+1, part)
		}
		rates[i] = rate
	}
	return rates, nil
}

func createCSV(path string, header []string) (*os.File, *csv.Writer, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("create %s: %w", path, err)
	}
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("write header to %s: %w", path, err)
	}
	return file, writer, nil
}

func writeRun(writer *csv.Writer, record runRecord) error {
	result := record.Result
	row := []string{
		string(record.Config.Strategy),
		formatFloat(record.Config.Lambda),
		formatServiceRates(record.Config.ServiceRates),
		strconv.Itoa(record.Run),
		strconv.FormatInt(record.Seed, 10),
		strconv.Itoa(result.Arrivals),
		strconv.Itoa(result.Completions),
		strconv.Itoa(result.Assigned[0]),
		strconv.Itoa(result.Assigned[1]),
		strconv.Itoa(result.Assigned[2]),
		formatFloat(result.Throughput),
		formatFloat(result.MeanResponse),
		formatFloat(result.MeanNumber),
		formatFloat(result.MeanWaiting),
		formatFloat(result.Utilization[0]),
		formatFloat(result.Utilization[1]),
		formatFloat(result.Utilization[2]),
		strconv.Itoa(result.WarmupNumber),
		strconv.Itoa(result.FinalNumber),
		formatFloat(result.QueueGrowthRate),
		formatFloat(littleLawGap(result)),
	}
	if err := writer.Write(row); err != nil {
		return fmt.Errorf("write run result: %w", err)
	}
	return nil
}

// littleLawGap measures |E[N] - X*E[R]|, which should be close to zero when
// Little's Law holds for the run.
func littleLawGap(result SimulationResult) float64 {
	return math.Abs(result.MeanNumber - result.Throughput*result.MeanResponse)
}

func writeTrace(writer *csv.Writer, record runRecord, trace []TracePoint) error {
	for _, point := range trace {
		n := point.Number
		row := []string{
			string(record.Config.Strategy),
			formatFloat(record.Config.Lambda),
			formatServiceRates(record.Config.ServiceRates),
			strconv.Itoa(record.Run),
			strconv.FormatInt(record.Seed, 10),
			formatFloat(point.Time),
			strconv.Itoa(n[0]),
			strconv.Itoa(n[1]),
			strconv.Itoa(n[2]),
			strconv.Itoa(queueLength(n[0])),
			strconv.Itoa(queueLength(n[1])),
			strconv.Itoa(queueLength(n[2])),
			strconv.Itoa(totalNumber(n)),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write queue trace: %w", err)
		}
	}
	return nil
}

// queueLength converts number-in-system into waiting requests only: one
// request is in service whenever the server is non-empty.
func queueLength(number int) int {
	if number == 0 {
		return 0
	}
	return number - 1
}

// writeSummaries writes one results_<strategy>.csv per policy with the mean
// of each metric across replications. The 95% confidence intervals are
// computed from runs.csv by confidence.py.
func writeSummaries(
	outputDir string,
	strategies []Strategy,
	lambdas []float64,
	records map[aggregateKey][]runRecord,
) error {
	oldSummary := filepath.Join(outputDir, "summary.csv")
	if err := os.Remove(oldSummary); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old summary: %w", err)
	}

	for _, strategy := range strategies {
		filename := "results_" + strings.ReplaceAll(string(strategy), "-", "_") + ".csv"
		file, writer, err := createCSV(
			filepath.Join(outputDir, filename),
			[]string{"X", "ER", "EN", "ETq", "U1", "U2", "U3", "lambda", "mu"},
		)
		if err != nil {
			return err
		}

		for _, lambda := range lambdas {
			group := records[aggregateKey{Strategy: strategy, Lambda: lambda}]
			xMean := resultMean(group, func(r SimulationResult) float64 { return r.Throughput })
			erMean := resultMean(group, func(r SimulationResult) float64 { return r.MeanResponse })
			enMean := resultMean(group, func(r SimulationResult) float64 { return r.MeanNumber })
			etqMean := resultMean(group, func(r SimulationResult) float64 { return r.MeanWaiting })
			u1Mean := resultMean(group, func(r SimulationResult) float64 { return r.Utilization[0] })
			u2Mean := resultMean(group, func(r SimulationResult) float64 { return r.Utilization[1] })
			u3Mean := resultMean(group, func(r SimulationResult) float64 { return r.Utilization[2] })

			row := []string{
				formatFloat(xMean),
				formatFloat(erMean),
				formatFloat(enMean),
				formatFloat(etqMean),
				formatFloat(u1Mean),
				formatFloat(u2Mean),
				formatFloat(u3Mean),
				formatFloat(lambda),
				formatServiceRates(group[0].Config.ServiceRates),
			}
			if err := writer.Write(row); err != nil {
				_ = file.Close()
				return fmt.Errorf("write %s: %w", filename, err)
			}
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			_ = file.Close()
			return fmt.Errorf("write %s: %w", filename, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close %s: %w", filename, err)
		}
	}
	return nil
}

func resultMean(records []runRecord, selectMetric func(SimulationResult) float64) float64 {
	total := 0.0
	count := 0
	for _, record := range records {
		value := selectMetric(record.Result)
		if !math.IsNaN(value) {
			total += value
			count++
		}
	}
	if count == 0 {
		return math.NaN()
	}
	return total / float64(count)
}

// freshSeed returns a non-zero seed from the operating system's entropy pool.
func freshSeed() int64 {
	var buffer [8]byte
	if _, err := cryptorand.Read(buffer[:]); err != nil {
		log.Fatalf("read random seed: %v", err)
	}
	seed := int64(binary.LittleEndian.Uint64(buffer[:]))
	if seed == 0 {
		seed = 714
	}
	return seed
}

// deriveRunSeed mixes the base seed with the configuration indices so every
// replication gets a distinct but reproducible seed.
func deriveRunSeed(base int64, lambdaIndex, run int) int64 {
	value := uint64(base) + uint64(lambdaIndex+1)*0x9e3779b97f4a7c15 + uint64(run)*0xbf58476d1ce4e5b9
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return int64(value)
}

func nearlyEqual(left, right float64) bool {
	scale := math.Max(1, math.Max(math.Abs(left), math.Abs(right)))
	return math.Abs(left-right) <= 1e-12*scale
}

func formatFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ""
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func formatServiceRates(rates [serverCount]float64) string {
	if rates[0] == rates[1] && rates[1] == rates[2] {
		return formatFloat(rates[0])
	}
	values := make([]string, serverCount)
	for i, rate := range rates {
		values[i] = formatFloat(rate)
	}
	return strings.Join(values, ";")
}
