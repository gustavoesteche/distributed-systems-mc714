package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
)

// The simulator advances an event-driven clock over two event types:
// Poisson arrivals routed by the balancer and service completions.
// Time is purely simulated, so a run never waits on the wall clock.

const serverCount = 3

type Strategy string

const (
	StrategyRandom        Strategy = "random"
	StrategyRoundRobin    Strategy = "round-robin"
	StrategyShortestQueue Strategy = "shortest-queue"
)

type SimulationConfig struct {
	Lambda        float64
	ServiceRates  [serverCount]float64
	Duration      float64
	Warmup        float64
	Strategy      Strategy
	Seed          int64
	TraceInterval float64
}

type SimulationResult struct {
	Arrivals        int
	Completions     int
	Assigned        [serverCount]int
	Throughput      float64
	MeanResponse    float64
	MeanWaiting     float64
	MeanNumber      float64
	Utilization     [serverCount]float64
	WarmupNumber    int
	FinalNumber     int
	QueueGrowthRate float64
}

type TracePoint struct {
	Time   float64
	Number [serverCount]int
}

// completion is a scheduled service-completion event. The sequence number
// makes the heap order deterministic when two events share the same time.
type completion struct {
	time        float64
	server      int
	arrivalTime float64
	serviceTime float64
	sequence    uint64
}

type completionHeap []completion

func (h completionHeap) Len() int { return len(h) }

func (h completionHeap) Less(i, j int) bool {
	if h[i].time == h[j].time {
		return h[i].sequence < h[j].sequence
	}
	return h[i].time < h[j].time
}

func (h completionHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *completionHeap) Push(value any) {
	*h = append(*h, value.(completion))
}

func (h *completionHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

type simulator struct {
	cfg SimulationConfig

	// Independent RNG streams keep arrivals, routing, and each server's
	// service times reproducible even when the policy changes.
	arrivalRNG  *rand.Rand
	balancerRNG *rand.Rand
	serviceRNG  [serverCount]*rand.Rand

	now             float64
	nextArrival     float64
	nextTrace       float64
	roundRobinIndex int
	sequence        uint64
	number          [serverCount]int
	availableAt     [serverCount]float64
	completions     completionHeap

	arrivals            int
	measuredCompletions int
	assigned            [serverCount]int
	responseSum         float64
	waitingSum          float64
	numberArea          float64
	busyArea            [serverCount]float64
	warmupNumber        int
	warmupCaptured      bool
	trace               []TracePoint
}

func RunSimulation(cfg SimulationConfig) (SimulationResult, []TracePoint, error) {
	if err := validateConfig(cfg); err != nil {
		return SimulationResult{}, nil, err
	}

	s := newSimulator(cfg)
	s.run()
	return s.result(), s.trace, nil
}

func validateConfig(cfg SimulationConfig) error {
	if cfg.Lambda <= 0 {
		return fmt.Errorf("lambda must be greater than zero")
	}
	for server, rate := range cfg.ServiceRates {
		if rate <= 0 {
			return fmt.Errorf("mu%d must be greater than zero", server+1)
		}
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf("duration must be greater than zero")
	}
	if cfg.Warmup < 0 || cfg.Warmup >= cfg.Duration {
		return fmt.Errorf("warmup must satisfy 0 <= warmup < duration")
	}
	if cfg.TraceInterval < 0 {
		return fmt.Errorf("trace interval cannot be negative")
	}
	switch cfg.Strategy {
	case StrategyRandom, StrategyRoundRobin, StrategyShortestQueue:
		return nil
	default:
		return fmt.Errorf("unsupported strategy %q", cfg.Strategy)
	}
}

func newSimulator(cfg SimulationConfig) *simulator {
	s := &simulator{
		cfg:         cfg,
		arrivalRNG:  newStream(cfg.Seed, 0),
		balancerRNG: newStream(cfg.Seed, 1),
	}
	for i := range serverCount {
		s.serviceRNG[i] = newStream(cfg.Seed, uint64(i+2))
	}
	s.nextArrival = exponential(s.arrivalRNG, cfg.Lambda)
	if cfg.TraceInterval > 0 {
		s.nextTrace = 0
	}
	heap.Init(&s.completions)
	return s
}

// run processes events in chronological order until the configured duration.
func (s *simulator) run() {
	for {
		nextCompletion := math.Inf(1)
		if len(s.completions) > 0 {
			nextCompletion = s.completions[0].time
		}

		nextEvent := math.Min(s.nextArrival, nextCompletion)
		if nextEvent > s.cfg.Duration {
			s.advanceTo(s.cfg.Duration)
			return
		}

		// A completion frees capacity before a simultaneous arrival is routed.
		if nextCompletion <= s.nextArrival {
			s.advanceTo(nextCompletion)
			s.completeRequest(heap.Pop(&s.completions).(completion))
			continue
		}

		s.advanceTo(s.nextArrival)
		s.acceptRequest()
		s.nextArrival += exponential(s.arrivalRNG, s.cfg.Lambda)
	}
}

// advanceTo moves the clock to target while accumulating the time-weighted
// metrics (area under N(t) and busy time) restricted to the measurement
// window after the warm-up, and emits queue-trace samples when enabled.
func (s *simulator) advanceTo(target float64) {
	if target < s.now {
		panic("simulation clock moved backwards")
	}

	if !s.warmupCaptured && s.cfg.Warmup >= s.now && s.cfg.Warmup <= target {
		s.warmupNumber = totalNumber(s.number)
		s.warmupCaptured = true
	}

	measurementStart := math.Max(s.now, s.cfg.Warmup)
	measurementEnd := math.Min(target, s.cfg.Duration)
	if measurementEnd > measurementStart {
		elapsed := measurementEnd - measurementStart
		s.numberArea += float64(totalNumber(s.number)) * elapsed
		for i, n := range s.number {
			if n > 0 {
				s.busyArea[i] += elapsed
			}
		}
	}

	if s.cfg.TraceInterval > 0 {
		const epsilon = 1e-12
		for s.nextTrace <= target+epsilon && s.nextTrace <= s.cfg.Duration+epsilon {
			s.trace = append(s.trace, TracePoint{
				Time:   math.Min(s.nextTrace, s.cfg.Duration),
				Number: s.number,
			})
			s.nextTrace += s.cfg.TraceInterval
		}
	}

	s.now = target
}

// acceptRequest routes an arrival and schedules its completion. Because each
// server is FCFS with a single thread, the service can only start once the
// server finishes everything already assigned to it (availableAt).
func (s *simulator) acceptRequest() {
	server := s.selectServer()
	serviceTime := exponential(s.serviceRNG[server], s.cfg.ServiceRates[server])
	startTime := math.Max(s.now, s.availableAt[server])
	completionTime := startTime + serviceTime

	s.sequence++
	heap.Push(&s.completions, completion{
		time:        completionTime,
		server:      server,
		arrivalTime: s.now,
		serviceTime: serviceTime,
		sequence:    s.sequence,
	})

	s.availableAt[server] = completionTime
	s.number[server]++
	s.assigned[server]++
	s.arrivals++
}

// completeRequest removes a finished request and records its response and
// waiting times, but only for completions inside the measurement window.
func (s *simulator) completeRequest(event completion) {
	s.number[event.server]--
	if s.number[event.server] < 0 {
		panic("negative server population")
	}

	if s.now > s.cfg.Warmup && s.now <= s.cfg.Duration {
		responseTime := s.now - event.arrivalTime
		s.measuredCompletions++
		s.responseSum += responseTime
		s.waitingSum += responseTime - event.serviceTime
	}
}

// selectServer applies the configured balancing policy. Shortest-queue
// compares the number in system (queue + in service) and breaks ties randomly.
func (s *simulator) selectServer() int {
	switch s.cfg.Strategy {
	case StrategyRandom:
		return s.balancerRNG.Intn(serverCount)
	case StrategyRoundRobin:
		selected := s.roundRobinIndex
		s.roundRobinIndex = (s.roundRobinIndex + 1) % serverCount
		return selected
	case StrategyShortestQueue:
		minimum := s.number[0]
		candidates := [serverCount]int{0}
		count := 1
		for i := 1; i < serverCount; i++ {
			switch {
			case s.number[i] < minimum:
				minimum = s.number[i]
				candidates[0] = i
				count = 1
			case s.number[i] == minimum:
				candidates[count] = i
				count++
			}
		}
		return candidates[s.balancerRNG.Intn(count)]
	default:
		panic("validated strategy became invalid")
	}
}

// result converts the accumulated counters into the metrics required by the
// assignment: X, E[R], E[N], E[Tq], and per-server utilization, all measured
// over the post-warm-up window.
func (s *simulator) result() SimulationResult {
	observationTime := s.cfg.Duration - s.cfg.Warmup
	result := SimulationResult{
		Arrivals:        s.arrivals,
		Completions:     s.measuredCompletions,
		Assigned:        s.assigned,
		Throughput:      float64(s.measuredCompletions) / observationTime,
		MeanResponse:    math.NaN(),
		MeanWaiting:     math.NaN(),
		MeanNumber:      s.numberArea / observationTime,
		WarmupNumber:    s.warmupNumber,
		FinalNumber:     totalNumber(s.number),
		QueueGrowthRate: float64(totalNumber(s.number)-s.warmupNumber) / observationTime,
	}
	if s.measuredCompletions > 0 {
		result.MeanResponse = s.responseSum / float64(s.measuredCompletions)
		result.MeanWaiting = s.waitingSum / float64(s.measuredCompletions)
	}
	for i := range serverCount {
		result.Utilization[i] = s.busyArea[i] / observationTime
	}
	return result
}

func totalNumber(number [serverCount]int) int {
	total := 0
	for _, n := range number {
		total += n
	}
	return total
}

// exponential samples an exponential variate via inverse-transform sampling.
func exponential(rng *rand.Rand, rate float64) float64 {
	return -math.Log1p(-rng.Float64()) / rate
}

// newStream derives an independent RNG per role (arrivals, balancer, each
// server) from one seed, using a SplitMix64-style mix to decorrelate them.
func newStream(seed int64, stream uint64) *rand.Rand {
	value := uint64(seed) + 0x9e3779b97f4a7c15*(stream+1)
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return rand.New(rand.NewSource(int64(value)))
}
