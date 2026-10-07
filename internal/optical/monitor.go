package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"time"
)

type Monitor struct {
	mu          sync.RWMutex
	samples     []model.ThicknessSample
	latest      model.ThicknessSample
	version     uint64
	runVersion  map[model.ID]uint64
	runLatest   map[model.ID]model.ThicknessSample
	waiters     map[chan struct{}]struct{}
}

func NewMonitor() *Monitor {
	return &Monitor{
		samples:    make([]model.ThicknessSample, 0, 64),
		runVersion: map[model.ID]uint64{},
		runLatest:  map[model.ID]model.ThicknessSample{},
		waiters:    map[chan struct{}]struct{}{},
	}
}

func (m *Monitor) Add(sample model.ThicknessSample) {
	m.mu.Lock()
	m.samples = append(m.samples, sample)
	m.latest = sample
	m.version++
	if sample.RunID != "" {
		m.runVersion[sample.RunID]++
		m.runLatest[sample.RunID] = sample
	}
	waiters := make([]chan struct{}, 0, len(m.waiters))
	for ch := range m.waiters {
		waiters = append(waiters, ch)
	}
	m.mu.Unlock()
	// Wake blocked waiters; a full/busy channel is dropped because every
	// waiter re-checks its own condition after waking, so a missed ping only
	// costs one more wait cycle until the timeout deadline.
	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (m *Monitor) Latest() model.ThicknessSample { m.mu.RLock(); defer m.mu.RUnlock(); return m.latest }

func (m *Monitor) Samples() []model.ThicknessSample {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.ThicknessSample, len(m.samples))
	copy(out, m.samples)
	return out
}

// WaitForRun blocks until a sample belonging to runID that was added after
// the call started arrives, or until timeout elapses. A previous run's latest
// sample, or any sample already present at wait time, can never satisfy the
// wait; the caller only receives a new sample of the target run. The timeout
// is a single overall deadline covering every wake-up, not a per-poll delay.
func (m *Monitor) WaitForRun(runID model.ID, timeout time.Duration) (model.ThicknessSample, bool) {
	if runID == "" {
		return model.ThicknessSample{}, false
	}
	notify := make(chan struct{}, 1)

	m.mu.Lock()
	baseline := m.runVersion[runID]
	// Registration and version read are atomic, so no Add can fall between
	// the baseline read and the subscription.
	m.waiters[notify] = struct{}{}
	m.mu.Unlock()
	defer m.unsubscribe(notify)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-notify:
			m.mu.RLock()
			current := m.runVersion[runID]
			sample := m.runLatest[runID]
			m.mu.RUnlock()
			if current > baseline {
				return sample, true
			}
		case <-deadline.C:
			return model.ThicknessSample{}, false
		}
	}
}

func (m *Monitor) unsubscribe(ch chan struct{}) {
	m.mu.Lock()
	delete(m.waiters, ch)
	m.mu.Unlock()
}

func (m *Monitor) Estimate(freq, transmission float64) model.ThicknessSample {
	return model.ThicknessSample{Frequency: freq, Transmission: transmission, Nanometers: freq*0.42 + (1-transmission)*10, Valid: freq > 0 && transmission >= 0, At: time.Now().UTC()}
}
