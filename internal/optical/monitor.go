package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"time"
)

type recordedSample struct {
	model.ThicknessSample
	seq int64
}

type Monitor struct {
	mu      sync.RWMutex
	samples []recordedSample
	latest  recordedSample
	seq     int64
}

func NewMonitor() *Monitor { return &Monitor{samples: make([]recordedSample, 0, 64)} }
func (m *Monitor) Add(sample model.ThicknessSample) {
	m.mu.Lock()
	m.seq++
	record := recordedSample{ThicknessSample: sample, seq: m.seq}
	m.samples = append(m.samples, record)
	m.latest = record
	m.mu.Unlock()
}
func (m *Monitor) Latest() model.ThicknessSample { m.mu.RLock(); defer m.mu.RUnlock(); return m.latest.ThicknessSample }

// LatestFor returns the most recently recorded sample belonging to runID and
// its 1-based recording sequence. ok is false until a sample for that run has
// been recorded; samples left over from earlier runs are never returned.
func (m *Monitor) LatestFor(runID model.ID) (model.ThicknessSample, int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for i := len(m.samples) - 1; i >= 0; i-- {
		record := m.samples[i]
		if record.RunID == runID {
			return record.ThicknessSample, record.seq, true
		}
	}
	return model.ThicknessSample{}, 0, false
}

func (m *Monitor) Samples() []model.ThicknessSample {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]model.ThicknessSample, len(m.samples))
	for i, record := range m.samples {
		out[i] = record.ThicknessSample
	}
	return out
}
func (m *Monitor) Estimate(freq, transmission float64) model.ThicknessSample {
	return model.ThicknessSample{Frequency: freq, Transmission: transmission, Nanometers: freq*0.42 + (1-transmission)*10, Valid: freq > 0 && transmission >= 0, At: time.Now().UTC()}
}
