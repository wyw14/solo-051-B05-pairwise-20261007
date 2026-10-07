package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"time"
)

type Consumer struct {
	mu         sync.Mutex
	samples    chan model.ThicknessSample
	monitor    *Monitor
	reconnects int
}

func NewConsumer(monitor *Monitor, samples chan model.ThicknessSample) *Consumer {
	return &Consumer{monitor: monitor, samples: samples}
}
func (c *Consumer) Run(stop <-chan struct{}) {
	for {
		c.mu.Lock()
		samples := c.samples
		c.mu.Unlock()
		if samples == nil {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				continue
			}
		}
		select {
		case sample, ok := <-samples:
			if !ok {
				c.mu.Lock()
				if c.samples == samples {
					c.samples = nil
				}
				c.mu.Unlock()
				continue
			}
			c.monitor.Add(sample)
		case <-stop:
			return
		}
	}
}
func (c *Consumer) Reconnect() chan model.ThicknessSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	samples := make(chan model.ThicknessSample, 8)
	c.samples = samples
	c.reconnects++
	return samples
}
func (c *Consumer) ReconnectCount() int { c.mu.Lock(); defer c.mu.Unlock(); return c.reconnects }
// WaitForSample blocks until a sample for runID is recorded after this call
// began, or until timeout elapses. Samples already buffered from earlier (or
// interleaved) runs are ignored, so the caller only ever receives a fresh
// sample of the target run. timeout bounds the total wait; an empty runID or a
// non-positive timeout never returns a stale sample.
func (c *Consumer) WaitForSample(runID model.ID, timeout time.Duration) (model.ThicknessSample, bool) {
	if runID == "" || timeout <= 0 {
		return model.ThicknessSample{}, false
	}
	// Only samples recorded after this baseline are "new".
	_, baseline, _ := c.monitor.LatestFor(runID)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if sample, seq, ok := c.monitor.LatestFor(runID); ok && seq > baseline {
			return sample, true
		}
		select {
		case <-deadline.C:
			return model.ThicknessSample{}, false
		case <-ticker.C:
		}
	}
}
