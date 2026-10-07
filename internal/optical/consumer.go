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
// WaitForSample blocks until the monitor receives a new sample for runID
// (one added after this call started), or until timeout elapses. A previous
// run's latest sample — or any sample already present at wait time — can
// never satisfy the wait; the caller only receives a new sample of the
// target run.
func (c *Consumer) WaitForSample(runID model.ID, timeout time.Duration) (model.ThicknessSample, bool) {
	if runID == "" {
		return model.ThicknessSample{}, false
	}
	return c.monitor.WaitForRun(runID, timeout)
}
