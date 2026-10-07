package optical

import (
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

func newSample(runID model.ID, nm float64) model.ThicknessSample {
	return model.ThicknessSample{RunID: runID, Nanometers: nm, Valid: true, At: time.Now().UTC()}
}

func newRunningConsumer(t *testing.T) (*Consumer, *Monitor, chan model.ThicknessSample, chan struct{}) {
	t.Helper()
	monitor := NewMonitor()
	samples := make(chan model.ThicknessSample, 8)
	consumer := NewConsumer(monitor, samples)
	stop := make(chan struct{})
	go consumer.Run(stop)
	return consumer, monitor, samples, stop
}

// A previous run's latest sample must not satisfy a wait for a new run:
// with no new sample for the target run the call must block until timeout.
func TestWaitForSampleIgnoresPreviousRun(t *testing.T) {
	consumer, monitor, _, stop := newRunningConsumer(t)
	defer close(stop)

	previous := model.ID("run-previous")
	monitor.Add(newSample(previous, 12.5))

	_, ok := consumer.WaitForSample(model.ID("run-new"), 30*time.Millisecond)
	if ok {
		t.Fatal("WaitForSample returned a stale sample from the previous run")
	}
}

// A sample already present for the target run at wait time must not satisfy
// the wait either: callers only ever receive a new sample.
func TestWaitForSampleIgnoresAlreadyPresentTargetSample(t *testing.T) {
	consumer, monitor, _, stop := newRunningConsumer(t)
	defer close(stop)

	runID := model.ID("run-1")
	monitor.Add(newSample(runID, 1.0))

	_, ok := consumer.WaitForSample(runID, 30*time.Millisecond)
	if ok {
		t.Fatal("WaitForSample returned a sample that predated the call")
	}
}

// A new sample for the target run (delivered through the consumer channel)
// must be returned, and it must be the new sample, not the old one.
func TestWaitForSampleReturnsNewTargetSample(t *testing.T) {
	consumer, monitor, samples, stop := newRunningConsumer(t)
	defer close(stop)

	previous := model.ID("run-previous")
	target := model.ID("run-target")
	monitor.Add(newSample(previous, 12.5))

	go func() {
		time.Sleep(10 * time.Millisecond)
		samples <- newSample(target, 42.0)
	}()

	sample, ok := consumer.WaitForSample(target, time.Second)
	if !ok {
		t.Fatal("WaitForSample timed out waiting for the target run sample")
	}
	if sample.RunID != target {
		t.Fatalf("returned sample belongs to %q, want %q", sample.RunID, target)
	}
	if sample.Nanometers != 42.0 {
		t.Fatalf("returned thickness %.1f, want 42.0 (stale sample returned?)", sample.Nanometers)
	}
}

// Samples from other runs arriving while waiting must neither satisfy the
// wait nor cause the subsequent target sample to be missed.
func TestWaitForSampleToleratesInterleavedRuns(t *testing.T) {
	consumer, monitor, _, stop := newRunningConsumer(t)
	defer close(stop)

	target := model.ID("run-target")
	monitor.Add(newSample(model.ID("run-previous"), 1.0))

	go func() {
		time.Sleep(5 * time.Millisecond)
		monitor.Add(newSample(model.ID("run-interloper-a"), 2.0))
		time.Sleep(5 * time.Millisecond)
		monitor.Add(newSample(target, 7.0))
		// A different run lands after the target sample; the waiter must
		// still observe the target run's latest state.
		time.Sleep(5 * time.Millisecond)
		monitor.Add(newSample(model.ID("run-interloper-b"), 3.0))
	}()

	sample, ok := consumer.WaitForSample(target, time.Second)
	if !ok {
		t.Fatal("WaitForSample timed out with interleaved runs")
	}
	if sample.RunID != target || sample.Nanometers != 7.0 {
		t.Fatalf("got run=%q nm=%.1f, want %q 7.0", sample.RunID, sample.Nanometers, target)
	}
}

// Two sequential waits for the same run must each observe distinct new
// samples, never the same cached latest twice.
func TestWaitForSampleSequentialRuns(t *testing.T) {
	consumer, monitor, _, stop := newRunningConsumer(t)
	defer close(stop)

	runA, runB := model.ID("run-a"), model.ID("run-b")
	monitor.Add(newSample(runA, 1.0))

	go func() {
		time.Sleep(5 * time.Millisecond)
		monitor.Add(newSample(runB, 2.0))
	}()
	first, ok := consumer.WaitForSample(runB, time.Second)
	if !ok || first.Nanometers != 2.0 {
		t.Fatalf("first wait: ok=%v sample=%+v", ok, first)
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		monitor.Add(newSample(runB, 3.0))
	}()
	second, ok := consumer.WaitForSample(runB, time.Second)
	if !ok {
		t.Fatal("second wait timed out")
	}
	if second.Nanometers != 3.0 {
		t.Fatalf("second wait returned cached thickness %.1f, want 3.0", second.Nanometers)
	}
}

func TestWaitForSampleRejectsEmptyRunID(t *testing.T) {
	consumer, _, _, stop := newRunningConsumer(t)
	defer close(stop)

	if _, ok := consumer.WaitForSample("", 10*time.Millisecond); ok {
		t.Fatal("empty run ID must not be accepted")
	}
}
