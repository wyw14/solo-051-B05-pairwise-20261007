package optical

import (
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

func sampleFor(runID model.ID, nm float64) model.ThicknessSample {
	return model.ThicknessSample{RunID: runID, Frequency: 10, Transmission: 0.9, Nanometers: nm, Valid: true, At: time.Now().UTC()}
}

// waitThenAdd sleeps long enough for a WaitForSample caller to snapshot its
// baseline and park on its deadline, then records the sample.
func waitThenAdd(monitor *Monitor, sample model.ThicknessSample) {
	time.Sleep(15 * time.Millisecond)
	monitor.Add(sample)
}

func TestWaitForSampleIgnoresPreviousRun(t *testing.T) {
	monitor := NewMonitor()
	previous := model.NewID()
	target := model.NewID()

	// Leftover thickness from the previous run must not be returned.
	monitor.Add(sampleFor(previous, 42))

	consumer := NewConsumer(monitor, make(chan model.ThicknessSample))
	start := time.Now()
	if sample, ok := consumer.WaitForSample(target, 30*time.Millisecond); ok {
		t.Fatalf("expected timeout against stale previous-run sample, got %+v", sample)
	}
	if elapsed := time.Since(start); elapsed < 25*time.Millisecond {
		t.Fatalf("timeout returned early after %v", elapsed)
	}

	// A fresh sample of the target run is delivered.
	go waitThenAdd(monitor, sampleFor(target, 7))
	sample, ok := consumer.WaitForSample(target, time.Second)
	if !ok {
		t.Fatal("expected to receive a fresh target-run sample")
	}
	if sample.RunID != target {
		t.Fatalf("got run %s, want %s", sample.RunID, target)
	}
	if sample.Nanometers != 7 {
		t.Fatalf("got %.1f nm, want 7", sample.Nanometers)
	}
}

func TestWaitForSampleRequiresSampleAfterCallStart(t *testing.T) {
	monitor := NewMonitor()
	runID := model.NewID()
	monitor.Add(sampleFor(runID, 11))

	consumer := NewConsumer(monitor, make(chan model.ThicknessSample))
	// Even a sample of the right run must be newer than the call.
	if _, ok := consumer.WaitForSample(runID, 20*time.Millisecond); ok {
		t.Fatal("expected timeout: pre-existing same-run sample is not new")
	}

	go waitThenAdd(monitor, sampleFor(runID, 13))
	sample, ok := consumer.WaitForSample(runID, time.Second)
	if !ok || sample.Nanometers != 13 {
		t.Fatalf("expected new sample (13), got ok=%v sample=%+v", ok, sample)
	}
}

func TestWaitForSampleIgnoresInterleavedRuns(t *testing.T) {
	monitor := NewMonitor()
	target := model.NewID()
	other := model.NewID()
	consumer := NewConsumer(monitor, make(chan model.ThicknessSample))

	go func() {
		time.Sleep(15 * time.Millisecond)
		monitor.Add(sampleFor(other, 99))
		time.Sleep(5 * time.Millisecond)
		monitor.Add(sampleFor(target, 3))
	}()
	sample, ok := consumer.WaitForSample(target, time.Second)
	if !ok {
		t.Fatal("expected target-run sample despite interleaved other-run sample")
	}
	if sample.RunID != target || sample.Nanometers != 3 {
		t.Fatalf("got %+v, want target run / 3 nm", sample)
	}
}

func TestWaitForSampleInvalidArgs(t *testing.T) {
	monitor := NewMonitor()
	consumer := NewConsumer(monitor, make(chan model.ThicknessSample))
	if _, ok := consumer.WaitForSample(model.NewID(), 0); ok {
		t.Fatal("non-positive timeout must not succeed")
	}
	if _, ok := consumer.WaitForSample("", time.Second); ok {
		t.Fatal("empty run ID must not succeed")
	}
}
