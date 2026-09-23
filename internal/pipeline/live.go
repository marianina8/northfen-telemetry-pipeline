package pipeline

import (
	"context"
	"time"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/store"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// TickEvent is reported after each streamed tick.
type TickEvent struct {
	Tick     int
	Result   ProcessResult
	Resolved []store.Alert // alerts explained/dispatched on this tick
}

// ByTick groups readings by tick, in tick order.
func ByTick(rs []telemetry.Reading) [][]telemetry.Reading {
	maxTick := -1
	for _, r := range rs {
		maxTick = max(maxTick, r.Tick)
	}
	out := make([][]telemetry.Reading, maxTick+1)
	for _, r := range rs {
		out[r.Tick] = append(out[r.Tick], r)
	}
	return out
}

// StreamLocal is the local stand-in for "simulator -> Kinesis -> consumer
// Lambda -> SQS -> explain Lambda": it feeds a run's readings one tick at a
// time at the run's pace (0 = as fast as possible), scores each tick, and
// resolves alerts once their settle time has passed. onTick may be nil.
func (s *Service) StreamLocal(ctx context.Context, run store.Run, rs []telemetry.Reading, onTick func(TickEvent)) error {
	interval := time.Duration(run.TickSeconds * float64(time.Second))
	var tk *time.Ticker
	if interval > 0 {
		tk = time.NewTicker(interval)
		defer tk.Stop()
	}
	for tick, batch := range ByTick(rs) {
		if tk != nil && tick > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tk.C:
			}
		}
		res, err := s.Process(ctx, batch)
		if err != nil {
			return err
		}
		done, err := s.Sweep(ctx, run.SessionID, true)
		if err != nil {
			return err
		}
		if onTick != nil {
			onTick(TickEvent{Tick: tick, Result: res, Resolved: done})
		}
	}
	// End of stream: anything still settling is resolved now.
	done, err := s.Sweep(ctx, run.SessionID, false)
	if err == nil && onTick != nil && len(done) > 0 {
		onTick(TickEvent{Tick: run.Ticks, Resolved: done})
	}
	return err
}
