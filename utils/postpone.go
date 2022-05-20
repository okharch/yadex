package utils

import (
	"context"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// implements object which can execute postponed operation
type PostponeExecutor struct {
	sync.RWMutex
	sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Postpone call postpones action(ctx) for specified duration.
// action should respect context and return immediately if context expired
// If before the execution of action there was another call of the Postpone method with this object,
// previous action will be cancelled. context for it will be cancelled so action can cancel in the middle
// still if action is already executed it will wait for the action to finish
func (ppe *PostponeExecutor) Postpone(ctx context.Context, f func(ctx context.Context), timeout time.Duration) {
	ppe.Cancel()
	ppe.Lock()
	ppe.ctx, ppe.cancel = context.WithCancel(ctx)
	ppe.Unlock()
	ppe.Add(1)
	go func() {
		defer ppe.Done()
		select {
		case <-ppe.ctx.Done():
			ppe.Lock()
			ppe.cancel()
			ppe.cancel = nil
			ppe.Unlock()
		case <-time.After(timeout):
			f(ppe.ctx)
			//log.Trace("cancelling")
			ppe.Lock()
			if ppe.cancel != nil {
				ppe.cancel()
				ppe.cancel = nil
			}
			ppe.Unlock()
		}
	}()
}

// Cancel method cancells action that were ppostponed for execution
// it cancels the context and if action is already being executed it waits for its completion
// a action should respect context and return immediately if context is cancelled
func (ppe *PostponeExecutor) Cancel() {
	ppe.RLock()
	cancel := ppe.cancel
	ppe.RUnlock()
	if cancel != nil {
		cancel()
		log.Info("real cancel, before wait!")
		ppe.Wait()
	}
	ppe.Lock()
	ppe.cancel = nil
	ppe.Unlock()
}
