package utils

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
	"yadex/config"
)

func TestPPExecute(t *testing.T) {
	config.SetLogger(log.TraceLevel, "")
	var ppe PostponeExecutor
	executed := "no"
	var l sync.Mutex
	// test Context cancellation in the middle of func
	ExecuteOnDelay := func(name string, delayMS int) func(ctx context.Context) {
		return func(ctx context.Context) {
			l.Lock()
			executed = "started"
			l.Unlock()
			time.Sleep(time.Millisecond * time.Duration(delayMS)) // sleep 50ms in the func
			if ctx.Err() == nil {
				l.Lock()
				executed = name
				l.Unlock()
				log.Tracef("executed:%s", executed)
			} else {
				log.Tracef("context expired:%s", executed)
			}
		}
	}
	var ctx context.Context
	var cancel context.CancelFunc

	// test two postpones, should execute only second
	ctx, cancel = context.WithCancel(context.TODO())
	ppe.Postpone(ctx, ExecuteOnDelay("1st", 1), time.Millisecond*100)
	ppe.Postpone(ctx, ExecuteOnDelay("2nd", 1), time.Millisecond*100)
	time.Sleep(time.Millisecond * 150)
	l.Lock()
	require.Equal(t, "2nd", executed)
	executed = "no"
	l.Unlock()
	cancel()
	ctx, cancel = context.WithCancel(context.TODO())
	executed = "no"
	ppe.Postpone(ctx, ExecuteOnDelay("yes", 40), time.Millisecond*100)
	cancel()
	ppe.Wait()
	l.Lock()
	require.Equal(t, "no", executed)
	l.Unlock()

	ctx, cancel = context.WithCancel(context.TODO())
	defer cancel()
	log.Tracef("executed:%s", executed)
	ppe.Postpone(ctx, ExecuteOnDelay("yes", 1), time.Millisecond*100)
	// still not executed
	time.Sleep(time.Millisecond * 60)
	l.Lock()
	require.Equal(t, "no", executed)
	l.Unlock()

	// now it should have
	time.Sleep(time.Millisecond * 110)
	l.Lock()
	require.Equal(t, "yes", executed)
	l.Unlock()

	// test Cancel
	executed = "no"
	ppe.Postpone(ctx, ExecuteOnDelay("yes", 1), time.Millisecond*100)
	ppe.Cancel()
	l.Lock()
	require.Equal(t, "no", executed)
	l.Unlock()

}
