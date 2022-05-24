package mongosync

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
	"yadex/utils"
)

// checkIdle facility created in order to detect situation
// when there is no incoming records
// and all the pending buffers with updates have been flushed.

func (ms *MongoSync) getBWSpeed() int {
	ms.bulkWriteMutex.RLock()
	defer ms.bulkWriteMutex.RUnlock()
	var totalDuration time.Duration
	totalBytes := 0
	for _, bwLog := range ms.bwLog {
		totalDuration += bwLog.duration
		totalBytes += bwLog.bytes
	}
	// avoid divide by zero
	if totalDuration == 0 {
		return 0
	}
	return totalBytes * int(time.Second) / int(totalDuration)
}

// WaitJobDone gets current ms.collUpdate state. If it is clean, it waits timeout if it stays clear all the time.
// WaitJobDone is intended for unit tests.
// it also checks ms.Ready and returns error if it is not
// It also checks if IsClean closed then it returns with error
func (ms *MongoSync) WaitJobDone(timeout time.Duration) error {
	WaitState(ms.Ready, true, "ms.Ready")
	ok := true
	for ok {
		if !GetState(ms.Ready) {
			return fmt.Errorf("ms is not Ready")
		}
		// we are clean here
		log.Trace("JobDone: waiting clean")
		var clean bool
		clean, ok = <-ms.IsClean
		log.Tracef("JobDone: got clean %v state", clean)
		if !clean {
			continue
		}
		log.Tracef("JobDone: waiting it is clean for %v", timeout)
		select {
		case clean, ok = <-ms.IsClean:
			log.Tracef("JobDone: got clean %v state(must be false)", clean)
			continue
		case <-time.After(timeout):
			log.Trace("JobDone: Ready!")
			return nil
		}
	}
	return fmt.Errorf("JobDone: IsClean channel closed")
}

type BWLog struct {
	duration time.Duration
	bytes    int
}

func (ms *MongoSync) showSpeed(ctx context.Context) {
	defer ms.routines.Done() // showSpeed
	for range time.Tick(time.Second) {
		if ctx.Err() != nil {
			log.Debug("gracefully shutdown showSpeed on cancelled context")
			return
		}
		log.Tracef("BulkWriteOp: avg speed %s bytes/sec", utils.IntCommaB(ms.getBWSpeed()))
	}
}

func showChan[T any](ch chan T) string {
	return fmt.Sprintf("%d(%d)", len(ch), cap(ch))
}

//var locked = make(map[string]string)
//var lmutex sync.Mutex

//func getLock(l *sync.RWMutex, read bool, trace string) {
//	if read {
//		l.RLock()
//	} else {
//		l.Lock()
//	}
//	return
//_, file, line, _ := runtime.Caller(1)
//place := fmt.Sprintf("%s:%d", file, line)
//timer := time.AfterFunc(time.Millisecond*20, func() {
//	lmutex.Lock()
//	panic(fmt.Sprintf("failed to acquire lock %s(%s) at %s. Previous lock at %s.", hashValue([]byte(trace)), trace, place,
//		locked[trace]))
//	lmutex.Unlock()
//})
//if read {
//	l.RLock()
//	timer.Stop()
//	lmutex.Lock()
//	locked[trace] = fmt.Sprintf("%s\nRlock:%s", locked[trace], place)
//	log.Tracef("RLock %s at %s.", hashValue([]byte(trace)), place)
//} else {
//	l.Lock()
//	timer.Stop()
//	lmutex.Lock()
//	locked[trace] = fmt.Sprintf("%s\nLock:%s", locked[trace], place)
//	log.Tracef("Lock %s at %s.", hashValue([]byte(trace)), place)
//}
//lmutex.Unlock()
//}

//func releaseLock(l *sync.RWMutex, read bool, trace string) {
//	if read {
//		l.RUnlock()
//	} else {
//		l.Unlock()
//	}
//	//return
//	//_, file, line, _ := runtime.Caller(1)
//	//place := fmt.Sprintf("%s:%d", file, line)
//	//lmutex.Lock()
//	//if read {
//	//	l.RUnlock()
//	//	locked[trace] = fmt.Sprintf("%s\nRlock:%s", locked[trace], place)
//	//	log.Tracef("RUnlock %s at %s.", hashValue([]byte(trace)), place)
//	//} else {
//	//	l.Unlock()
//	//	locked[trace] = fmt.Sprintf("%s\nUnlock:%s", locked[trace], place)
//	//	log.Tracef("Unlock %s at %s.", hashValue([]byte(trace)), place)
//	//}
//	//lmutex.Unlock()
//}

func hashValue(value []byte) string {
	v := md5.Sum(value)
	result := hex.EncodeToString(v[0:])
	return result[:6]
}

//func ShowDelta(n interface{}) string {
//	result := fmt.Sprintf("%d", n)
//	if result[0] != '-' {
//		result = "+" + result
//	}
//	return "delta " + result
//}
//
//func CancelWait(ctx context.Context, signal chan struct{}) bool {
//	if signal != nil {
//		select {
//		case <-signal:
//		case <-ctx.Done():
//			return true
//		}
//	}
//	return false
//}

// Send signal(struct{}{}) non-blockingly
//func SendSignal(ch chan struct{}) {
//	if ch == nil {
//		return
//	}
//	select {
//	case ch <- struct{}{}:
//	default:
//	}
//}
