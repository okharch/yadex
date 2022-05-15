package mongosync

import log "github.com/sirupsen/logrus"

// Keys returns slice with keys of the map
func Keys[T comparable, V any](m map[T]V) []T {
	result := make([]T, len(m))
	count := 0
	for k := range m {
		result[count] = k
		count++
	}
	return result
}

// Values returns slice with values of the map
func Values[T comparable, V any](m map[T]V) []V {
	result := make([]V, len(m))
	count := 0
	for _, v := range m {
		result[count] = v
		count++
	}
	return result
}

// SendState is used for state channels (cap==1) to communicate state to interested clients
func SendState[T any](state chan T, value T, traceOpt ...string) {
	var trace string
	if len(traceOpt) > 0 {
		trace = traceOpt[0]
	}
	if state == nil {
		log.Tracef("%s: ignored state %v", trace, value)
		return // ignore signal
	}
	// runtime check logical error
	if cap(state) != 1 {
		panic("Capacity of state channel should be 1!")
	}
	// nob-blocking clean the channel
	select {
	case pop := <-state:
		log.Tracef("%s: poped state %v", trace, pop)
	default:
	}
	// non-blocking set the value, if it was set before us, let it be
	select {
	case state <- value:
		log.Tracef("%s: set state %v", trace, value)
	default:
		log.Tracef("%s: dropped state %v", trace, value)
	}
}

// WaitState watches channel until desired value is received
func WaitState[T comparable](state chan T, desired T, trace string) {
	log.Tracef("waiting %s", trace)
	for state := range state {
		log.Tracef("%s got state %v", trace, state)
		if state == desired {
			log.Tracef("%s met state %v", trace, state)
			break
		}
		log.Tracef("waiting state %s", trace)
	}
	// put state back
	SendState(state, desired)
	log.Tracef("waiting %s done!", trace)
}

// ClearState drops all avlues from the channel non-blocking
func ClearState[T any](ch chan T) {
	for {
		// nb read from channel until empty
		select {
		case <-ch:
		default:
			return
		}
	}
}

// GetState works with channel of capacity 1 to pop the value until it pops
// and then push it back to the channel
func GetState[T any](ch chan T) T {
	if cap(ch) != 1 {
		panic("channel must have capacity one!")
	}
	state := <-ch
	// while it blocks to input refresh state again
	for {
		select {
		case ch <- state:
			return state
		case state = <-ch:
		}
	}
}

func MapSlice[T any, V any](list []T, F func(T) V) []V {
	result := make([]V, len(list))
	for i, v := range list {
		result[i] = F(v)
	}
	return result
}

func Filter[T any](list []T, f func(T) bool) []T {
	result := make([]T, len(list))
	count := 0
	for _, v := range list {
		if f(v) {
			result[count] = v
			count++
		}
	}
	return result[:count]
}
