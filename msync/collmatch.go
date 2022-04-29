package mongosync

import (
	log "github.com/sirupsen/logrus"
	"regexp"
	"sync"
	"yadex/config"
)

type DataSyncCompiled struct {
	Delay, Batch int64
	Exclude      []*regexp.Regexp
}
type collMatchEntry struct {
	Delay, Batch int64
	rt           bool
}
type collMatch []struct {
	re *regexp.Regexp
	ds DataSyncCompiled
}

func compileRegexps(m map[string]*config.DataSync) collMatch {
	result := make(collMatch, len(m))
	i := 0
	for key, val := range m {
		cre, err := regexp.Compile(key)
		if err != nil {
			log.Warnf("ignored config RegExp %s : can't compile RegExp : %s", key, err)
			continue
		}
		result[i].re = cre
		// compile exclude
		exclude := make([]*regexp.Regexp, len(val.Exclude))
		j := 0
		for _, e := range val.Exclude {
			c, err := regexp.Compile(e)
			if err != nil {
				log.Warnf("skip exclude entry %s: failed to compile regexp", err)
				continue
			}
			exclude[j] = c
			j++
		}
		result[i].ds = DataSyncCompiled{Delay: val.Delay, Batch: val.Batch, Exclude: exclude[:j]}
		i++
	}
	return result[:i]
}
func findEntry(cms collMatch, collName []byte) (delay, batch int64) {
next_match:
	for _, cm := range cms {
		if cm.re.Match(collName) {
			for _, exclude := range cm.ds.Exclude {
				if exclude.Match(collName) {
					continue next_match
				}
			}
			return cm.ds.Delay, cm.ds.Batch
		}
	}
	return -1, -1
}

type CollMatch func(coll string) (Delay, Batch int64, realtime bool)

// GetCollMatch returns func which returns Delay, Batch, realtime params for the collection
// it's behaviour is defined by configuration
// if it can't find an entry matched for the collection it returns Delay=-1
func GetCollMatch(c *config.ExchangeConfig) CollMatch {
	// compile all regexp for RT
	rt := compileRegexps(c.RT)
	st := compileRegexps(c.ST)
	collEntry := make(map[string]collMatchEntry)
	var ceMu sync.RWMutex
	return func(coll string) (Delay, Batch int64, realtime bool) {
		// try cache first
		ceMu.RLock()
		ce, ok := collEntry[coll]
		ceMu.RUnlock()
		cm := []byte(coll)
		if ok {
			return ce.Delay, ce.Batch, ce.rt
		}
		// look for RT
		Delay, Batch = findEntry(rt, cm)
		if Delay != -1 {
			realtime = true
		} else {
			// look whether it ST
			Delay, Batch = findEntry(st, cm)
		}
		ceMu.Lock()
		collEntry[coll] = collMatchEntry{Delay: Delay, Batch: Batch, rt: realtime}
		ceMu.Unlock()
		return Delay, Batch, realtime
	}
}
