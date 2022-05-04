package mongosync

import (
	log "github.com/sirupsen/logrus"
	"regexp"
	"sync"
	"yadex/config"
)

type DataSyncCompiled struct {
	Delay, Batch int
	Exclude      []*regexp.Regexp
}
type collMatchEntry struct {
	Delay, Batch int
	rt           bool
}
type collMatch []struct {
	re *regexp.Regexp
	ds DataSyncCompiled
}

// compileRegexps compiles regexps from config into array of patterns
// to match collection Name for DataSync configuration, that is MaxDelay and MaxBatch
func compileRegexps(m map[string]*config.DataSync) collMatch {
	result := make(collMatch, len(m))
	i := 0
	for key, val := range m {
		// key is regexp, val is config.DataSync
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
			// item of exclude array is RegExp
			c, err := regexp.Compile(e)
			if err != nil {
				log.Warnf("skip exclude entry %s: failed to compile regexp", err)
				continue
			}
			exclude[j] = c
			j++
		}
		exclude = exclude[:j]
		result[i].ds = DataSyncCompiled{Delay: val.Delay, Batch: val.Batch, Exclude: exclude[:j]}
		i++
	}
	return result[:i]
}

// findEntry iterates over collMatch to match collName against collMatch[i].RegExp
// it returns -1,-1 if the match has not been found
// otherwsie it returns (Max)Delay and (Max)Batch for the collection
func findEntry(cms collMatch, collName []byte) (delay, batch int) {
nextMatch:
	for _, cm := range cms {
		if cm.re.Match(collName) {
			for _, exclude := range cm.ds.Exclude {
				if exclude.Match(collName) {
					continue nextMatch
				}
			}
			return cm.ds.Delay, cm.ds.Batch
		}
	}
	return -1, -1
}

type CollMatch func(coll string) (Delay, Batch int, realtime bool)

// GetCollMatch returns CollMatch func which returns Delay, Batch, realtime params for the collection
// it's behaviour is defined by configuration
// if it can't find an entry matched for the collection it returns Delay equal -1
func GetCollMatch(c *config.ExchangeConfig) CollMatch {
	// compile all regexp for RT
	rt := compileRegexps(c.RT)
	st := compileRegexps(c.ST)
	collEntry := make(map[string]collMatchEntry)
	var collEntryMutex sync.RWMutex
	return func(coll string) (Delay, Batch int, realtime bool) {
		// try cache first
		collEntryMutex.RLock()
		ce, ok := collEntry[coll]
		collEntryMutex.RUnlock()
		if ok {
			return ce.Delay, ce.Batch, ce.rt
		}
		// look for RT
		cm := []byte(coll)
		Delay, Batch = findEntry(rt, cm)
		if Delay != -1 {
			realtime = true
		} else {
			// look for ST
			Delay, Batch = findEntry(st, cm)
		}
		// cache the request, so next time it will be faster
		collEntryMutex.Lock()
		collEntry[coll] = collMatchEntry{Delay: Delay, Batch: Batch, rt: realtime}
		collEntryMutex.Unlock()
		return Delay, Batch, realtime
	}
}
