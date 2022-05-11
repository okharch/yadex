package mongosync

import (
	log "github.com/sirupsen/logrus"
	"regexp"
	"sync"
	"yadex/config"
)

type (
	DataSyncCompiled struct {
		config  *config.DataSync
		exclude []*regexp.Regexp
	}
	collMatchEntry struct {
		config *config.DataSync
		rt     bool
	}
	collMatch []struct {
		re *regexp.Regexp
		ds DataSyncCompiled
	}
)

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
		result[i].ds = DataSyncCompiled{config: val, exclude: exclude[:j]}
		i++
	}
	return result[:i]
}

// findEntry iterates over collMatch to match collName against collMatch[i].RegExp
// it returns -1,-1 if the match has not been found
// otherwsie it returns (Max)Delay and (Max)Batch for the collection
func findEntry(cms collMatch, collName []byte) *config.DataSync {
nextMatch:
	for _, cm := range cms {
		if cm.re.Match(collName) {
			for _, exclude := range cm.ds.exclude {
				if exclude.Match(collName) {
					continue nextMatch
				}
			}
			return cm.ds.config
		}
	}
	return nil
}

type CollMatch func(coll string) (config *config.DataSync, realtime bool)

// GetCollMatch returns CollMatch func which returns Delay, Batch, realtime params for the collection
// it's behaviour is defined by configuration
// if it can't find an entry matched for the collection it returns Delay equal -1
func GetCollMatch(c *config.ExchangeConfig) CollMatch {
	// compile all regexp for RT
	rt := compileRegexps(c.RT)
	st := compileRegexps(c.ST)
	collEntry := make(map[string]collMatchEntry)
	var collEntryMutex sync.RWMutex
	return func(coll string) (config *config.DataSync, realtime bool) {
		// try cache first
		collEntryMutex.RLock()
		ce, ok := collEntry[coll]
		collEntryMutex.RUnlock()
		if ok {
			return ce.config, ce.rt
		}
		// look for RT
		cm := []byte(coll)
		config = findEntry(rt, cm)
		if config != nil {
			realtime = true
		} else {
			// look for ST
			config = findEntry(st, cm)
		}
		// cache the request, so next time it will be faster
		collEntryMutex.Lock()
		collEntry[coll] = collMatchEntry{config: config, rt: realtime}
		collEntryMutex.Unlock()
		return config, realtime
	}
}
