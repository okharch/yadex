package mongosync

import (
	log "github.com/sirupsen/logrus"
	"regexp"
	"yadex/config"
)

type (
	DataSyncCompiled struct {
		config  *config.DataSync
		exclude []*regexp.Regexp
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
