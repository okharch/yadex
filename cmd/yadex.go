package main

import (
	"context"
	"flag"
	log "github.com/sirupsen/logrus"
	"sync"
	"yadex/config"
	mongosync "yadex/msync"
)

func createExchanges(ctx context.Context, cfg *config.Config) []*mongosync.MongoSync {
	var result []*mongosync.MongoSync
	for _, s := range cfg.Exchanges {
		msync, err := mongosync.NewMongoSync(ctx, s)
		if err != nil {
			log.Errorf("Failed to establish sync with config %+v", msync)
			continue
		}
		result = append(result, msync)
	}
	return result
}

var configFileName string

func processCommandLine() {
	flag.StringVar(&configFileName, "config", "config.yaml", "path to config file")
	//flag.BoolVar(&fields, "fields", false, "Count difference for each field present in documents (may slow down)")
	//flag.BoolVar(&verbose, "verbose", false, "Show report including where collections is identical")
	flag.Parse()
}

func main() {
	log.SetReportCaller(true)
	processCommandLine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configChan := config.MakeWatchConfigChannel(ctx, configFileName)
	var Exchanges []*mongosync.MongoSync
	var wg sync.WaitGroup
	stopExchanges := func() {
		if Exchanges == nil {
			return
		}
		log.Info("Waiting stopping exchanges...")
		for _, msync := range Exchanges {
			close(msync.ExchangeAvailable)
		}
		wg.Wait()
	}
	// here we watch for changes in config and restarting exchanges
	for cfg := range configChan {
		// close previous exchanges
		stopExchanges()
		// create new exchanges from Cfg
		Exchanges = createExchanges(ctx, cfg)
		for _, msync := range Exchanges {
			wg.Add(1)
			go func(msync *mongosync.MongoSync) {
				defer wg.Done()
				// watch over exchange availability and start/stop exchange depending on it
				for avail := range msync.ExchangeAvailable {
					if avail {
						msync.Start()
					} else {
						msync.Stop()
					}
				}
				msync.Stop()
			}(msync)
		}
	}
	stopExchanges()
	log.Info("yadex exited gracefully")
}
