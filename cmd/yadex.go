package main

import (
	"context"
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"yadex/config"
	mongosync "yadex/msync"
)

func createExchanges(ctx context.Context, cfg *config.Config, wg *sync.WaitGroup) []*mongosync.MongoSync {
	var result []*mongosync.MongoSync
	for _, s := range cfg.Exchanges {
		msync, err := mongosync.NewMongoSync(ctx, s, wg)
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
	// configFileName
	configFileName = os.Getenv("YADEX_CONFIG")
	if configFileName == "" {
		configFileName = "yadex-config.yaml"
	}
	flag.StringVar(&configFileName, "config", configFileName, "path to config file")
	flag.Parse()
}

func main() {
	log.SetReportCaller(true)
	processCommandLine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	configChan := config.MakeWatchConfigChannel(ctx, configFileName)
	var Exchanges []*mongosync.MongoSync
	var waitExchanges sync.WaitGroup
	stopExchanges := func() {
		if Exchanges == nil {
			return
		}
		log.Info("wait stopping exchanges...")
		for _, msync := range Exchanges {
			close(msync.ExchangeAvailable)
		}
		waitExchanges.Wait()
	}
	// here we watch for changes in config and restarting exchanges
	for cfg := range configChan {
		// close previous exchanges
		stopExchanges()
		// create new exchanges from Cfg
		Exchanges = createExchanges(ctx, cfg, &waitExchanges)
	}
	stopExchanges()
	log.Info("yadex exited gracefully")
}
