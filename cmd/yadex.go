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

func createExchanges(ctx context.Context, cfg *config.Config, waitExchanges *sync.WaitGroup) {
	for _, s := range cfg.Exchanges {
		ms, err := mongosync.NewMongoSync(ctx, s, waitExchanges)
		if err != nil {
			log.Errorf("Failed to establish sync %s", ms.Name())
			continue
		}
		log.Infof("running exchange %s...", ms.Name())
	}
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
	var ctx context.Context
	var cancel context.CancelFunc
	var waitExchanges sync.WaitGroup
	stopExchanges := func() {
		if ctx == nil {
			return
		}
		log.Info("wait stopping exchanges...")
		cancel()
		waitExchanges.Wait()
	}
	// here we watch for changes in config and restarting exchanges
	// configChan gets closed on os.Interrupt signal
	configChan := config.MakeWatchConfigChannel(ctx, configFileName)
	for cfg := range configChan {
		// close previous exchanges
		stopExchanges()
		// create new exchanges from Cfg
		ctx, cancel = context.WithCancel(context.Background())
		createExchanges(ctx, cfg, &waitExchanges)
	}
	stopExchanges()
	log.Info("yadex exited gracefully")
}
