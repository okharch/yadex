package main

import (
	"context"
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"yadex/config"
	msync "yadex/msync"
)

func createExchanges(ctx context.Context, cfg *config.Config) []*msync.MongoSync {
	makeMSync := func(s *config.ExchangeConfig) *msync.MongoSync {
		ready := make(chan bool, 1)
		ms, err := msync.NewMongoSync(ctx, s, ready)
		if err != nil {
			log.Errorf("Failed to establish sync %s", ms.Name())
			return nil
		}
		return ms
	}
	return msync.MapSlice(cfg.Exchanges, makeMSync)
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
	configChan := config.MakeWatchConfigChannel(context.TODO(), configFileName)
	for cfg := range configChan {
		// close previous exchanges
		stopExchanges()
		// create new exchanges from Cfg
		ctx, cancel = context.WithCancel(context.Background())
		mss := createExchanges(ctx, cfg)
		for _, ms := range mss {
			if ms == nil {
				continue
			}
			waitExchanges.Add(1)
			go func(ms *msync.MongoSync) {
				defer waitExchanges.Done()
				ms.Run(ctx)
			}(ms)
		}
	}
	stopExchanges()
	log.Info("yadex exited gracefully")
}
