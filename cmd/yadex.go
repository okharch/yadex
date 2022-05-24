package main

import (
	"context"
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"yadex/config"
	"yadex/logger"
	msync "yadex/msync"
)

func createExchanges(ctx context.Context, cfg *config.Config) []*msync.MongoSync {
	makeMSync := func(exchangeConfig *config.ExchangeConfig) *msync.MongoSync {
		ms, err := msync.NewMongoSync(ctx, exchangeConfig)
		if err != nil {
			log.Fatalf("Failed to establish sync %s", ms.Name())
		}
		log.Tracef("exchange %s has been created", ms.Name())
		return ms
	}
	return msync.MapSlice(cfg.Exchanges, makeMSync)
}

var configFileName string
var logFileName string
var logLevel int

func processCommandLine() {
	// configFileName
	configFileName = os.Getenv("YADEX_CONFIG")
	if configFileName == "" {
		configFileName = "yadex-config.yaml"
	}
	logFileName = os.Getenv("YADEX_LOG")
	flag.StringVar(&configFileName, "config", configFileName, "path to config file")
	flag.StringVar(&logFileName, "logfile", logFileName, "path to config file")
	flag.IntVar(&logLevel, "loglevel", int(log.InfoLevel), "level from 1 to 6 (critical,error,warn,info,debug,trace))")
	flag.Parse()
}

func main() {
	processCommandLine()
	logger.SetLogger(log.Level(logLevel), logFileName)
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
	configChan := config.MakeWatchConfigChannel(context.TODO(), configFileName)
	for cfg := range configChan {
		// close previous exchanges
		stopExchanges()
		// create new exchanges from Cfg
		ctx, cancel = context.WithCancel(context.Background())
		log.Trace("creating exchanges...")
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
