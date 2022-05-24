package logger

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func SetLogger(level log.Level, logFile string) {
	log.SetLevel(level)
	log.SetReportCaller(true)
	Formatter := new(log.TextFormatter)
	//Formatter.TimestampFormat = "2006-01-02T15:04:05.999999999Z07:00"
	Formatter.TimestampFormat = "2006-01-02T15:04:05.999"
	Formatter.FullTimestamp = true
	//Formatter.ForceColors = true
	log.SetFormatter(Formatter)
	// logFile is more error prone, setup it last
	//if logFile == "" {
	//	logFile = os.Getenv("YADEX_LOG")
	//}
	if logFile != "" {
		log.Infof("logging to file %s", logFile)
		//	os.Remove(fileName)
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Errorf("error opening file %s: %v", logFile, err)
			return
		}
		log.SetOutput(f)
	}
}
