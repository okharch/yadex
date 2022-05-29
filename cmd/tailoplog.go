package main

import (
	"context"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"os"
	"os/signal"
	"yadex/mdb"
	msync "yadex/msync"
)

func main() {
	uri := "mongodb://localhost:27021"
	dbName := "test"
	ctx, cancel := context.WithCancel(context.TODO())
	//signal.Notify(osSignal, syscall.SIGHUP) // reload config
	client, available, err := mdb.ConnectMongo(ctx, uri)
	if err != nil {
		log.Fatalf("Failed to connect to %s: %s", uri, err)
	}
	db := client.Database(dbName)
	// watch over osSignal and cancel context on Terminate
	go func() {
		osSignal := make(chan os.Signal, 1)
		signal.Notify(osSignal, os.Interrupt)
		<-osSignal
		cancel()
	}()
	for avail := range available {
		if !avail {
			log.Warnf("DB at %s is not available", uri)
			continue
		}
		log.Info("initialize watching changestream at %s db %s", uri, dbName)
		changeStream, err := msync.GetChangeStream(ctx, db, "")
		if err != nil {
			log.Fatalf("failed to tail oplog at %s : %s")
		}
		log.Info("watching changestream at %s db %s", uri, dbName)
		for {
			next := changeStream.TryNext(ctx)
			if !next {
				if ctx.Err() != nil {
					log.Tracef("leaving oplog %s due cancelled context ", uri)
					return
				}
				log.Infof("getOplog %s db %s: idling", uri, dbName)
				next = changeStream.Next(ctx)
			}
			if next {
				var op bson.M
				err := bson.Unmarshal(changeStream.Current, &op)
				if err != nil {
					log.Errorf("failed to unmarshal oplog entry: %s", err)
					continue
				}
				log.Infof("oplog entry: %+v", op)
				continue
			}
			if err := changeStream.Err(); err != nil {
				if errors.Is(err, context.Canceled) {
					log.Infof("gracefully exit on Ctrl-C")
					return
				}
				log.Errorf("failed to get %s oplog entry: %s", uri, err)
				return
			}
		}
	}
}
