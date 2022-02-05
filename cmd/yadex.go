package main

import (
	"context"
	"flag"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

// ConnectMongo establishes monitored connection to uri database server.
// connections state is broadcast by available channel.
// unless channel returned true there is no possibility to work with the connection
func ConnectMongo(ctx context.Context, uri string) (client *mongo.Client, available chan bool, err error) {
	available = make(chan bool)
	svrMonitor := &event.ServerMonitor{
		TopologyDescriptionChanged: func(changedEvent *event.TopologyDescriptionChangedEvent) {
			servers := changedEvent.NewDescription.Servers
			avail := false
			for _, server := range servers {
				if server.AverageRTTSet {
					avail = true
				}
			}
			available <- avail
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(svrMonitor)
	//clientOpts.SetConnectTimeout(time.Second*5)
	client, err = mongo.Connect(ctx, clientOpts)
	return client, available, err
}

func FatalError(context string, err error) {
	if err == nil {
		return
	}
	log.Fatal(context + ": "+ err.Error())
}

func RunExchange(ctx context.Context, senderURI, receiverURI string) {
	receiver, rcvAvail, err := ConnectMongo(ctx, receiverURI)
	if err != nil {
		log.Fatalf("failed to connect to receiver %s : %s", receiverURI, err)
	}
	sender, sndAvail, err := ConnectMongo(ctx, senderURI)
	if err != nil {
		log.Fatalf("failed to connect to sender %s : %s", senderURI, err)
	}
	dex := CreateDEX(sender, receiver)
	var receiverAvailable, senderAvailable bool
	dexing:	for {
		prevCanDex := receiverAvailable && senderAvailable
		select {
		case receiverAvailable = <- rcvAvail:
		case senderAvailable = <- sndAvail:
		case <-ctx.Done():
			dex.Stop()
			break dexing
		}
		canDex := receiverAvailable && senderAvailable
		if prevCanDex == canDex {
			continue dexing
		}
		if receiverAvailable && senderAvailable {
			dex.Start()
		} else {
			dex.Stop()
		}
	}
}


func main() {
	ctx := context.Background()
	RunExchange()
}
