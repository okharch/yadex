package mdb // ConnectMongo establishes monitored connection to uri database server.
import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ConnectMongo connections state is broadcast by available channel.
// unless channel returned true there is no possibility to work with the connection
func ConnectMongo(ctx context.Context, uri string) (client *mongo.Client, available chan bool, err error) {
	available = make(chan bool, 1)
	svrMonitor := &event.ServerMonitor{
		TopologyDescriptionChanged: func(changedEvent *event.TopologyDescriptionChangedEvent) {
			servers := changedEvent.NewDescription.Servers
			avail := false
			for _, server := range servers {
				if server.AverageRTTSet {
					avail = true
				}
			}
			var availStr string
			if !avail {
				availStr = " not"
			}
			log.Infof("server %s is%s available", uri, availStr)
			available <- avail
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(svrMonitor)
	client, err = mongo.Connect(ctx, clientOpts)

	// wait until available
	avail := false
	for !avail {
		select {
		case <-ctx.Done():
			avail = true
		case avail = <-available:
		}
	}

	return client, available, ctx.Err()
}
