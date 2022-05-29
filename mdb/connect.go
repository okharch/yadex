package mdb // ConnectMongo establishes monitored connection to uri database server.
import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// ConnectMongo connections state is broadcast by available channel.
// unless channel returned true there is no possibility to work with the connection
func ConnectMongo(ctx context.Context, uri string) (client *mongo.Client, available chan bool, err error) {
	available = make(chan bool, 1)
	init := false
	svrMonitor := &event.ServerMonitor{
		ServerClosed: func(openingEvent *event.ServerClosedEvent) {
			log.Infof("server %s closed", uri)
			available <- false
		},
		TopologyDescriptionChanged: func(changedEvent *event.TopologyDescriptionChangedEvent) {
			servers := changedEvent.NewDescription.Servers
			avail := false
			for _, server := range servers {
				if server.AverageRTTSet {
					avail = true
					break
				}
			}
			if init {
				if !avail {
					log.Warnf("server %s is down", uri)
				} else {
					log.Infof("server %s is up", uri)
				}
			}
			available <- avail
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetDirect(true).SetServerMonitor(svrMonitor)
	client, err = mongo.Connect(ctx, clientOpts)
	avail := <-available
	expire := time.Now().Add(time.Second * 3)
	for !avail && time.Now().Before(expire) {
		select {
		case avail = <-available:
		case <-time.After(time.Millisecond * 500):
		}
	}
	init = true
	available <- avail
	return client, available, ctx.Err()
}
