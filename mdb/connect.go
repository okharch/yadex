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
	svrMonitor := &event.ServerMonitor{
		ServerClosed: func(openingEvent *event.ServerClosedEvent) {
			log.Infof("server %s closed", uri)
			SendNB(available, false)
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
			if !avail {
				log.Warnf("server %s is down", uri)
			} else {
				log.Infof("server %s is up", uri)
			}
			SendNB(available, avail)
		},
	}
	clientOpts := options.Client().ApplyURI(uri).SetServerMonitor(svrMonitor)
	client, err = mongo.Connect(ctx, clientOpts)
	avail := <-available
	if !avail {
		expire := time.Now().Add(time.Second)
		for !avail && time.Now().Before(expire) {
			select {
			case avail = <-available:
			case <-time.After(time.Millisecond * 500):
			}
		}
	}
	SendNB(available, avail)
	return client, available, ctx.Err()
}

func SendNB(ch chan bool, val bool) {
	// empty, then put
	for {
		select {
		case <-ch:
		default:
			select {
			case ch <- val:
			default:
			}
			return
		}
	}
}
