package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CollCompare struct {
	Name         string
	sDocs, rDocs []bson.Raw
	sDocsBytes,
	rDocsBytes,
	sDocsCount,
	rDocsCount,
	Changed,
	Removed,
	Added int
	fieldNames []string
	Fields     map[string]int // fields which are different count
}

var verbose, fields bool

func ConnectServer(ctx context.Context, mongoURI string) (*mongo.Client, error) {
	log.Debugf("Connecting to mongo instance : %s", mongoURI)
	client, err := mongo.NewClient(options.Client().ApplyURI(mongoURI).SetDirect(true))
	if err != nil {
		return nil, err
	}
	err = client.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("was not able to connect to mongod instance %s: %w", mongoURI, err)
	}
	return client, nil
}

func connectDB(ctx context.Context, name, uri, dbName string, collsRE *regexp.Regexp) (*mongo.Database, []string, error) {
	log.Tracef("Connecting to %s DB %s at %s", name, dbName, uri)
	client, err := ConnectServer(ctx, uri)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to %s DB %s at %s: %w", name, dbName, uri, err)
	}
	db := client.Database(dbName)
	all, err := db.ListCollectionNames(ctx, bson.M{})
	var colls []string
	for _, coll := range all {
		if collsRE.Match([]byte(coll)) {
			colls = append(colls, coll)
		}
	}
	return db, colls, err

}

func GetCompareChan() (chan *CollCompare, *sync.WaitGroup) {
	// lets make it buffered to allow paralel comparizon which uses all CPUs
	threads := runtime.NumCPU()
	if threads < 1 {
		threads = 1
	} else {
		//fmt.Printf("Will use %d threads to compare\n", threads)
	}
	CollsCompare := make(chan *CollCompare, threads-1)
	var wg sync.WaitGroup
	var t CollCompare
	var out sync.Mutex
	var remain int
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range CollsCompare {
				out.Lock()
				remain++
				out.Unlock()
				CompareDocs(c)
				fieldsDiff := getFieldsDiff(c.Fields)
				if c.Changed+c.Added+c.Removed > 0 || verbose {
					out.Lock()
					fmt.Printf("%d(%d) \t%d(%d)\t*%d\t+%d\t-%d\t%s\t%s\n", len(c.sDocs), c.sDocsBytes, len(c.rDocs), c.rDocsBytes, c.Changed,
						c.Added, c.Removed, fieldsDiff, c.Name)
					remain--
					out.Unlock()
				}
				t.Removed += c.Removed
				t.Added += c.Added
				t.Changed += c.Changed
				t.sDocsBytes += c.sDocsBytes
				t.rDocsBytes += c.rDocsBytes
				t.sDocsCount += len(c.sDocs)
				t.rDocsCount += len(c.rDocs)
			}
		}()
	}
	go func() {
		wg.Wait()
		fmt.Printf("%d(%d)\t%d(%d)\t*%d\t+%d\t-%d\tTotal\n", t.sDocsCount, t.sDocsBytes, t.rDocsCount, t.rDocsBytes, t.Changed, t.Added, t.Removed)
	}()
	return CollsCompare, &wg
}

func getFieldsDiff(fields map[string]int) string {
	var sb strings.Builder
	for f, c := range fields {
		sb.WriteString("[" + f + "]:" + strconv.Itoa(c) + " ")
	}
	return sb.String()
}

func main() {
	var senderURI, senderDB, receiverURI, receiverDB, colls string
	flag.StringVar(&senderURI, "sender-uri", "mongodb://localhost:27019", "sender URI to connect")
	flag.StringVar(&receiverURI, "receiver-uri", "mongodb://localhost:27020", "receiver URI to connect")
	flag.StringVar(&senderDB, "sender-db", "IonM", "sender URI to connect")
	flag.StringVar(&receiverDB, "receiver-db", "IonM", "receiver URI to connect")
	flag.StringVar(&colls, "colls", ".*", "Regexp to limit collections for comparison to particular template, default - all collections")
	flag.BoolVar(&fields, "fields", false, "Count difference for each field present in documents (may slow down)")
	flag.BoolVar(&verbose, "verbose", false, "Show report including where collections is identical")
	flag.Parse()

	log.SetReportCaller(true)
	log.SetLevel(log.InfoLevel)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	collsRE, err := regexp.Compile(colls)
	if err != nil {
		log.Fatalf("invalid regexp for -colls parameter: %s: %s", colls, err)
	}
	sender, sColls, err := connectDB(ctx, "sender", senderURI, senderDB, collsRE)
	if err != nil {
		log.Fatal(err)
		return
	}
	receiver, rColls, err := connectDB(ctx, "receiver", receiverURI, receiverDB, collsRE)
	if err != nil {
		log.Fatal(err)
		return
	}
	CollReceived := make(map[string]struct{})
	for _, collName := range rColls {
		CollReceived[collName] = struct{}{}
	}
	compChan, wg := GetCompareChan()
	for _, collName := range sColls {
		_, ok := CollReceived[collName]
		if !ok {
			fmt.Printf("%s not received\n", collName)
			continue
		}
		//log.Infof("compare collections %s", collName)
		c, err := ReadDocs(ctx, collName, sender, receiver)
		if err != nil {
			log.Fatalf("error reading documents for collection %s: %s", collName, err)
		}
		compChan <- c
	}
	close(compChan)
	wg.Wait()
	time.Sleep(time.Millisecond * 20) // wait for goroutine to output totals
}

func getDocs(ctx context.Context, coll *mongo.Collection) (result []bson.Raw, err error) {
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"_id", 1}})
	cur, err := coll.Find(ctx, bson.D{}, findOptions)
	if err != nil {
		return nil, err
	}
	err = cur.All(ctx, &result)
	return result, err
}

func ReadDocs(ctx context.Context, collName string, sender, receiver *mongo.Database) (*CollCompare, error) {
	var r CollCompare
	var errSender, errReceiver error
	var wg sync.WaitGroup
	r.Name = collName
	wg.Add(1)
	go func() {
		defer wg.Done()
		coll := sender.Collection(r.Name)
		r.sDocs, errSender = getDocs(ctx, coll)
		if !fields {
			return
		}
		// find  metadata for the columns of "fields" array
		var md bson.M
		for _, d := range r.sDocs {
			t := d.Lookup("type")
			if t.Value == nil {
				continue
			}
			s := t.StringValue()
			if s == "MetaData" {
				if err := bson.Unmarshal(d, &md); err != nil {
					return
				}
				break
			}
		}
		if md == nil {
			return
		}

		// each regular collection has record of "Record" type which has "fields" array
		// which contains values for those "fields"
		// here get field's names list from metadata
		// and put that list into r.fieldNames
		fl, ok := md["fields"]
		if !ok {
			return
		}
		fc, ok := md["fieldCount"]
		if !ok {
			return
		}
		c, ok := fc.(int32)
		if !ok {
			return
		}
		r.fieldNames = make([]string, c)
		fla, ok := fl.(bson.A)
		if !ok {
			return
		}
		for i := 0; i < int(c); i++ {
			f, ok := fla[i].(bson.M)
			if !ok {
				r.fieldNames[i] = "#" + strconv.Itoa(i)
				continue
			}
			n, ok := f["name"]
			if !ok {
				r.fieldNames[i] = "#" + strconv.Itoa(i)
				continue
			}
			r.fieldNames[i], _ = n.(string)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.rDocs, errReceiver = getDocs(ctx, receiver.Collection(r.Name))
	}()
	wg.Wait()
	if errSender != nil {
		return nil, errSender
	}
	if errReceiver != nil {
		return nil, errReceiver
	}
	return &r, nil
}

func CompareDocs(cmp *CollCompare) {
	if len(cmp.sDocs) == 0 {
		cmp.Added = len(cmp.rDocs) - len(cmp.sDocs)
		for _, r := range cmp.rDocs {
			cmp.rDocsBytes += len(r)
		}
		return
	} else if len(cmp.rDocs) == 0 {
		cmp.Removed = len(cmp.sDocs) - len(cmp.rDocs)
		for _, r := range cmp.sDocs {
			cmp.rDocsBytes += len(r)
		}
		return
	}
	i1 := 0
	i2 := 0
	for i1 < len(cmp.sDocs) && i2 < len(cmp.rDocs) {
		r1 := cmp.sDocs[i1]
		r2 := cmp.rDocs[i2]
		cmp.rDocsBytes += len(r1)
		cmp.sDocsBytes += len(r2)
		c := bytes.Compare(r1, r2)
		if c == 0 {
			// docs are identical
			i1++
			i2++
			continue
		}
		// compare ids if they are not equal we got different docs here so we operate on Added++ or Removed++
		id1 := r1.Lookup("_id")
		id2 := r2.Lookup("_id")
		c = bytes.Compare(id1.Value, id2.Value)
		if c < 0 {
			i1++
			cmp.rDocsBytes += len(r1)
			cmp.Removed++
		} else if c > 0 {
			i2++
			cmp.sDocsBytes += len(r2)
			cmp.Added++
		} else {
			// documents have the same id: Changed++
			cmp.Changed++
			i1++
			i2++
			if fields {
				// compare documents on per field basis
				if cmp.Fields == nil {
					cmp.Fields = make(map[string]int)
				}
				compareFields(r1, r2, cmp)
			}
		}
	}
	cmp.Removed += len(cmp.sDocs) - i1
	cmp.Added += len(cmp.rDocs) - i2
	return
}

func compareFields(r1, r2 bson.Raw, c *CollCompare) {
	var m1, m2 bson.M
	if err := bson.Unmarshal(r1, &m1); err != nil {
		return
	}
	if err := bson.Unmarshal(r2, &m2); err != nil {
		return
	}
	//if c.Fields == nil {
	//	c.Fields = make(map[string]int)
	//}
	for f, v1 := range m1 {
		v2, ok := m2[f]
		if !ok {
			c.Fields[f] = c.Fields[f] + 1
			continue
		}
		// specials handling for "fields" field which contain record as an array.
		if f == "fields" {
			a1, ok := v1.(bson.A)
			if !ok {
				c.Fields[f] = c.Fields[f] + 1
				continue
			}
			a2, ok := v2.(bson.A)
			if !ok {
				c.Fields[f] = c.Fields[f] + 1
				continue
			}
			if len(a1) != len(a2) || len(a1) != len(c.fieldNames) {
				c.Fields[f] = c.Fields[f] + 1
				continue
			}
			for i := range a1 {
				if isEqual(a1[i], a2[i]) {
					continue
				}
				f1, ok1 := a1[i].(float64)
				f2, ok2 := a2[i].(float64)
				if ok1 && ok2 && almostEqual(f1, f2) {
					continue
				}
				// metadata for each element array including the name of the field in the array is stored into "MetaData record of the
				// collection and is extracted before comparing records (see
				f := c.fieldNames[i]
				c.Fields[f] = c.Fields[f] + 1
			}
		} else {
			if v1 != v2 {
				c.Fields[f] = c.Fields[f] + 1
			}
		}
	}
}

func almostEqual(a, b float64) bool {
	const float64EqualityThreshold = 1e-6
	if a == 0 && b == 0 {
		return true
	}
	return math.Abs(math.Abs(a)-math.Abs(b))/(math.Abs(a)+math.Abs(b)) <= float64EqualityThreshold
}

func isEqual(a, b interface{}) (result bool) {
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	result = a == b
	return
}

// perl -lne'$c{$1}++ for m{\[(.*?)\]}g; END{print "$_:$c{$_}\n" for keys %c}' ll
