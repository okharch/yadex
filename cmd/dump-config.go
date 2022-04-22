package main

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"os"
	"yadex/config"
)

func main() {
	// create a config with 2 exchanges
	c := &config.Config{
		Exchange: []*config.ExchangeConfig{
		{
			SenderURI:   "mongodb://localhost:27021",
			SenderDB:    "IonM",
			ReceiverURI: "mongodb://localhost:27023",
			ReceiverDB:  "IonM",
			RT: map[string]*config.DataSync{"realtime": {
				Delay:   100,
				Batch:   500,
				Exclude: "",
			},
			},
			ST:map[string]*config.DataSync{".*": {
				Delay:   100,
				Batch:   500,
				Exclude: "realtime",
			},
			},
		},
			{
				SenderURI:   "mongodb://localhost:27021",
				SenderDB:    "IonM",
				ReceiverURI: "mongodb://localhost:27024",
				ReceiverDB:  "IonM",
				RT: map[string]*config.DataSync{"realtime": {
					Delay:   100,
					Batch:   500,
					Exclude: "",
				},
				},
				ST:map[string]*config.DataSync{".*": {
					Delay:   100,
					Batch:   500,
					Exclude: "realtime",
				},
				},
			},
		},
	}
	b, err := yaml.Marshal(c)
	if err != nil {
		log.Fatalf("Failed to marshal YAML:%s",err)
	}
	fmt.Printf("YAML:\n%s\n", string(b))
	os.WriteFile("yadex-config.yaml", b, 0644)
	var yo config.Config
	err = yaml.Unmarshal(b, &yo)
	if err != nil {
		log.Fatalf("failed to unmarshal: %s", err)
	}
	b, err = json.MarshalIndent(c,"","\t")
	if err != nil {
		log.Fatalf("Failed to marshal YAML:%s",err)
	}
	os.WriteFile("yadex-config.json", b, 0644)
	err = json.Unmarshal(b, &yo)
	if err != nil {
		log.Fatalf("Failed to unmarshal JSON:%s",err)
	}
	fmt.Printf("JSON:\n%s\n", string(b))
}
