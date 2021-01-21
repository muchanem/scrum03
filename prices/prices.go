package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
	"log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"os"
)

type price struct {
	Kind string `json:"kind,omitempty"`
	Date primitive.DateTime `json:"date,omitempty"`
	Price float64 `json:"price,omitempty"`
	Volume int `json:"volume,omitempty"`
}
var start string = "2019-06-01"
var end string = "2019-08-31"
func main() {
	
	client, err := mongo.NewClient(options.Client().ApplyRI("mongodb://myUserAdmin:scrum3ncssm@138.197.6.239:27017"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)
	db := client.Database("testing")
	s, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	//var wg sync.WaitGroup
	for _, v := range s {
		getprices(ctx, v, client)	
		//wg.Add(1)
		//go worker(ctx, v, &wg, client)
	}
	//wg.Wait()

}

func worker(ctx context.Context, id string, wg *sync.WaitGroup, client *mongo.Client) {
	defer wg.Done()
	fmt.Println("Starting Worker: " + id)
	getprices(ctx, id, client)
	
}

func getprices(ctx context.Context, id string, client *mongo.Client) {
	startf := stringtotime(start)	
	for {
		val, err := alpaca.GetSymbolBars(id,alpaca.ListBarParams{Timeframe:"5m"})
		if err != nil {
			fmt.Println(err)	
			time.Sleep(1 * time.Minute)
			continue
		}
		if startf.AddDate(0,0,6) == stringtotime(end) {
			break
		}
		startf = startf.AddDate(0,0,6)

	}
}
func stringtotime(datetime string) time.Time {
	layout := "2006-01-02"
	t, err := time.Parse(layout, datetime)
	if err != nil {
		log.Fatal(err)
	}
	return t
}
