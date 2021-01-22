package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
)

type tweetbefore struct {
	ID   primitive.ObjectID `json:"_id,omitempty"`
	Date string             `json:"date,omitempty"`
}
type tweetafter struct {
	ID   primitive.ObjectID `json:"_id,omitempty"`
	Date primitive.DateTime `json:"date,omitempty"`
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI(""))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)
	db := client.Database("twitter")
	s, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	filter := bson.M{"date": bson.M{"$type": "string"}}
	for _, v := range s {
		wg.Add(1)
		go worker(ctx, v, &wg, client, filter)
	}
	wg.Wait()

}
func worker(ctx context.Context, id string, wg *sync.WaitGroup, client *mongo.Client, filter bson.M) {
	defer wg.Done()
	fmt.Println("Starting Worker: " + id)
	loopandreplace(ctx, id, client, filter)
}
func loopandreplace(ctx context.Context, stock string, client *mongo.Client, filter bson.M) {
	coll := client.Database("twitter").Collection(stock)
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	var results []tweetbefore
	if err := cursor.All(ctx, &results); err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup	
	for _, v := range results {
		wg.Add(1)	
		go worker1(ctx, tweetafter{ID: v.ID, Date: stringtotime(v.Date)}, coll, &wg)
	}
	wg.Wait()
}
func worker1(ctx context.Context, tweet tweetafter, coll *mongo.Collection, wg *sync.WaitGroup) {
	defer wg.Done()
	sendwithdatetime(ctx, tweet, coll)
}
func stringtotime(datetime string) primitive.DateTime {
	layout := "Mon Jan 02 15:04:05 -0700 2006"
	t, err := time.Parse(layout, datetime)
	if err != nil {
		log.Fatal(err)
	}
	return primitive.NewDateTimeFromTime(t)
}
func sendwithdatetime(ctx context.Context, tweet tweetafter, coll *mongo.Collection) {
//	filter := bson.M{"_id": tweet.ID}
	
//	update := bson.M{"date": tweet.Date}
	fmt.Println(tweet)	
	//_, err := coll.UpdateOne(ctx, filter, update)
	//if err != nil {
		//log.Fatal(err)
	//}
}
