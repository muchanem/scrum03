package main

import (
        "archive/tar"
        "bufio"
        "bytes"
        "compress/bzip2"
        "context"
        "encoding/json"
        "fmt"
        "go.mongodb.org/mongo-driver/mongo"
        "go.mongodb.org/mongo-driver/mongo/options"
        "io"
        "log"
        "os"
        "strings"
		"time"
		"sync"
)

//var tweetcount int

func main() {
        //fmt.Println(os.Args[1])

        client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://myUserAdmin:scrum3ncssm@138.197.6.239:27017"))
        if err != nil {
                log.Fatal(err)
        }
        ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
        err = client.Connect(ctx)
        if err != nil {
                log.Fatal(err)
        }
        defer client.Disconnect(ctx)
        dir, err := os.Open(os.Args[1])
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
        }
        files, err := dir.Readdirnames(0)
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
		}
		var wg sync.WaitGroup
		for _, f := range files {
			if !(strings.Contains(f, ".tar")) {
				continue
			}
			wg.Add(1)
			go worker(f, &wg, (os.Args[1] + "/" + f), client)
		}
		wg.Wait()
        dir.Close()
}
func worker(id string, wg *sync.WaitGroup, tarb string, client *mongo.Client) {
	defer wg.Done()
	fmt.Println("Starting Worker:" + id)
	readfiles(tarb, client)
}
func readfiles(tarb string, client *mongo.Client) error {
        reader, err := os.Open(tarb)
        if err != nil {
                log.Fatal(err)
        }
        tr := tar.NewReader(reader)
        for {
                header, err := tr.Next()

                switch {
                case err == io.EOF:
                        return nil
                case err != nil:
                        return err
                case header == nil:
                        continue
                }
                switch header.Typeflag {
                case tar.TypeDir:
                        continue

				case tar.TypeReg:
					bz := bzip2.NewReader(tr)
					scanner := bufio.NewScanner(bz)
					for scanner.Scan() {
							if bytes.Contains(scanner.Bytes(), []byte("{\"retweeted_status\":")) {
									continue
							}
			
							if bytes.Contains(scanner.Bytes(), []byte("{\"delete\":")) {
									continue
							}
							tw := tojsontweet(scanner.Bytes())
							c := containssymbol(tw.Body)
							if c.contains {
									sendocument(tw, c.symbol, client, tarb)
							}
					}
					if err := scanner.Err(); err != nil {
							fmt.Fprintln(os.Stderr, "error:", err)
					}
			
				}
        }

}

type tweet struct {
        Date      string `json:"date,omitempty"`
        Body      string `json:"body,omitempty"`
        Uid       string `json:"uid,omitempty"`
        Userid    string `json:"userid,omitempty"`
        Username  string `json:"username,omitempty"`
        Followers int    `json:"followers,omitempty"`
        Rt        int    `json:"rt,omitempty"`
        Likes     int    `json:"likes,omitempty"`
        Verified  bool   `json:"verified,omitempty"`
}

type containsandsymbol struct {
        contains bool
        symbol   string
}

func tojsontweet(line []byte) tweet {
        var tw tweet
        var result map[string]interface{}
        json.Unmarshal(line, &result)
                user, err := result["user"].(map[string]interface{})
                if err != nil {
                        
                }
        tw = tweet{
                Date:      result["created_at"].(string),
                Body:      result["text"].(string),
                Uid:       result["id_str"].(string),
                Userid:    user["id_str"].(string),
                Username:  user["screen_name"].(string),
                Followers: int(user["followers_count"].(float64)),
                Rt:        int(result["retweet_count"].(float64)),
                Likes:     int(result["favorite_count"].(float64)),
                Verified:  user["verified"].(bool)}
        return tw
}

func sendocument(tw tweet, stock string, client *mongo.Client, tarb string) {
		//tweetcount++	
		coll := client.Database("twitter").Collection(stock)
        _, err := coll.InsertOne(context.TODO(), tw)
        if err != nil {
                log.Fatal(err)
		}
		fmt.Println("sent " + stock + " " + tarb)
        //fmt.Printf("sent %d \n", tweetcount)
}
func containssymbol(body string) containsandsymbol {
        stocks := []string{"UNH", "ABB", "MSFT", "RDS-B", "UN", "BUD", "KO", "CAT", "JPM", "DUK", "HD", "PFE", "BSAC", "C", "TM", "FB", "PPL", "MCD", "CHL", "PTR", "UTX", "PCG", "MMM", "CMCSA", "PG", "WFC", "PICO", "MRK", "JNJ", "DIS", "BAC", "NVS", "AMZN", "VZ", "NEE", "BP", "BA", "CHTR", "TOT", "ABBV", "BRK-A", "REX", "CODI", "UL", "SNP", "SLB", "BBL", "GOOG", "V", "TSM", "LMT", "AMGN", "GD", "WMT", "INTC", "NGG", "DHR", "MA", "AGFS", "SO", "IEP", "SRE", "GE", "AAPL", "PEP", "MDT", "T", "EXC", "BABA", "PM", "ORCL", "HON", "D", "BHP", "MO", "HRG", "XOM", "SPLP", "SNY", "CSCO", "PCLN", "CVX", "AEP", "UPS", "CELG", "BCH", "HSBC"}
        for _, stock := range stocks {
                if strings.Contains(body, ("$" + stock)) {
                        return containsandsymbol{contains: true, symbol: stock}
                }
        }
        return containsandsymbol{false, ""}
}