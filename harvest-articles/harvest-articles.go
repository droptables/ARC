package main

import (
	"fmt"
    "gopkg.in/mgo.v2"	
    "gopkg.in/mgo.v2/bson"
    "log"
    "github.com/mmcdole/gofeed"
    "time"
    //"math/rand"
    )

type Article struct {
    Title string `json:"title" bson:"title"`
    Description string `json:"description" bson:"description"`  
    Link string  `json:"link" bson:"link"`
    Published string `json:"published" bson:"published"`
    GUID string `json:"id" bson:"_id"`
}

type Feed struct {
    GUID string `json:"id" bson:"_id"`
    Domain string `json:"domain" bson:"domain"` 
}


func harvest_rss_feed(feedurl string, badfeedCH chan string,articledocCH chan Article, chFinished chan bool){
    //r := rand.Intn(10)    
    //time.Sleep(time.Duration(r) * time.Microsecond)
    defer func() {
        chFinished <- true
    }()
    session, err := mgo.Dial("localhost")
    if err != nil {
            panic(err)
    }
    defer session.Close()
    session.SetMode(mgo.Monotonic, true)
    ac := session.DB("arc").C("articles3")

    fp := gofeed.NewParser()
    feed, _ := fp.ParseURL(feedurl)
    if feed == nil {
        badfeedCH <- feedurl
        return
    }

    //fmt.Println(feedurl)
    for _, article := range feed.Items {
        //fmt.Println(article.Title)
        v := Article{article.Title, article.Description, article.Link, article.Published, article.GUID}         
        ac.Insert(v)
        articledocCH <-v
     }       

}


func main() {
    start := time.Now()

    articledocCH := make(chan Article)
    badfeedCH := make(chan string)    
    chFinished := make(chan bool)
    badUrls := make(map[string]bool)
    goodDocs := make(map[string]bool)

    session, err := mgo.Dial("localhost")
    if err != nil {
            panic(err)
    }
    defer session.Close()
	session.SetMode(mgo.Monotonic, true)
    c := session.DB("arc").C("feeds")
    ac := session.DB("arc").C("articles3")

	startcount, err2 := ac.Find(bson.M{}).Count()

	if err2 != nil {
		log.Fatal(err2)
	}

    var feeds []Feed

    //get all the feed records from the collection
    err = c.Find(bson.M{}).All(&feeds)
    if err != nil {
        panic(err)
    }

    fmt.Println("Total RSS Feeds: ", len(feeds))

    for _, a := range feeds {
        go harvest_rss_feed(a.GUID, badfeedCH, articledocCH, chFinished)
    }

    // Subscribe channels
    for c := 0; c < len(feeds); {
        select {

        case articledoc := <-articledocCH:
            goodDocs[articledoc.GUID]=true
            //fmt.Println(articledoc.Title)

        case badfeedurl := <-badfeedCH:
            badUrls[badfeedurl]=false
            fmt.Println(badfeedurl)

        case <-chFinished:
            //fmt.Println(c)
            c++
        }
    }

    close(articledocCH)
    close(badfeedCH)
    close(chFinished)

    fmt.Println("-------Finished-------")
    ac = session.DB("arc").C("articles3") 
    endcount, err2 := ac.Find(bson.M{}).Count()
    if err2 != nil {
        log.Fatal(err2)
    }
    t := time.Now()
    elapsed := t.Sub(start)

    fmt.Printf("Articles In DB Before Crawl: %d\n", startcount)
    fmt.Printf("Articles In DB After Crawl: %d\n", endcount)  
    fmt.Printf("New Articles: %d\n", endcount-startcount)
    fmt.Printf("Failed Fetches: %d\n", len(badUrls))
    fmt.Printf("Articles Found: %d\n", len(goodDocs))    
    fmt.Printf("Elapsed Time: %d\n", elapsed/1000000000)

}