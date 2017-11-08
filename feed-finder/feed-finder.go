package main

import (
	"fmt"
	"golang.org/x/net/html"
	"net/http"
	"os"
	"strings"
	"github.com/mmcdole/gofeed"
	"net/url"
    "gopkg.in/mgo.v2"
    "log"
    "gopkg.in/mgo.v2/bson"	

)

type Feed struct {
	GUID string	`json:"id" bson:"_id"`
	Domain string `json:"domain" bson:"domain"`	
}

//pull the href attribute from a Token
func getHref(t html.Token) (ok bool, href string) {
	// Iterate over all of the Token's attributes until we find an "href"
	for _, a := range t.Attr {
		if a.Key == "href" {
			href = a.Val
			ok = true
		}
	}
	return
}

// Extract all http** links from a given webpage
func crawl(url string, chFoundurl chan string, failCh chan string ,chFinished chan bool) {
	resp, err := http.Get(url)

	defer func() {
		// Notify that we're done after this function
		chFinished <- true
	}()

	if err != nil {
		fmt.Println("ERROR: Failed to crawl \"" + url + "\"")
		return
	}

	b := resp.Body
	defer b.Close() // close Body when the function returns
	z := html.NewTokenizer(b)

	for {
		tt := z.Next()

		switch {

		case tt == html.ErrorToken:
			// End of the document, we're done
			return

		case tt == html.StartTagToken:
			t := z.Token()
			// Check if the token is an <a> tag
			isAnchor := t.Data == "a"
			if !isAnchor {
				continue
			}

			// Extract the href value, if there is one
			ok, url := getHref(t)
			if !ok {
				continue
			}

			// Make sure the url begines in http
			hasProto := strings.Index(url, "http") == 0

			switch (hasProto) {

				case true:
					chFoundurl <- url

				case false:
					failCh <- url

			}
		}
	}
}


func check_rss_feed(feedurl string, badCH chan string,goodCH chan string  ,chFinished chan bool){

	defer func() {
		// Notify that we're done after this function
		chFinished <- true
	}()

	//use gofeed package to parse as RSS
	fp := gofeed.NewParser()
	feed, _ := fp.ParseURL(feedurl)
	if feed == nil {
		badCH <- feedurl
		return
	}

	goodCH <- feedurl
		 
}


func main() {

	//Connect to MongoDB and get Collection
    session, err := mgo.Dial("localhost")
    if err != nil {
            panic(err)
    }
    defer session.Close()
	session.SetMode(mgo.Monotonic, true)
    c := session.DB("arc").C("feeds")
    
    //get the amount of feeds already in the collection
	startcount, err2 := c.Find(bson.M{}).Count()
	if err2 != nil {
		log.Fatal(err2)
	}	

	//these maps will store the URLs found from the initial crawl,, 
	foundUrls := make(map[string]bool)
	badUrls := make(map[string]bool)//URLS that are not RSS Feeds
	goodUrls := make(map[string]bool)//URLS that are RSS Feeds
	seedUrls := os.Args[1:] //starting urls to begin concurrent crawl

	// Channels
	chFoundurl  := make(chan string)//catches links that start with http* found by the crawler 
	badCH := make(chan string)//catches links that are not RSS
	goodCH := make(chan string)//catches good RSS Feeds found
	failCh := make(chan string)//catches crawled links that do not start with http*
	chFinished := make(chan bool)//tell routines we're done

	// Launch the crawl process (concurrently)
	for _, url := range seedUrls {
		go crawl(url, chFoundurl, failCh, chFinished)
	}

	// Subscribe to the channels
	for c := 0; c < len(seedUrls); {

		select {

		case foundurl := <-chFoundurl:
			foundUrls[foundurl]=true

		case failurl := <-failCh:
			//fmt.Println("This URL failed: "+failurl)
			badUrls[failurl]=true
			
		case <-chFinished:
			c++
		}
	}
	fmt.Println("Done crawling for HTTP links")
	close(chFoundurl)
	close(failCh)

	//check the found urls if they are valid RSS feeds
	for url, _ := range foundUrls {
		go check_rss_feed(url,badCH, goodCH, chFinished)
	}

	for c := 0; c < len(foundUrls); {

		select {

			case badresult := <- badCH:
				badUrls[badresult]=false

			case goodresult := <- goodCH:
				goodUrls[goodresult]=true

			case <-chFinished:
				c++				
		}
	}

	fmt.Println("Done checking HTTP Links if valid RSS")

	fmt.Println("Inserting to mongo")
	for goodurl, _ := range goodUrls {

	    u, err := url.Parse(goodurl)
	    if err != nil {
	        panic(err)
	    }		
		feedrecord := Feed{goodurl, u.Host}
		c.Insert(feedrecord)

	}

	close(badCH)
	close(goodCH)

	fmt.Println("-------Finished-------")
	endcount, err2 := c.Find(bson.M{}).Count()
	if err2 != nil {
		log.Fatal(err2)
	}
	fmt.Printf("Feeds In DB Before Crawl: %d\n", startcount)
	fmt.Printf("Starting URLS: %d\n", len(seedUrls))
	fmt.Printf("Found URLs: %d\n", len(foundUrls))	
	fmt.Printf("Succesfull Fetch Of RSS Feeds: %d\n", len(goodUrls))
	fmt.Printf("Failed Fetch Of RSS Feeds: %d\n", len(badUrls))		
	fmt.Printf("Feeds In DB After Crawl: %d\n", endcount)


}