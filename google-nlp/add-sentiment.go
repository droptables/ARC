package main

import (
	"fmt"
    "gopkg.in/mgo.v2"	
    "gopkg.in/mgo.v2/bson"
    "log"
    language "cloud.google.com/go/language/apiv1"
    "golang.org/x/net/context"
    languagepb "google.golang.org/genproto/googleapis/cloud/language/v1"      
    )

type Article struct {
    Title string `json:"title" bson:"title"`
    Description string `json:"description" bson:"description"`  
    Link string  `json:"link" bson:"link"`
    Published string `json:"published" bson:"published"`
    GUID string `json:"id" bson:"_id"`
    Sentiment string `json:"sentiment" bson:"sentiment"`
    Sentimentscore float32 `json:"sentimentscore" bson:"sentimentscore"`
    Sentimentmag float32 `json:"sentimentmag" bson:"sentimentmag"`    
}

var sentimenttype string

func analyzeEntities(ctx context.Context, client *language.Client, text string) (*languagepb.AnalyzeEntitiesResponse, error) {
        return client.AnalyzeEntities(ctx, &languagepb.AnalyzeEntitiesRequest{
                Document: &languagepb.Document{
                        Source: &languagepb.Document_Content{
                                Content: text,
                        },
                        Type: languagepb.Document_PLAIN_TEXT,
                },
                EncodingType: languagepb.EncodingType_UTF8,
        })
}

func analyzeSyntax(ctx context.Context, client *language.Client, text string) (*languagepb.AnnotateTextResponse, error) {
        return client.AnnotateText(ctx, &languagepb.AnnotateTextRequest{
                Document: &languagepb.Document{
                        Source: &languagepb.Document_Content{
                                Content: text,
                        },
                        Type: languagepb.Document_PLAIN_TEXT,
                },
                Features: &languagepb.AnnotateTextRequest_Features{
                        ExtractSyntax: true,
                        ExtractEntities: true,
                        ExtractDocumentSentiment: true,
                },
                EncodingType: languagepb.EncodingType_UTF8,
        })
}



func nlp(ctx context.Context, client *language.Client, text string) {       

        // Detects the sentiment of the text.
        sentiment, err := client.AnalyzeSentiment(ctx, &languagepb.AnalyzeSentimentRequest{
                Document: &languagepb.Document{
                        Source: &languagepb.Document_Content{
                                Content: text,
                        },
                        Type: languagepb.Document_PLAIN_TEXT,
                },
                EncodingType: languagepb.EncodingType_UTF8,
        })
        if err != nil {
                log.Fatalf("Failed to analyze text: %v", err)
        }
        //fmt.Println(sentimenttype)
        if sentiment.DocumentSentiment.Score >= 0 {
                fmt.Println("Sentiment: positive")
                sentimenttype="positive"
        } else {
                fmt.Println("Sentiment: negative")
                sentimenttype="negative"
        }

}

func main() {
   ctx := context.Background()
   client, err := language.NewClient(ctx)
   if err != nil {
           log.Fatalf("Failed to create client: %v", err)
    }  
    session, err := mgo.Dial("localhost")
    if err != nil {
            panic(err)
    }
    defer session.Close()
	session.SetMode(mgo.Monotonic, true)
    //c := session.DB("arc").C("feeds")
    ac := session.DB("arc").C("articles2")

	startcount, err2 := ac.Find(bson.M{}).Count()

	if err2 != nil {
		log.Fatal(err2)
	}

	fmt.Println(startcount)

    var doc Article
    err = ac.Find(bson.M{"_id":"msnbc-host-skewers-claims-clinton-020745433.html"}).One(&doc)
    fmt.Println(doc.Title)
    //nlp(ctx, client)
    //fmt.Println(analyzeEntities(ctx, client, text))
    result, err := analyzeSyntax(ctx, client, doc.Title)
    //fmt.Println(result.Entities)
    for _, v := range result.Entities {
         fmt.Println(v.Name)
    }
    
    // for _, a := range newresults {
    //     colQuerier := bson.M{"_id":a.GUID}
    //     change := bson.M{"$set": bson.M{"title":a.Title, "sentiment":a.Sentiment }}
    //     err := c.Update(colQuerier, change)
    //     if err != nil {
    //         panic(err)
    //     }        
    // }
	// var result []Article
	// err = ac.Find(bson.M{"sentimentscore": nil}).All(&result)
	// if err != nil {
	//     // handle error
	// }
	// for _, v := range result {
	//      fmt.Println(v.Sentiment)
	// }

}	