package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"sort"
	"strings"
	"time"
	"fmt"
	"net/url"
	"context"

   
	
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var filename = getEnv("SPARKLE_FILENAME", "sparkledb")     // "sparkledb"

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

// SparkleDatabase holds all the sparkle data
type SparkleDatabase struct {
	Sparkles   []Sparkle
	UnSparkles []Sparkle
}

func handleErrors(err error) {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			switch serr.ServiceCode() { // Compare serviceCode to ServiceCodeXxx constants
			case azblob.ServiceCodeContainerAlreadyExists:
				fmt.Println("Received 409. Container already exists")
				return
			}
		}
		log.Fatal(err)
	}
}

// Save the database
func (s *SparkleDatabase) Save() {
	// Persist the database to file
	var data bytes.Buffer
	contents := gob.NewEncoder(&data)
	err := contents.Encode(s)
	if err != nil {
		panic(err)
	}

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create a random string for the quick start container
	containerName := "sparkles"

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Here's how to upload a blob.
	blobURL := containerURL.NewBlockBlobURL(filename)

	ctx := context.Background() // This example uses a never-expiring context

	// The high-level API UploadFileToBlockBlob function uploads blocks in parallel for optimal performance, and can handle large files as well.
	// This function calls PutBlock/PutBlockList for files larger 256 MBs, and calls PutBlob for any file smaller
	fmt.Printf("Uploading the file with blob name: %s\n", filename)
	_, err = azblob.UploadBufferToBlockBlob(ctx, data.Bytes(), blobURL, azblob.UploadToBlockBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	handleErrors(err)

}

// LoadDB loads the SparkleDatabase from S3
func LoadDB() SparkleDatabase {


	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		log.Fatal("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// Create a random string for the quick start container
	containerName := "sparkles"

	URL, _ := url.Parse(
		fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	containerURL := azblob.NewContainerURL(*URL, p)

	// Here's how to download a blob.
	blobURL := containerURL.NewBlobURL("sparkledb")
	ctx := context.Background() // This example uses a never-expiring context
	
	blobProperties, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{})
	handleErrors(err)
	buf := make([]byte, blobProperties.ContentLength())
    err = azblob.DownloadBlobToBuffer(ctx, blobURL, 0, 0, buf, azblob.DownloadFromBlobOptions{
		BlockSize:   4 * 1024 * 1024,
		Parallelism: 16})
	//handleErrors(err)
	if err != nil { 
		log.Println(fmt.Println(err))
	}

	body := bytes.NewReader(buf)
	dec := gob.NewDecoder(body)

	var sparkleDB SparkleDatabase
	err = dec.Decode(&sparkleDB)

	if err != nil {
		log.Print("There was an error loading the sparkle database. Using a blank one.")
		log.Print(err)
	}

	return sparkleDB
	
}

// AddSparkle adds a sparkle to the database and returns a Leader record
func (s *SparkleDatabase) AddSparkle(sparkle Sparkle) Leader {
	// Add a sparkle to the database
	sparkle.Time = time.Now()
	s.Sparkles = append(s.Sparkles, sparkle)

	// After the sparkle has been added, save the data file
	s.Save()

	// Return the receiver record so that Hubot can report the users total sparkles
	var t time.Time
	receivers := s.Receivers(t)
	var recipient Leader
	for _, v := range receivers {
		if v.Name == sparkle.Sparklee {
			recipient = v
		}
	}

	return recipient
}

// UnSparkle adds an unsparkle to the database and returns a Leader record
func (s *SparkleDatabase) UnSparkle(unsparkle Sparkle) Leader {
	unsparkle.Time = time.Now()
	s.UnSparkles = append(s.UnSparkles, unsparkle)

	// After the unsparkle has been added, save the data file
	s.Save()

	// Return the receiver so that Hubot can report the user's total unsparkles
	var t time.Time
	receivers := s.Receivers(t)
	var recipient Leader
	for _, v := range receivers {
		if v.Name == unsparkle.Sparklee {
			recipient = v
		}
	}

	return recipient
}

// Givers returns the top Leaders
func (s *SparkleDatabase) Givers(earliestDate time.Time) []Leader {
	var g = make(map[string]int)
	for _, v := range s.Sparkles {
		if v.Time.After(earliestDate) {
			g[v.Sparkler]++
		}
	}

	var leaders []Leader
	for k, v := range g {
		leader := Leader{Name: k, Score: v}
		leaders = append(leaders, leader)
	}

	return leaders
}

// Receivers returns the top Receivers
func (s *SparkleDatabase) Receivers(earliestDate time.Time) []Leader {
	var g = make(map[string]int)
	for _, v := range s.Sparkles {
		if v.Time.After(earliestDate) {
			g[v.Sparklee]++
		}
	}

	var leaders []Leader
	for k, v := range g {
		leader := Leader{Name: k, Score: v}
		leaders = append(leaders, leader)
	}

	return leaders
}

// ByScore is used for building the leaderboard
type ByScore []Leader

func (a ByScore) Len() int           { return len(a) }
func (a ByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByScore) Less(i, j int) bool { return a[i].Score < a[j].Score }

// TopGiven returns the top Givers
func (s *SparkleDatabase) TopGiven(since time.Time) []Leader {
	leaders := s.Givers(since)
	sort.Sort(sort.Reverse(ByScore(leaders)))
	return leaders
}

// TopReceived returns the top Receivers
func (s *SparkleDatabase) TopReceived(since time.Time) []Leader {
	leaders := s.Receivers(since)
	sort.Sort(sort.Reverse(ByScore(leaders)))
	return leaders
}

// SparklesForUser returns sparkles for user <user>
func (s *SparkleDatabase) SparklesForUser(user string) []Sparkle {
	// Return all the sparkles for <user>
	var list []Sparkle
	for _, v := range s.Sparkles {
		if strings.ToLower(v.Sparklee) == strings.ToLower(user) {
			list = append(list, v)
		}
	}

	var reversed []Sparkle
	for i := len(list) - 1; i >= 0; i-- {
		reversed = append(reversed, list[i])
	}

	return reversed
}

// MigrateSparkles moves all sparkles from <from> to <to>
func (s *SparkleDatabase) MigrateSparkles(from, to string) {
	for k, v := range s.Sparkles {
		if strings.ToLower(v.Sparklee) == strings.ToLower(from) {
			s.Sparkles[k].Sparklee = to
		}
	}

	s.Save()
}
