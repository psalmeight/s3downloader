package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// Configuration - replace these with your actual values
	bucket := "hashfleet-data-lake-prod"
	// year := "2025"
	// month := "10"
	// newPrefix := "new_folder/"    // New folder for the uploaded zip
	// zipName := "archive.zip"      // Name of the zip file
	localDir := "/tmp/downloads/" // Local directory to store downloads
	region := "us-west-2"         // Your AWS region

	// Create local directory if it doesn't exist
	if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create local directory: %v", err)
	}

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Unable to load SDK config: %v", err)
	}

	// Create S3 client
	svc := s3.NewFromConfig(cfg)

	// Create downloader and uploader
	//downloader := manager.NewDownloader(svc)
	//uploader := manager.NewUploader(svc)

	// Collect all .json.gz keys recursively
	var keys []string
	collectRecursive(svc, bucket, "miner_data/2025/10/01/00", &keys)

	// Download files concurrently
	//downloadFiles(context.TODO(), downloader, bucket, localDir, keys)

	// Create zip file from downloaded files
	// zipPath := "/tmp/" + zipName
	// if err := createZip(localDir, zipPath); err != nil {
	// 	log.Fatalf("Failed to create zip: %v", err)
	// }

	// Upload the zip file to the new folder in the bucket
	// if err := uploadFile(context.TODO(), uploader, bucket, newPrefix+zipName, zipPath); err != nil {
	// 	log.Fatalf("Failed to upload zip: %v", err)
	// }

	log.Println("Process completed successfully.")
}

// collectRecursive recursively traverses the S3 prefix and collects .json.gz keys
func collectRecursive(svc *s3.Client, bucket, prefix string, keys *[]string) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("Error listing %s: %v", prefix, err)
			return
		}

		for _, cp := range page.CommonPrefixes {
			collectRecursive(svc, bucket, *cp.Prefix, keys)
		}

		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".json.gz") {
				*keys = append(*keys, *obj.Key)

				fmt.Println("Found file:", *obj.Key) // Debug print
			}
		}
	}
}

// downloadFiles downloads the files concurrently using goroutines
func downloadFiles(ctx context.Context, downloader *manager.Downloader, bucket, localDir string, keys []string) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 20) // Limit concurrent downloads to 20

	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Mirror the S3 key structure locally
			filePath := filepath.Join(localDir, key)
			if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
				log.Printf("Failed to create dir for %s: %v", key, err)
				return
			}

			file, err := os.Create(filePath)
			if err != nil {
				log.Printf("Failed to create file %s: %v", filePath, err)
				return
			}
			defer file.Close()

			_, err = downloader.Download(ctx, file, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				log.Printf("Failed to download %s: %v", key, err)
			} else {
				log.Printf("Downloaded %s to %s", key, filePath)
			}
		}(key)
	}

	wg.Wait()
}

// createZip compresses all files in sourceDir into a zip file at zipPath
func createZip(sourceDir, zipPath string) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		w, err := zipWriter.Create(relPath)
		if err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(w, f)
		return err
	})

	return err
}

// uploadFile uploads the file at filePath to S3 at the given key
func uploadFile(ctx context.Context, uploader *manager.Uploader, bucket, key, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	return err
}
