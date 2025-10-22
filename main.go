package main

import (
	"compress/gzip"
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
	bucket := "hashfleet-data-lake-prod"
	localDir := "./downloads/"
	region := "us-east-2"

	if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create local directory: %v", err)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		log.Fatalf("Unable to load SDK config: %v", err)
	}

	svc := s3.NewFromConfig(cfg)

	downloader := manager.NewDownloader(svc)
	var keys []string
	collectRecursive(svc, bucket, "miner_data/2025/10/20/13", &keys)
	downloadFiles(context.TODO(), downloader, bucket, localDir, keys)

	log.Println("Decompressing .json.gz files...")
	if err := decompressGzipFiles(localDir); err != nil {
		log.Fatalf("Failed to decompress files: %v", err)
	}
}

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

func decompressGzipFiles(rootDir string) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, ".json.gz") {
			return nil
		}

		outputPath := strings.TrimSuffix(path, ".gz")

		gzFile, err := os.Open(path)
		if err != nil {
			log.Printf("Failed to open %s: %v", path, err)
			return nil
		}
		defer gzFile.Close()

		gzReader, err := gzip.NewReader(gzFile)
		if err != nil {
			log.Printf("Failed to create gzip reader for %s: %v", path, err)
			return nil
		}
		defer gzReader.Close()

		outFile, err := os.Create(outputPath)
		if err != nil {
			log.Printf("Failed to create output file %s: %v", outputPath, err)
			return nil
		}
		defer outFile.Close()

		_, err = io.Copy(outFile, gzReader)
		if err != nil {
			log.Printf("Failed to decompress %s to %s: %v", path, outputPath, err)
			return nil
		}

		log.Printf("Decompressed %s to %s", path, outputPath)

		if err := os.Remove(path); err != nil {
			log.Printf("Warning: Failed to remove original file %s: %v", path, err)
		}

		return nil
	})
}
