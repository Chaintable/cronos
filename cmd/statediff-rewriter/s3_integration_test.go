package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func benchmarkEnvironmentInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	require.NoError(t, err)
	require.Positive(t, parsed)
	return parsed
}

// TestProductionS3ReadThroughput is opt-in because it performs real, read-only
// requests against the sealed manifest's production bucket. It never writes S3.
func TestProductionS3ReadThroughput(t *testing.T) {
	if os.Getenv("STATEDIFF_S3_BENCHMARK") != "1" {
		t.Skip("set STATEDIFF_S3_BENCHMARK=1 to run the production read-only gate")
	}
	objectCount := benchmarkEnvironmentInt(t, "S3_BENCHMARK_OBJECTS", 100000)
	concurrency := benchmarkEnvironmentInt(t, "S3_BENCHMARK_CONCURRENCY", defaultObjectConcurrency)
	target := benchmarkEnvironmentInt(t, "S3_BENCHMARK_TARGET", 1000)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	store, err := newS3ObjectStore(ctx, defaultRegion, concurrency)
	require.NoError(t, err)

	keys := make([]string, 0, objectCount)
	paginator := s3.NewListObjectsV2Paginator(store.readClient, &s3.ListObjectsV2Input{
		Bucket: aws.String(defaultBucket), Prefix: aws.String(defaultPrefix + "/"),
	})
	for len(keys) < objectCount && paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		require.NoError(t, err)
		for _, object := range page.Contents {
			if object.Key != nil && strings.HasSuffix(*object.Key, "/stateDiff") {
				keys = append(keys, *object.Key)
				if len(keys) == objectCount {
					break
				}
			}
		}
	}
	require.Len(t, keys, objectCount)

	started := time.Now()
	var totalBytes atomic.Int64
	err = runOrderedPipeline(ctx, 1, concurrency, concurrency*4,
		func(_ context.Context, emit func(uint64, string) error) error {
			for index, key := range keys {
				if err := emit(uint64(index+1), key); err != nil {
					return err
				}
			}
			return nil
		},
		func(workerCtx context.Context, key string) (int, error) {
			object, err := store.Get(workerCtx, defaultBucket, key)
			if err != nil {
				return 0, err
			}
			return len(object.Body), nil
		},
		func(_ uint64, size int) error {
			totalBytes.Add(int64(size))
			return nil
		},
	)
	require.NoError(t, err)
	duration := time.Since(started)
	rate := float64(objectCount) / duration.Seconds()
	t.Logf("objects=%d concurrency=%d bytes=%d duration=%s rate=%.2f objects/s", objectCount, concurrency, totalBytes.Load(), duration, rate)
	require.GreaterOrEqual(t, rate, float64(target))
}
