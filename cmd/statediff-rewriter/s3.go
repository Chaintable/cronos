package main

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type storedObject struct {
	Body    []byte
	ETag    string
	Headers objectHeaders
}

type objectStore interface {
	Get(context.Context, string, string) (storedObject, error)
	Put(context.Context, string, string, []byte, string, objectHeaders) error
}

type s3ObjectStore struct{ client *s3.Client }

func newS3ObjectStore(ctx context.Context, region string) (*s3ObjectStore, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return &s3ObjectStore{client: s3.NewFromConfig(awsConfig)}, nil
}

func (s *s3ObjectStore) Get(ctx context.Context, bucket, key string) (storedObject, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return storedObject{}, err
	}
	defer output.Body.Close()
	body, err := io.ReadAll(io.LimitReader(output.Body, maxObjectSize+1))
	if err != nil {
		return storedObject{}, err
	}
	if int64(len(body)) > maxObjectSize {
		return storedObject{}, fmt.Errorf("object %s exceeds %d bytes", key, maxObjectSize)
	}
	metadata, err := canonicalMetadata(output.Metadata)
	if err != nil {
		return storedObject{}, err
	}
	return storedObject{
		Body: body, ETag: aws.ToString(output.ETag),
		Headers: objectHeaders{
			Metadata: metadata, CacheControl: aws.ToString(output.CacheControl),
			ContentDisposition: aws.ToString(output.ContentDisposition), ContentEncoding: aws.ToString(output.ContentEncoding),
			ContentLanguage: aws.ToString(output.ContentLanguage), ContentType: aws.ToString(output.ContentType),
			WebsiteRedirect: aws.ToString(output.WebsiteRedirectLocation),
		},
	}, nil
}

func (s *s3ObjectStore) Put(ctx context.Context, bucket, key string, body []byte, ifMatch string, headers objectHeaders) error {
	metadata, err := decodeMetadata(headers.Metadata)
	if err != nil {
		return err
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), IfMatch: aws.String(ifMatch),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32c, Metadata: metadata,
	}
	setOptionalString := func(value string, target **string) {
		if value != "" {
			*target = aws.String(value)
		}
	}
	setOptionalString(headers.CacheControl, &input.CacheControl)
	setOptionalString(headers.ContentDisposition, &input.ContentDisposition)
	setOptionalString(headers.ContentEncoding, &input.ContentEncoding)
	setOptionalString(headers.ContentLanguage, &input.ContentLanguage)
	setOptionalString(headers.ContentType, &input.ContentType)
	setOptionalString(headers.WebsiteRedirect, &input.WebsiteRedirectLocation)
	_, err = s.client.PutObject(ctx, input)
	return err
}
