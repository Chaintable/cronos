package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var (
	errObjectConflict       = errors.New("object precondition conflict")
	errObjectWriteUncertain = errors.New("object write result is uncertain")
	crc32cTable             = crc32.MakeTable(crc32.Castagnoli)
)

type storedObject struct {
	Body    []byte
	ETag    string
	Headers objectHeaders
}

type putResult struct {
	ETag string
}

type objectStore interface {
	Get(context.Context, string, string) (storedObject, error)
	Put(context.Context, string, string, []byte, string, objectHeaders) (putResult, error)
}

type s3ObjectStore struct {
	readClient            *s3.Client
	writeClient           *s3.Client
	validateGetAttributes bool
}

func newS3ObjectStore(ctx context.Context, region string, concurrency int) (*s3ObjectStore, error) {
	return newS3ObjectStoreWithValidation(ctx, region, concurrency, true)
}

func newRefillS3ObjectStore(ctx context.Context, region string, concurrency int) (*s3ObjectStore, error) {
	return newS3ObjectStoreWithValidation(ctx, region, concurrency, false)
}

func newS3ObjectStoreWithValidation(
	ctx context.Context,
	region string,
	concurrency int,
	validateGetAttributes bool,
) (*s3ObjectStore, error) {
	if concurrency < 1 || concurrency > maximumObjectConcurrency {
		return nil, fmt.Errorf("S3 concurrency must be between 1 and %d", maximumObjectConcurrency)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = concurrency * 2
	transport.MaxIdleConnsPerHost = concurrency
	transport.MaxConnsPerHost = concurrency
	httpClient := &http.Client{Transport: transport}
	awsConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	readClient := s3.NewFromConfig(awsConfig)
	writeClient := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		// A conditional PUT must not be retried invisibly: the first attempt can commit
		// while its response is lost, and a retry with the old ETag then returns 412.
		options.Retryer = retry.NewStandard(func(standard *retry.StandardOptions) {
			standard.MaxAttempts = 1
		})
	})
	return &s3ObjectStore{
		readClient: readClient, writeClient: writeClient,
		validateGetAttributes: validateGetAttributes,
	}, nil
}

func (s *s3ObjectStore) Get(ctx context.Context, bucket, key string) (storedObject, error) {
	output, err := s.readClient.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
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
	if s.validateGetAttributes {
		if err := validateS3ExpressObject(output); err != nil {
			return storedObject{}, fmt.Errorf("object %s has unsupported attributes: %w", key, err)
		}
	}
	if aws.ToString(output.ETag) == "" {
		return storedObject{}, fmt.Errorf("object %s has no ETag", key)
	}
	return storedObject{
		Body: body, ETag: aws.ToString(output.ETag),
		Headers: objectHeaders{
			Metadata: metadata, CacheControl: aws.ToString(output.CacheControl),
			ContentDisposition: aws.ToString(output.ContentDisposition), ContentEncoding: aws.ToString(output.ContentEncoding),
			ContentLanguage: aws.ToString(output.ContentLanguage), ContentType: aws.ToString(output.ContentType),
			WebsiteRedirect: aws.ToString(output.WebsiteRedirectLocation),
			StorageClass:    string(output.StorageClass), ServerSideEncryption: string(output.ServerSideEncryption),
		},
	}, nil
}

func validateS3ExpressObject(output *s3.GetObjectOutput) error {
	if output.StorageClass != s3types.StorageClassExpressOnezone {
		return fmt.Errorf("storage class is %q, want %q", output.StorageClass, s3types.StorageClassExpressOnezone)
	}
	if output.ServerSideEncryption != s3types.ServerSideEncryptionAes256 {
		return fmt.Errorf("server-side encryption is %q, want %q", output.ServerSideEncryption, s3types.ServerSideEncryptionAes256)
	}
	if output.BucketKeyEnabled != nil || output.DeleteMarker != nil || aws.ToInt32(output.MissingMeta) != 0 || output.ObjectLockLegalHoldStatus != "" ||
		output.ObjectLockMode != "" || output.ObjectLockRetainUntilDate != nil || output.ReplicationStatus != "" ||
		output.RequestCharged != "" || output.Restore != nil || output.SSECustomerAlgorithm != nil ||
		output.SSECustomerKeyMD5 != nil || output.SSEKMSKeyId != nil || aws.ToInt32(output.TagCount) != 0 ||
		output.VersionId != nil || output.WebsiteRedirectLocation != nil || output.ExpiresString != nil {
		return fmt.Errorf("directory-bucket-unsupported metadata is present")
	}
	if output.Expiration != nil && aws.ToString(output.Expiration) != "NotImplemented" {
		return fmt.Errorf("object expiration is %q", aws.ToString(output.Expiration))
	}
	return nil
}

func (s *s3ObjectStore) Put(ctx context.Context, bucket, key string, body []byte, ifMatch string, headers objectHeaders) (putResult, error) {
	metadata, err := decodeMetadata(headers.Metadata)
	if err != nil {
		return putResult{}, err
	}
	checksum := crc32cBase64(body)
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body), IfMatch: aws.String(ifMatch),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32c, ChecksumCRC32C: aws.String(checksum), Metadata: metadata,
		StorageClass:         s3types.StorageClass(headers.StorageClass),
		ServerSideEncryption: s3types.ServerSideEncryption(headers.ServerSideEncryption),
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
	output, err := s.writeClient.PutObject(ctx, input)
	if err == nil {
		result, validateErr := validatePutObjectResult(output, checksum)
		if validateErr != nil {
			return putResult{}, fmt.Errorf("%w: %w", errObjectWriteUncertain, validateErr)
		}
		return result, nil
	}
	return putResult{}, classifyPutObjectError(err)
}

func classifyPutObjectError(err error) error {
	var responseError *smithyhttp.ResponseError
	if errors.As(err, &responseError) {
		status := responseError.HTTPStatusCode()
		switch {
		case status == http.StatusConflict || status == http.StatusPreconditionFailed:
			return fmt.Errorf("%w: HTTP %d: %w", errObjectConflict, status, err)
		case status == 0 || status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError:
			return fmt.Errorf("%w: HTTP %d: %w", errObjectWriteUncertain, status, err)
		default:
			return err
		}
	}
	return fmt.Errorf("%w: %w", errObjectWriteUncertain, err)
}

func crc32cBase64(body []byte) string {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], crc32.Checksum(body, crc32cTable))
	return base64.StdEncoding.EncodeToString(encoded[:])
}

func validatePutObjectResult(output *s3.PutObjectOutput, expectedChecksum string) (putResult, error) {
	if output == nil {
		return putResult{}, fmt.Errorf("PUT response is empty")
	}
	if aws.ToString(output.ChecksumCRC32C) != expectedChecksum {
		return putResult{}, fmt.Errorf("PUT response CRC32C is %q, want %q", aws.ToString(output.ChecksumCRC32C), expectedChecksum)
	}
	if aws.ToString(output.ETag) == "" {
		return putResult{}, fmt.Errorf("PUT response has no ETag")
	}
	return putResult{ETag: aws.ToString(output.ETag)}, nil
}
