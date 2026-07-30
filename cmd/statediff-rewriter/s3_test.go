package main

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func validS3ExpressGetOutput() *s3.GetObjectOutput {
	return &s3.GetObjectOutput{
		StorageClass:         s3types.StorageClassExpressOnezone,
		ServerSideEncryption: s3types.ServerSideEncryptionAes256,
		Expiration:           aws.String("NotImplemented"),
	}
}

func TestValidateS3ExpressObject(t *testing.T) {
	require.NoError(t, validateS3ExpressObject(validS3ExpressGetOutput()))

	invalidClass := validS3ExpressGetOutput()
	invalidClass.StorageClass = s3types.StorageClassStandard
	require.ErrorContains(t, validateS3ExpressObject(invalidClass), "storage class")

	withTags := validS3ExpressGetOutput()
	withTags.TagCount = aws.Int32(1)
	require.ErrorContains(t, validateS3ExpressObject(withTags), "unsupported metadata")

	withKMS := validS3ExpressGetOutput()
	withKMS.SSEKMSKeyId = aws.String("key")
	require.ErrorContains(t, validateS3ExpressObject(withKMS), "unsupported metadata")

	withExpiration := validS3ExpressGetOutput()
	withExpiration.Expiration = aws.String("expiry-date=tomorrow")
	require.ErrorContains(t, validateS3ExpressObject(withExpiration), "object expiration")
}

func TestCRC32CBase64(t *testing.T) {
	require.Equal(t, "4waSgw==", crc32cBase64([]byte("123456789")))
}

func TestValidatePutObjectResult(t *testing.T) {
	checksum := crc32cBase64([]byte("body"))
	result, err := validatePutObjectResult(&s3.PutObjectOutput{
		ChecksumCRC32C: aws.String(checksum),
		ETag:           aws.String("etag-new"),
	}, checksum)
	require.NoError(t, err)
	require.Equal(t, "etag-new", result.ETag)

	_, err = validatePutObjectResult(nil, checksum)
	require.ErrorContains(t, err, "empty")
	_, err = validatePutObjectResult(&s3.PutObjectOutput{ETag: aws.String("etag-new")}, checksum)
	require.ErrorContains(t, err, "CRC32C")
	_, err = validatePutObjectResult(&s3.PutObjectOutput{
		ChecksumCRC32C: aws.String("different"), ETag: aws.String("etag-new"),
	}, checksum)
	require.ErrorContains(t, err, "CRC32C")
	_, err = validatePutObjectResult(&s3.PutObjectOutput{ChecksumCRC32C: aws.String(checksum)}, checksum)
	require.ErrorContains(t, err, "ETag")
}

func TestClassifyPutObjectError(t *testing.T) {
	responseError := func(status int) error {
		return &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      errors.New("request failed"),
		}
	}
	for _, status := range []int{http.StatusConflict, http.StatusPreconditionFailed} {
		require.ErrorIs(t, classifyPutObjectError(responseError(status)), errObjectConflict)
	}
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		require.ErrorIs(t, classifyPutObjectError(responseError(status)), errObjectWriteUncertain)
	}
	noResponse := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 0}},
		Err:      io.EOF,
	}
	require.ErrorIs(t, classifyPutObjectError(noResponse), errObjectWriteUncertain)
	definite := responseError(http.StatusForbidden)
	require.Same(t, definite, classifyPutObjectError(definite))
	require.ErrorIs(t, classifyPutObjectError(io.EOF), errObjectWriteUncertain)
	require.ErrorIs(t, classifyPutObjectError(errors.New("connection reset")), errObjectWriteUncertain)
}
