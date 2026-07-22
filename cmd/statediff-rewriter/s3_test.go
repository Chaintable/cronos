package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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
