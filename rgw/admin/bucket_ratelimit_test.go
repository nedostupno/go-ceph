package admin

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testBucketRateLimitReadBytes int64 = 1024
var testBucketRateLimitWriteBytes int64 = 2048

func (suite *RadosGWTestSuite) TestBucketRateLimit() {
	suite.SetupConnection()
	co, err := New(suite.endpoint, suite.accessKey, suite.secretKey, newDebugHTTPClient(http.DefaultClient))
	assert.NoError(suite.T(), err)

	s3, err := newS3Agent(suite.accessKey, suite.secretKey, suite.endpoint, true)
	assert.NoError(suite.T(), err)

	err = s3.createBucket(suite.bucketTestName)
	assert.NoError(suite.T(), err)

	suite.T().Run("set bucket rate limit but no bucket is specified", func(_ *testing.T) {
		err := co.SetBucketRateLimit(context.Background(), RateLimitSpec{})
		assert.Error(suite.T(), err)
		assert.EqualError(suite.T(), err, errMissingBucket.Error())
	})

	suite.T().Run("set bucket rate limit", func(_ *testing.T) {
		err := co.SetBucketRateLimit(context.Background(), RateLimitSpec{
			Bucket:        suite.bucketTestName,
			MaxReadBytes:  &testBucketRateLimitReadBytes,
			MaxWriteBytes: &testBucketRateLimitWriteBytes,
		})
		assert.NoError(suite.T(), err)
	})

	suite.T().Run("get bucket rate limit", func(_ *testing.T) {
		rateLimit, err := co.GetBucketRateLimit(context.Background(), RateLimitSpec{Bucket: suite.bucketTestName})
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), rateLimit)

		assert.Equal(suite.T(), &testBucketRateLimitReadBytes, rateLimit.MaxReadBytes)
		assert.Equal(suite.T(), &testBucketRateLimitWriteBytes, rateLimit.MaxWriteBytes)
	})
}
