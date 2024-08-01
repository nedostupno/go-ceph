package admin

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testGlobalMaxReadBytes  int64 = 10000
	testGlobalMaxWriteBytes int64 = 5000
)

func (suite *RadosGWTestSuite) TestGlobalRateLimit() {
	suite.SetupConnection()
	co, err := New(suite.endpoint, suite.accessKey, suite.secretKey, newDebugHTTPClient(http.DefaultClient))
	assert.NoError(suite.T(), err)

	suite.T().Run("get global rate limit", func(_ *testing.T) {
		_, err := co.GetGlobalRateLimit(context.Background())
		assert.NoError(suite.T(), err)
	})

	suite.T().Run("set global user rate limit without global flag", func(_ *testing.T) {
		err := co.SetGlobalUserRateLimit(context.Background(), RateLimitSpec{Scope: "user"})
		assert.Error(suite.T(), err)
		assert.EqualError(suite.T(), err, "global must be true for global user rate limit")
	})

	suite.T().Run("set global user rate limit", func(_ *testing.T) {
		global := true
		err := co.SetGlobalUserRateLimit(context.Background(), RateLimitSpec{
			Global:        &global,
			MaxReadBytes:  &testGlobalMaxReadBytes,
			MaxWriteBytes: &testGlobalMaxWriteBytes,
		})
		assert.NoError(suite.T(), err)
	})

	suite.T().Run("set global bucket rate limit without global flag", func(_ *testing.T) {
		err := co.SetGlobalBucketRateLimit(context.Background(), RateLimitSpec{Scope: "bucket"})
		assert.Error(suite.T(), err)
		assert.EqualError(suite.T(), err, "global must be true for global bucket rate limit")
	})

	suite.T().Run("set global bucket rate limit", func(_ *testing.T) {
		global := true
		err := co.SetGlobalBucketRateLimit(context.Background(), RateLimitSpec{
			Global:        &global,
			MaxReadBytes:  &testGlobalMaxReadBytes,
			MaxWriteBytes: &testGlobalMaxWriteBytes,
		})
		assert.NoError(suite.T(), err)
	})

	suite.T().Run("set global anonymous rate limit without global flag", func(_ *testing.T) {
		err := co.SetGlobalAnonymousRateLimit(context.Background(), RateLimitSpec{Scope: "anon"})
		assert.Error(suite.T(), err)
		assert.EqualError(suite.T(), err, "global must be true for global anonymous rate limit")
	})

	suite.T().Run("set global anonymous rate limit", func(_ *testing.T) {
		global := true
		err := co.SetGlobalAnonymousRateLimit(context.Background(), RateLimitSpec{
			Global:        &global,
			MaxReadBytes:  &testGlobalMaxReadBytes,
			MaxWriteBytes: &testGlobalMaxWriteBytes,
		})
		assert.NoError(suite.T(), err)
	})
}
