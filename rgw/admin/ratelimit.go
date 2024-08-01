package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// RateLimitSpec describes rate limits for a user, bucket, or global setting
type RateLimitSpec struct {
	UID           string `json:"uid,omitempty" url:"uid,omitempty"`
	Bucket        string `json:"bucket,omitempty" url:"bucket,omitempty"`
	Global        *bool  `json:"global,omitempty" url:"global,omitempty"`
	Scope         string `json:"ratelimit-scope" url:"ratelimit-scope"`
	Enabled       *bool  `json:"enabled,omitempty" url:"enabled,omitempty"`
	MaxReadBytes  *int64 `json:"max-read-bytes,omitempty" url:"max-read-bytes,omitempty"`
	MaxWriteBytes *int64 `json:"max-write-bytes,omitempty" url:"max-write-bytes,omitempty"`
	MaxReadOps    *int64 `json:"max-read-ops,omitempty" url:"max-read-ops,omitempty"`
	MaxWriteOps   *int64 `json:"max-write-ops,omitempty" url:"max-write-ops,omitempty"`
}

// GetUserRateLimit retrieves rate limits for a specific user
// https://docs.ceph.com/en/latest/radosgw/adminops/#get-user-rate-limit
func (api *API) GetUserRateLimit(ctx context.Context, ratelimit RateLimitSpec) (RateLimitSpec, error) {
	if ratelimit.UID == "" {
		return RateLimitSpec{}, errMissingUserID
	}

	ratelimit.Scope = "user"
	body, err := api.call(ctx, http.MethodGet, "/ratelimit", valueToURLParams(ratelimit, []string{"uid", "ratelimit-scope"}))
	if err != nil {
		return RateLimitSpec{}, err
	}

	var ref RateLimitSpec
	if err = json.Unmarshal(body, &ref); err != nil {
		return RateLimitSpec{}, fmt.Errorf("%s. %s. %w", unmarshalError, string(body), err)
	}

	return ref, nil
}

// SetUserRateLimit sets rate limits for a specific user
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-user-rate-limit
func (api *API) SetUserRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.UID == "" {
		return errMissingUserID
	}

	ratelimit.Scope = "user"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"uid", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}
