package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GetGlobalRateLimit retrieves global rate limits
// https://docs.ceph.com/en/latest/radosgw/adminops/#get-global-rate-limit
func (api *API) GetGlobalRateLimit(ctx context.Context) (RateLimitSpec, error) {
	global := true
	ratelimit := RateLimitSpec{
		Global: &global,
		Scope:  "global",
	}

	body, err := api.call(ctx, http.MethodGet, "/ratelimit", valueToURLParams(ratelimit, []string{"global"}))
	if err != nil {
		return RateLimitSpec{}, err
	}

	var ref RateLimitSpec
	if err = json.Unmarshal(body, &ref); err != nil {
		return RateLimitSpec{}, fmt.Errorf("%s. %s. %w", unmarshalError, string(body), err)
	}

	return ref, nil
}

// SetGlobalUserRateLimit sets global rate limits for users
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-global-user-rate-limit
func (api *API) SetGlobalUserRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.Global == nil || *ratelimit.Global != true {
		return fmt.Errorf("global must be true for global user rate limit")
	}

	ratelimit.Scope = "user"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"global", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}

// SetGlobalBucketRateLimit sets global rate limits for buckets
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-global-rate-limit-bucket
func (api *API) SetGlobalBucketRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.Global == nil || *ratelimit.Global != true {
		return fmt.Errorf("global must be true for global bucket rate limit")
	}

	ratelimit.Scope = "bucket"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"global", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}

// SetGlobalAnonymousRateLimit sets global rate limits for anonymous users
// https://docs.ceph.com/en/latest/radosgw/adminops/#set-global-anonymous-user-rate-limit
func (api *API) SetGlobalAnonymousRateLimit(ctx context.Context, ratelimit RateLimitSpec) error {
	if ratelimit.Global == nil || *ratelimit.Global != true {
		return fmt.Errorf("global must be true for global anonymous rate limit")
	}

	ratelimit.Scope = "anon"
	_, err := api.call(ctx, http.MethodPost, "/ratelimit", valueToURLParams(ratelimit, []string{"global", "ratelimit-scope", "enabled", "max-read-bytes", "max-write-bytes", "max-read-ops", "max-write-ops"}))
	return err
}
