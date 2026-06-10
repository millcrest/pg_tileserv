package main

import (
	"net/url"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Regression guard for the identity-hash mismatch: the serve path receives the
// project identifier as a plain "project_uuid" query param, while the
// invalidation path sends "$project_uuid". Both must resolve to the same
// identity hash, otherwise invalidation silently matches nothing.
func TestSplitParamsIdentityRoundTrip(t *testing.T) {
	viper.Set("CacheIdentityParams", "project_uuid")
	defer viper.Set("CacheIdentityParams", "")

	// As built by the tile-serving client (no '$').
	serve := url.Values{"project_uuid": {"abc"}, "srid": {"28350"}, "tolerance": {"0.001"}}
	// As sent by the invalidation caller ('$' marks identity).
	inval := url.Values{"$project_uuid": {"abc"}, "bbox": {"1,2,3,4"}, "srid": {"28350"}}

	serveIdentity, serveRender := splitParams(serve)
	invalIdentity, _ := splitParams(inval)

	assert.Equal(t, paramsHash(serveIdentity), paramsHash(invalIdentity),
		"serve and invalidate identity hashes must match")

	// srid and tolerance are render variants, not identity.
	_, tolInIdentity := serveIdentity["tolerance"]
	assert.False(t, tolInIdentity, "tolerance must not be an identity param")
	_, sridInIdentity := serveIdentity["srid"]
	assert.False(t, sridInIdentity, "srid must not be an identity param")
	_, tolInRender := serveRender["tolerance"]
	assert.True(t, tolInRender, "tolerance must be a render param")
}

// The '$' prefix is optional/normalized: configuring "project_uuid" must match
// both "project_uuid" and "$project_uuid" incoming keys.
func TestSplitParamsDollarInsensitive(t *testing.T) {
	viper.Set("CacheIdentityParams", "$project_uuid")
	defer viper.Set("CacheIdentityParams", "")

	plain, _ := splitParams(url.Values{"project_uuid": {"x"}})
	dollar, _ := splitParams(url.Values{"$project_uuid": {"x"}})

	assert.Equal(t, paramsHash(plain), paramsHash(dollar))
	_, ok := plain["project_uuid"]
	assert.True(t, ok, "identity key must be normalized without '$'")
}

func TestTileAddrFromKey(t *testing.T) {
	z, x, y, ok := tileAddrFromKey("tile:common.project_feature_tiles:b1430ebecf34009c:7:12:34:fc533f8cd831e6ba")
	assert.True(t, ok)
	assert.Equal(t, 7, z)
	assert.Equal(t, 12, x)
	assert.Equal(t, 34, y)

	_, _, _, ok = tileAddrFromKey("tile:layer:hash:notanint:0:0:r")
	assert.False(t, ok)
	_, _, _, ok = tileAddrFromKey("too:few:parts")
	assert.False(t, ok)
}
