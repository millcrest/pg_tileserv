package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// globalCache is the Redis-backed tile cache. nil means caching is disabled.
var globalCache *TileCache

// TileCache wraps a Redis client for tile caching.
type TileCache struct {
	client *redis.Client
	ttl    time.Duration
}

// initCache reads Redis configuration and connects.
// If RedisAddr is empty the cache is left nil (graceful degradation).
func initCache() {
	addr := viper.GetString("RedisAddr")
	if addr == "" {
		log.Info("Redis not configured (RedisAddr unset); tile caching disabled")
		return
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: viper.GetString("RedisPassword"),
		DB:       viper.GetInt("RedisDB"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.WithError(err).Warn("Redis ping failed; tile caching disabled")
		return
	}

	ttl := time.Duration(viper.GetInt("RedisTTL")) * time.Second
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	globalCache = &TileCache{client: client, ttl: ttl}
	log.Infof("Redis tile cache connected: %s (TTL: %s)", addr, ttl)
}

// Get returns cached tile bytes for key. Returns (nil, false) on miss or error.
func (c *TileCache) Get(ctx context.Context, key string) ([]byte, bool) {
	data, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false
	}
	if err != nil {
		log.WithError(err).Warn("tile cache Get error")
		return nil, false
	}
	return data, true
}

// Set stores tile bytes under key. Cache errors are logged and swallowed.
func (c *TileCache) Set(ctx context.Context, key string, data []byte) {
	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		log.WithError(err).Warn("tile cache Set error")
	}
}

// tileXYInCRS converts a projected point (px, py) in the given CRS to tile
// (x, y) indices at zoom. For SRID 4326 the Mercator slippy-map formula is
// used; for all other SRIDs a linear subdivision of the CRS extent is used.
func tileXYInCRS(zoom int, px, py float64, csXmin, csXmax, csYmin, csYmax float64) (x, y int) {
	n := math.Pow(2, float64(zoom))
	x = int(math.Floor((px - csXmin) / (csXmax - csXmin) * n))
	y = int(math.Floor((csYmax - py) / (csYmax - csYmin) * n)) // y increases downward
	maxIdx := int(n) - 1
	if x < 0 {
		x = 0
	}
	if x > maxIdx {
		x = maxIdx
	}
	if y < 0 {
		y = 0
	}
	if y > maxIdx {
		y = maxIdx
	}
	return x, y
}

// InvalidateBBox deletes all cached tiles whose (z,x,y) falls within the
// bounding box [xmin,ymin,xmax,ymax] (in the given srid) for every zoom in [minZoom,maxZoom].
//
// When srid is 0 the server's default coordinate system is used.
// layerID filters to a specific layer (empty = all layers).
// identityParams filters to a specific identity hash derived from the configured
// identity params (e.g. project_uuid); nil/empty = all identities.
//
// Returns the number of Redis keys deleted.
func (c *TileCache) InvalidateBBox(ctx context.Context, xmin, ymin, xmax, ymax float64, minZoom, maxZoom, srid int, layerID string, identityParams url.Values) (int64, error) {
	// Resolve CRS bounds via getServerBounds: uses globalDefaultCoordinateSystem when
	// srid is 0, reads from DB or config, and caches results in globalServerBounds.
	var sridPtr *int
	if srid != 0 {
		sridPtr = &srid
	}
	bounds, err := getServerBounds(sridPtr)
	if err != nil {
		return 0, fmt.Errorf("SRID %d: %w", srid, err)
	}

	// Clamp bbox to the CRS extent.
	xmin = math.Max(xmin, bounds.Xmin)
	xmax = math.Min(xmax, bounds.Xmax)
	ymin = math.Max(ymin, bounds.Ymin)
	ymax = math.Min(ymax, bounds.Ymax)

	if xmin >= xmax || ymin >= ymax {
		return 0, fmt.Errorf("invalid bbox: xmin/ymin must be strictly less than xmax/ymax")
	}

	// Compute the identity hash once (empty string = wildcard in pattern).
	iHash := ""
	if len(identityParams) > 0 {
		iHash = paramsHash(identityParams)
	}

	// Guard against runaway invalidation (e.g. full-world at zoom 20). Count
	// addresses cheaply first so we never allocate an oversized target set.
	limit := int64(viper.GetInt("CacheMaxInvalidateTiles"))
	if limit <= 0 {
		limit = 500_000
	}

	type zRange struct{ z, x1, y1, x2, y2 int }
	ranges := make([]zRange, 0, maxZoom-minZoom+1)
	var addrCount int64
	for z := minZoom; z <= maxZoom; z++ {
		x1, y1 := tileXYInCRS(z, xmin, ymax, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax)
		x2, y2 := tileXYInCRS(z, xmax, ymin, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax)
		ranges = append(ranges, zRange{z, x1, y1, x2, y2})
		addrCount += int64((x2 - x1 + 1) * (y2 - y1 + 1))
	}

	if addrCount > limit {
		return 0, fmt.Errorf("bbox spans %d tile addresses (limit %d); reduce zoom range or bbox area",
			addrCount, limit)
	}

	targets := make(map[[3]int]struct{}, addrCount)
	for _, r := range ranges {
		for x := r.x1; x <= r.x2; x++ {
			for y := r.y1; y <= r.y2; y++ {
				targets[[3]int{r.z, x, y}] = struct{}{}
			}
		}
	}

	return c.scanAndUnlink(ctx, tilePrefixPattern(layerID, iHash), func(key string) bool {
		z, x, y, ok := tileAddrFromKey(key)
		if !ok {
			return false
		}
		_, hit := targets[[3]int{z, x, y}]
		return hit
	})
}

// scanAndUnlink iterates Redis keys matching pattern via SCAN and removes those
// for which keep returns true (a nil keep removes every match). It uses UNLINK
// (asynchronous, non-blocking memory reclaim) rather than DEL so bulk
// invalidation does not stall the Redis main thread.
func (c *TileCache) scanAndUnlink(ctx context.Context, pattern string, keep func(string) bool) (int64, error) {
	var cursor uint64
	var total int64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return total, fmt.Errorf("redis SCAN(%q): %w", pattern, err)
		}
		batch := keys
		if keep != nil {
			batch = batch[:0]
			for _, k := range keys {
				if keep(k) {
					batch = append(batch, k)
				}
			}
		}
		if len(batch) > 0 {
			n, err := c.client.Unlink(ctx, batch...).Result()
			if err != nil {
				return total, fmt.Errorf("redis UNLINK: %w", err)
			}
			total += n
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return total, nil
}

// tileAddrFromKey extracts the (z,x,y) tile address from a cache key of the form
// tile:{layerID}:{identityHash}:{z}:{x}:{y}:{renderHash}. ok is false when the
// key does not have the expected shape (layerID never contains a colon).
func tileAddrFromKey(key string) (z, x, y int, ok bool) {
	parts := strings.Split(key, ":")
	if len(parts) != 7 {
		return 0, 0, 0, false
	}
	var err error
	if z, err = strconv.Atoi(parts[3]); err != nil {
		return 0, 0, 0, false
	}
	if x, err = strconv.Atoi(parts[4]); err != nil {
		return 0, 0, 0, false
	}
	if y, err = strconv.Atoi(parts[5]); err != nil {
		return 0, 0, 0, false
	}
	return z, x, y, true
}

// InvalidateLayer deletes all cached tiles for the given layer and optional
// identity params, regardless of z/x/y. Use when you want to purge an entire
// layer or a specific project identity without knowing the spatial extent.
// If layerID is empty all tiles in the cache are deleted.
func (c *TileCache) InvalidateLayer(ctx context.Context, layerID string, identityParams url.Values) (int64, error) {
	iHash := ""
	if len(identityParams) > 0 {
		iHash = paramsHash(identityParams)
	}
	// Pattern matches all z/x/y/render combinations for the given layer+identity.
	return c.scanAndUnlink(ctx, tilePrefixPattern(layerID, iHash), nil)
}

// tileCacheKey builds a deterministic Redis key:
//
//	tile:{layerID}:{identityHash}:{z}:{x}:{y}:{renderHash}
//
// Identity params are those configured via CacheIdentityParams (e.g. project_uuid);
// render params are everything else (srid, tolerance, properties, …).
func tileCacheKey(layerID string, z, x, y int, params url.Values) string {
	identity, render := splitParams(params)
	return fmt.Sprintf("tile:%s:%s:%d:%d:%d:%s", layerID, paramsHash(identity), z, x, y, paramsHash(render))
}

// tilePrefixPattern returns a Redis SCAN glob matching every tile for a
// layer+identity, across all z/x/y/render values. Pass an empty layerID or
// identityHash to match any value in that position.
func tilePrefixPattern(layerID, identityHash string) string {
	layer := "*"
	if layerID != "" {
		layer = layerID
	}
	iHash := "*"
	if identityHash != "" {
		iHash = identityHash
	}
	return fmt.Sprintf("tile:%s:%s:*", layer, iHash)
}

// identityParamNames returns the set of query-param names (from the
// CacheIdentityParams config) that identify a logical tile set rather than a
// render variant. A leading '$' is stripped so "project_uuid" and
// "$project_uuid" are treated identically.
func identityParamNames() map[string]struct{} {
	set := make(map[string]struct{})
	for n := range strings.SplitSeq(viper.GetString("CacheIdentityParams"), ",") {
		n = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(n), "$"))
		if n != "" {
			set[n] = struct{}{}
		}
	}
	return set
}

// splitParams separates query params into identity and render buckets using the
// configured identity-param names. Identity keys are normalized (any leading '$'
// removed) so the serve path (which sees e.g. "project_uuid") and the
// invalidation path (which may send "$project_uuid") hash to the same identity.
func splitParams(params url.Values) (identity url.Values, render url.Values) {
	identity = make(url.Values)
	render = make(url.Values)
	idNames := identityParamNames()
	for k, v := range params {
		name := strings.TrimPrefix(k, "$")
		if _, ok := idNames[name]; ok {
			identity[name] = v
		} else {
			render[k] = v
		}
	}
	return identity, render
}

// paramsHash produces a compact 16-hex-char SHA-256 fingerprint of the query params.
func paramsHash(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		vals := make([]string, len(params[k]))
		copy(vals, params[k])
		sort.Strings(vals)
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(strings.Join(vals, ","))
		sb.WriteByte('&')
	}
	h := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("%x", h[:8])
}
