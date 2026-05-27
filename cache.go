package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"net/url"
	"sort"
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

// InvalidateBBox deletes all cached tiles whose (z,x,y) falls within the
// bounding box [xmin,ymin,xmax,ymax] (in the given srid) for every zoom in [minZoom,maxZoom].
// When srid is 0 the server's default coordinate system is used.
//
// layerID filters to a specific layer (empty = all layers).
// identityParams filters to a specific identity hash derived from $-prefixed
// params such as $project_uuid (nil/empty = all identities).
//
// Returns the number of Redis keys deleted.

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

	// Guard against runaway invalidation (e.g. full-world at zoom 20).
	limit := int64(viper.GetInt("CacheMaxInvalidateTiles"))
	if limit <= 0 {
		limit = 500_000
	}
	var addrCount int64
	for z := minZoom; z <= maxZoom; z++ {
		x1, y1 := tileXYInCRS(z, xmin, ymax, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax)
		x2, y2 := tileXYInCRS(z, xmax, ymin, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax)
		addrCount += int64((x2 - x1 + 1) * (y2 - y1 + 1))
	}
	if addrCount > limit {
		return 0, fmt.Errorf("bbox spans %d tile addresses (limit %d); reduce zoom range or bbox area",
			addrCount, limit)
	}

	var deleted int64
	for z := minZoom; z <= maxZoom; z++ {
		x1, y1 := tileXYInCRS(z, xmin, ymax, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax) // y increases downward in tile coords
		x2, y2 := tileXYInCRS(z, xmax, ymin, bounds.Xmin, bounds.Xmax, bounds.Ymin, bounds.Ymax)
		for x := x1; x <= x2; x++ {
			for y := y1; y <= y2; y++ {
				n, err := c.scanAndDelete(ctx, tileCachePattern(layerID, iHash, z, x, y))
				if err != nil {
					return deleted, err
				}
				deleted += n
			}
		}
	}
	return deleted, nil
}

// scanAndDelete finds all Redis keys matching pattern via SCAN and deletes them.
func (c *TileCache) scanAndDelete(ctx context.Context, pattern string) (int64, error) {
	var cursor uint64
	var total int64
	for {
		keys, next, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return total, fmt.Errorf("redis SCAN(%q): %w", pattern, err)
		}
		if len(keys) > 0 {
			n, err := c.client.Del(ctx, keys...).Result()
			if err != nil {
				return total, fmt.Errorf("redis DEL: %w", err)
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

// InvalidateLayer deletes all cached tiles for the given layer and optional
// identity params, regardless of z/x/y. Use when you want to purge an entire
// layer or a specific project identity without knowing the spatial extent.
// If layerID is empty all tiles in the cache are deleted.
func (c *TileCache) InvalidateLayer(ctx context.Context, layerID string, identityParams url.Values) (int64, error) {
	layer := "*"
	if layerID != "" {
		layer = layerID
	}
	iHash := "*"
	if len(identityParams) > 0 {
		iHash = paramsHash(identityParams)
	}
	// Pattern matches all z/x/y/render combinations for the given layer+identity.
	pattern := fmt.Sprintf("tile:%s:%s:*", layer, iHash)
	return c.scanAndDelete(ctx, pattern)
}

// tileCacheKey builds a deterministic Redis key:
//
//	tile:{layerID}:{identityHash}:{z}:{x}:{y}:{renderHash}
//
// Identity params are $-prefixed PostgreSQL function arguments (e.g. $project_uuid).
// Render params are everything else (srid, tolerance, properties, …).
func tileCacheKey(layerID string, z, x, y int, params url.Values) string {
	identity, render := splitParams(params)
	return fmt.Sprintf("tile:%s:%s:%d:%d:%d:%s", layerID, paramsHash(identity), z, x, y, paramsHash(render))
}

// tileCachePattern returns a Redis SCAN glob for a tile address (z,x,y).
// Pass an empty layerID or identityHash to match any value in that position.
func tileCachePattern(layerID, identityHash string, z, x, y int) string {
	layer := "*"
	if layerID != "" {
		layer = layerID
	}
	iHash := "*"
	if identityHash != "" {
		iHash = identityHash
	}
	return fmt.Sprintf("tile:%s:%s:%d:%d:%d:*", layer, iHash, z, x, y)
}

// splitParams separates query params into identity ($-prefixed) and render (rest).
func splitParams(params url.Values) (identity url.Values, render url.Values) {
	identity = make(url.Values)
	render = make(url.Values)
	for k, v := range params {
		if strings.HasPrefix(k, "$") {
			identity[k] = v
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
