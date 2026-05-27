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
// WGS84 bounding box [xmin,ymin,xmax,ymax] for every zoom in [minZoom,maxZoom].
//
// layerID filters to a specific layer (empty = all layers).
// identityParams filters to a specific identity hash derived from $-prefixed
// params such as $project_uuid (nil/empty = all identities).
//
// Returns the number of Redis keys deleted.
func (c *TileCache) InvalidateBBox(ctx context.Context, xmin, ymin, xmax, ymax float64, minZoom, maxZoom int, layerID string, identityParams url.Values) (int64, error) {
	// Clamp to valid WGS84 / Web Mercator extents.
	xmin = math.Max(xmin, -180)
	xmax = math.Min(xmax, 180)
	ymin = math.Max(ymin, -85.051129)
	ymax = math.Min(ymax, 85.051129)

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
		x1, y1 := wgs84ToTileXY(z, xmin, ymax)
		x2, y2 := wgs84ToTileXY(z, xmax, ymin)
		addrCount += int64((x2 - x1 + 1) * (y2 - y1 + 1))
	}
	if addrCount > limit {
		return 0, fmt.Errorf("bbox spans %d tile addresses (limit %d); reduce zoom range or bbox area",
			addrCount, limit)
	}

	var deleted int64
	for z := minZoom; z <= maxZoom; z++ {
		x1, y1 := wgs84ToTileXY(z, xmin, ymax) // y increases downward in tile coords
		x2, y2 := wgs84ToTileXY(z, xmax, ymin)
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

// wgs84ToTileXY converts a WGS84 (lng, lat) point to XYZ tile coordinates at zoom
// using the standard Web Mercator slippy-map formula.
func wgs84ToTileXY(zoom int, lng, lat float64) (x, y int) {
	n := math.Pow(2, float64(zoom))
	x = int(math.Floor((lng + 180.0) / 360.0 * n))
	latRad := lat * math.Pi / 180.0
	y = int(math.Floor((1.0 - math.Log(math.Tan(latRad)+1.0/math.Cos(latRad))/math.Pi) / 2.0 * n))
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
