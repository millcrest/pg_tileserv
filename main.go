package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"

	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	// REST routing
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"

	// Database connectivity
	"github.com/jackc/pgx/v4/pgxpool"

	// Logging
	log "github.com/sirupsen/logrus"

	// Configuration
	"github.com/pborman/getopt/v2"
	"github.com/spf13/viper"

	// Template functions
	"github.com/Masterminds/sprig/v3"

	// Prometheus metrics
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// programName is the name string we use
const programName string = "pg_tileserv"

// programVersion is the version string we use
// const programVersion string = "0.1"
var programVersion string

// globalDb is a global database connection pointer
var globalDb *pgxpool.Pool

// globalVersions holds the parsed output of postgis_full_version()
var globalVersions map[string]string

// globalPostGISVersion is numeric, sortable postgis version (3.2.1 => 3002001)
var globalPostGISVersion int

// serverBounds are the coordinate reference system and extent from
// which tiles are constructed
var globalServerBounds = make(map[int]*Bounds)
var globalDefaultCoordinateSystem int
var globalProjectionBoundsTableName string

// timeToLive is the Cache-Control timeout value that will be advertised
// in the response headers
var globalTimeToLive = -1

// A global array of Layer where the state is held for performance
// Refreshed when LoadLayerTableList is called
// Key is of the form: schemaname.tablename
var globalLayers map[string]Layer
var globalLayersMutex = &sync.Mutex{}

/******************************************************************************/

func init() {
	viper.SetDefault("DbConnection", "sslmode=disable")
	viper.SetDefault("HttpHost", "0.0.0.0")
	viper.SetDefault("HttpPort", 7800)
	viper.SetDefault("HttpsPort", 7801)
	viper.SetDefault("TlsServerCertificateFile", "")
	viper.SetDefault("TlsServerPrivateKeyFile", "")
	viper.SetDefault("UrlBase", "")
	viper.SetDefault("DefaultResolution", 4096)
	viper.SetDefault("DefaultBuffer", 256)
	viper.SetDefault("MaxFeaturesPerTile", 50000)
	viper.SetDefault("DefaultMinZoom", 0)
	viper.SetDefault("DefaultMaxZoom", 22)
	viper.SetDefault("Debug", false)
	viper.SetDefault("ShowPreview", true)
	viper.SetDefault("AssetsPath", "./assets")
	// 1d, 1h, 1m, 1s, see https://golang.org/pkg/time/#ParseDuration
	viper.SetDefault("DbPoolMaxConnLifeTime", "1h")
	viper.SetDefault("DbPoolMaxConns", 4)
	viper.SetDefault("DbTimeout", 10)
	viper.SetDefault("CORSOrigins", []string{"*"})
	viper.SetDefault("BasePath", "/")
	viper.SetDefault("CacheTTL", 0)          // cache timeout in seconds
	viper.SetDefault("EnableMetrics", false) // Prometheus metrics

	// Redis tile cache
	viper.SetDefault("RedisAddr", "")           // e.g. "localhost:6379"
	viper.SetDefault("RedisPassword", "")
	viper.SetDefault("RedisDB", 0)
	viper.SetDefault("RedisTTL", 86400)         // per-tile TTL in seconds (default 24 h)
	viper.SetDefault("CacheMaxInvalidateTiles", 500_000)

	viper.SetDefault("DefaultCoordinateSystem", 3857)
	// XMin, YMin, XMax, YMax, must be square
	viper.SetDefault("CoordinateSystem.3857.Xmin", -20037508.3427892)
	viper.SetDefault("CoordinateSystem.3857.Ymin", -20037508.3427892)
	viper.SetDefault("CoordinateSystem.3857.Xmax", 20037508.3427892)
	viper.SetDefault("CoordinateSystem.3857.Ymax", 20037508.3427892)

	viper.SetDefault("HealthEndpoint", "/health")
}

func main() {

	// Read the commandline
	flagDebugOn := getopt.BoolLong("debug", 'd', "log debugging information")
	flagConfigFile := getopt.StringLong("config", 'c', "", "full path to config file", "config.toml")
	flagHelpOn := getopt.BoolLong("help", 'h', "display help output")
	flagVersionOn := getopt.BoolLong("version", 'v', "display version number")
	flagHidePreview := getopt.BoolLong("no-preview", 'n', "hide web interface")
	flagHealthEndpoint := getopt.StringLong("health", 'e', "", "desired path to health endpoint, e.g. \"/health\"")
	getopt.Parse()

	if *flagHelpOn {
		getopt.PrintUsage(os.Stdout)
		os.Exit(1)
	}

	if *flagVersionOn {
		fmt.Printf("%s %s\n", programName, programVersion)
		os.Exit(0)
	}

	viper.AutomaticEnv()
	viper.SetEnvPrefix("ts")

	// Enable debug mode if specified by commandline argument, regardless of what is in config file
	if *flagDebugOn {
		viper.Set("Debug", true)
		log.SetLevel(log.TraceLevel)
	}

	if *flagConfigFile != "" {
		viper.SetConfigFile(*flagConfigFile)
	} else {
		viper.SetConfigName(programName)
		viper.SetConfigType("toml")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("/config")
		viper.AddConfigPath("/etc")
	}

	if *flagHidePreview {
		viper.Set("ShowPreview", false)
	}

	if *flagHealthEndpoint != "" {
		viper.Set("HealthEndpoint", *flagHealthEndpoint)
	}

	// Report our status
	log.Infof("%s %s", programName, programVersion)
	log.Info("Run with --help parameter for commandline options")

	// Read environment configuration first
	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		viper.Set("DbConnection", dbURL)
		log.Info("Using database connection info from environment variable DATABASE_URL")
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Debugf("viper.ConfigFileNotFoundError: %s", err)
		} else {
			if _, ok := err.(viper.UnsupportedConfigError); ok {
				log.Debugf("viper.UnsupportedConfigError: %s", err)
			} else {
				log.Fatalf("Configuration file error: %s", err)
			}
		}
	} else {
		// Really would like to log location of filename we found...
		// 	log.Infof("Reading configuration file %s", cf)
		if cf := viper.ConfigFileUsed(); cf != "" {
			log.Infof("Using config file: %s", cf)
		} else {
			log.Info("Config file: none found, using defaults")
		}
	}

	// enable debug mode if specified in config file, even if not specified by commandline argument
	if viper.GetBool("Debug") {
		log.SetLevel(log.TraceLevel)
	}

	basePath := viper.GetString("BasePath")
	log.Infof("Serving HTTP  at %s/", formatBaseURL(fmt.Sprintf("http://%s:%d",
		viper.GetString("HttpHost"), viper.GetInt("HttpPort")), basePath))
	log.Infof("Serving HTTPS at %s/", formatBaseURL(fmt.Sprintf("http://%s:%d",
		viper.GetString("HttpHost"), viper.GetInt("HttpsPort")), basePath))

	globalDefaultCoordinateSystem = viper.GetInt("DefaultCoordinateSystem")
	log.Infof("Default CoordinateSystem: %d", globalDefaultCoordinateSystem)

	globalProjectionBoundsTableName = viper.GetString("ProjectionBoundsTableName")

	// Load the global layer list right away
	// Also connects to database
	if err := loadLayers(); err != nil {
		log.Fatal(err)
	}

	// Read the postgis_full_version string and store
	// in a global for version testing
	if errv := loadVersions(); errv != nil {
		log.Fatal(errv)
	}
	log.WithFields(log.Fields{
		"event":       "connect",
		"topic":       "versions",
		"postgis":     globalVersions["POSTGIS"],
		"geos":        globalVersions["GEOS"],
		"pgsql":       globalVersions["PGSQL"],
		"libprotobuf": globalVersions["LIBPROTOBUF"],
	}).Debugf("Connected to PostGIS version %s\n", globalVersions["POSTGIS"])

	// Initialise the Redis tile cache (no-op when RedisAddr is not configured)
	initCache()

	// Get to work
	handleRequests()
}

/******************************************************************************/

func requestPreview(w http.ResponseWriter, r *http.Request) error {
	lyrID := mux.Vars(r)["name"]
	log.WithFields(log.Fields{
		"event": "request",
		"topic": "layerpreview",
		"key":   lyrID,
	}).Tracef("requestPreview: %s", lyrID)

	// reqProperties := r.FormValue("properties")
	// reqLimit := r.FormValue("limit")
	// reqResolution := r.FormValue("resolution")
	// reqBuffer := r.FormValue("buffer")

	// Refresh the layers list
	if err := loadLayers(); err != nil {
		return err
	}
	// Get the requested layer
	lyr, err := getLayer(lyrID)
	if err != nil {
		errLyr := tileAppError{
			HTTPCode: 404,
			SrcErr:   err,
		}
		return errLyr
	}

	switch l := lyr.(type) {
	case LayerTable:
		tmpl, err := template.ParseFiles(fmt.Sprintf("%s/preview-table.html", viper.GetString("AssetsPath")))
		if err != nil {
			return err
		}
		tmpl.Execute(w, l)
	case LayerFunction:
		tmpl, err := template.ParseFiles(fmt.Sprintf("%s/preview-function.html", viper.GetString("AssetsPath")))
		if err != nil {
			return err
		}
		tmpl.Execute(w, l)
	default:
		return errors.New("unknown layer type") // never get here
	}
	return nil
}

func requestListHTML(w http.ResponseWriter, r *http.Request) error {
	log.WithFields(log.Fields{
		"event": "request",
		"topic": "layerlist",
	}).Trace("requestListHtml")
	// Update the global in-memory list from
	// the database
	if err := loadLayers(); err != nil {
		return err
	}
	jsonLayers := getJSONLayers(r)

	content, err := os.ReadFile(fmt.Sprintf("%s/index.html", viper.GetString("AssetsPath")))

	if err != nil {
		return err
	}

	t, err := template.New("index").Funcs(sprig.FuncMap()).Parse(string(content))

	if err != nil {
		return err
	}
	t.Execute(w, jsonLayers)
	return nil
}

func requestListJSON(w http.ResponseWriter, r *http.Request) error {
	log.WithFields(log.Fields{
		"event": "request",
		"topic": "layerlist",
	}).Trace("requestListJSON")
	// Update the global in-memory list from
	// the database
	if err := loadLayers(); err != nil {
		return err
	}
	w.Header().Add("Content-Type", "application/json")
	jsonLayers := getJSONLayers(r)
	json.NewEncoder(w).Encode(jsonLayers)
	return nil
}

func requestDetailJSON(w http.ResponseWriter, r *http.Request) error {
	lyrID := mux.Vars(r)["name"]
	log.WithFields(log.Fields{
		"event": "request",
		"topic": "layerdetail",
	}).Tracef("requestDetailJSON(%s)", lyrID)

	// Refresh the layers list
	if err := loadLayers(); err != nil {
		return err
	}

	lyr, err := getLayer(lyrID)
	if err != nil {
		errLyr := tileAppError{
			HTTPCode: 404,
			SrcErr:   err,
		}
		return errLyr
	}

	errWrite := lyr.WriteLayerJSON(w, r)
	if errWrite != nil {
		return errWrite
	}
	return nil
}

// requestTile fetches a single tile, returning the MVT data and whether it was
// served from the Redis cache (true = HIT, false = MISS or cache disabled).
func requestTile(r *http.Request, source string, srid *int) ([]byte, bool, error) {
	vars := mux.Vars(r)

	lyr, err := getLayer(source)
	if err != nil {
		return nil, false, tileAppError{HTTPCode: 404, SrcErr: err}
	}

	tile, errTile := makeTile(vars, srid)
	if errTile != nil {
		return nil, false, errTile
	}

	log.WithFields(log.Fields{
		"event": "request",
		"topic": "tile",
		"key":   tile.String(),
	}).Tracef("requestTile: %s", tile.String())

	// Check the Redis tile cache before hitting the database.
	var cacheKey string
	if globalCache != nil {
		cacheKey = tileCacheKey(lyr.GetID(), tile.Zoom, tile.X, tile.Y, r.URL.Query())
		if data, ok := globalCache.Get(r.Context(), cacheKey); ok {
			log.WithFields(log.Fields{
				"event": "cache",
				"topic": "hit",
				"key":   tile.String(),
			}).Tracef("cache hit: %s", tile.String())
			return data, true, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration("DbTimeout")*time.Second)
	defer cancel()

	tilerequest := lyr.GetTileRequest(tile, r)
	mvt, errMvt := dBTileRequest(ctx, &tilerequest)
	if errMvt != nil {
		return nil, false, errMvt
	}

	// Store in cache on successful DB fetch.
	if globalCache != nil {
		globalCache.Set(r.Context(), cacheKey, mvt)
	}

	return mvt, false, nil
}

// requestTiles handles a tile request for a given layer, including multi layer tile requests
func requestTiles(w http.ResponseWriter, r *http.Request) error {
	var layers []byte
	vars := mux.Vars(r)

	var srid *int
	sridParam := r.URL.Query().Get("srid")
	sridInt, err := strconv.Atoi(sridParam)
	if err == nil {
		srid = &sridInt
	}

	sources := strings.Split(vars["name"], ",")
	var extant []string
	allCacheHits := globalCache != nil // start optimistic; set false on any MISS
	for _, source := range sources {
		if !slices.Contains(extant, source) {
			layer, cacheHit, err := requestTile(r, source, srid)
			if err != nil {
				return err
			}
			if !cacheHit {
				allCacheHits = false
			}
			layers = append(layers, layer...)
			extant = append(extant, source)
		} else {
			log.Debugf("Skipping duplicate layer %s in request %s", source, sources)
		}
	}

	w.Header().Add("Content-Type", "application/vnd.mapbox-vector-tile")
	if globalCache != nil {
		if allCacheHits {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
	}

	if _, errWrite := w.Write(layers); errWrite != nil {
		return errWrite
	}

	return nil
}

// A simple health check endpoint
func healthCheck(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("200 OK"))
	return nil
}

// requestCacheInvalidate evicts Redis-cached tiles matching the given filters.
// Query parameters:
//
//	layer     – optional layer ID to restrict invalidation (e.g. "common.project_feature_tiles")
//	$<param>  – optional $-prefixed identity params matching those used on tile requests
//	            (e.g. "$project_uuid=my-unique-id"); requires layer to also be set
//	bbox      – optional, repeatable "xmin,ymin,xmax,ymax" in the CRS given by srid;
//	            each bbox is invalidated independently; when omitted all tiles matching
//	            layer+identity are deleted regardless of location
//	srid      – optional EPSG code representing the projection of the bbox(es) (default: server's default CRS)
//	min_zoom  – optional, default 0                (only used with bbox)
//	max_zoom  – optional, default DefaultMaxZoom   (only used with bbox)
//
// Examples:
//
//	# Invalidate all tiles for one project (no bbox required)
//	POST /cache/invalidate?layer=common.project_feature_tiles&$project_uuid=my-id
//
//	# Invalidate one bbox
//	POST /cache/invalidate?layer=common.project_feature_tiles&$project_uuid=my-id&bbox=300000,6800000,400000,6900000&srid=28350
//
//	# Invalidate multiple bboxes in one request
//	POST /cache/invalidate?layer=common.project_feature_tiles&$project_uuid=my-id&bbox=300000,6800000,400000,6900000&bbox=500000,6700000,600000,6800000&srid=28350
func requestCacheInvalidate(w http.ResponseWriter, r *http.Request) error {
	if globalCache == nil {
		return tileAppError{
			HTTPCode: http.StatusServiceUnavailable,
			SrcErr:   fmt.Errorf("tile caching is not enabled"),
		}
	}

	q := r.URL.Query()

	// Optional layer filter.
	layerID := q.Get("layer")

	// Collect $-prefixed identity params (PostgreSQL function arguments).
	identityParams := make(map[string][]string)
	for k, v := range q {
		if strings.HasPrefix(k, "$") {
			identityParams[k] = v
		}
	}
	if len(identityParams) > 0 && layerID == "" {
		return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("'layer' is required when identity params ($...) are specified")}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	bboxValues := q["bbox"] // all repeated bbox= values; nil/empty when not provided

	// When no bbox is given, invalidate all tiles for the layer+identity.
	if len(bboxValues) == 0 {
		deleted, err := globalCache.InvalidateLayer(ctx, layerID, identityParams)
		if err != nil {
			return tileAppError{HTTPCode: 500, SrcErr: err}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"deleted": deleted,
			"message": fmt.Sprintf("invalidated %d cached tile(s)", deleted),
		})
		return nil
	}

	// Parse shared zoom / srid params (apply to every bbox).
	minZoom := 0
	maxZoom := viper.GetInt("DefaultMaxZoom")
	if z := q.Get("min_zoom"); z != "" {
		v, err := strconv.Atoi(z)
		if err != nil || v < 0 {
			return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("invalid min_zoom: %s", z)}
		}
		minZoom = v
	}
	if z := q.Get("max_zoom"); z != "" {
		v, err := strconv.Atoi(z)
		if err != nil || v < 0 {
			return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("invalid max_zoom: %s", z)}
		}
		maxZoom = v
	}
	if minZoom > maxZoom {
		return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("min_zoom (%d) must be <= max_zoom (%d)", minZoom, maxZoom)}
	}

	srid := 0 // 0 = use the server's default coordinate system
	if s := q.Get("srid"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v <= 0 {
			return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("invalid srid: %s", s)}
		}
		srid = v
	}

	// Invalidate each bbox, accumulating the total deleted count.
	var totalDeleted int64
	for i, bboxStr := range bboxValues {
		parts := strings.Split(bboxStr, ",")
		if len(parts) != 4 {
			return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("bbox[%d] must be 4 comma-separated values: xmin,ymin,xmax,ymax", i)}
		}
		coords := make([]float64, 4)
		for j, p := range parts {
			v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
			if err != nil {
				return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("bbox[%d]: invalid value %q: %w", i, p, err)}
			}
			coords[j] = v
		}
		deleted, err := globalCache.InvalidateBBox(ctx, coords[0], coords[1], coords[2], coords[3], minZoom, maxZoom, srid, layerID, identityParams)
		if err != nil {
			return tileAppError{HTTPCode: 400, SrcErr: fmt.Errorf("bbox[%d]: %w", i, err)}
		}
		totalDeleted += deleted
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"deleted": totalDeleted,
		"message": fmt.Sprintf("invalidated %d cached tile(s)", totalDeleted),
	})
	return nil
}

/******************************************************************************/

// tileAppError is an optional error structure functions can return
// if they want to specify the particular HTTP error code to be used
// in their error return
type tileAppError struct {
	HTTPCode int
	SrcErr   error
	Topic    string
	Message  string
}

// Error prints out a reasonable string format
func (tae tileAppError) Error() string {
	if tae.Message != "" {
		return fmt.Sprintf("%s\n%s", tae.Message, tae.SrcErr.Error())
	}
	return tae.SrcErr.Error()
}

// tileAppHandler is a function handler that can replace the
// existing handler and provide richer error handling, see below and
// https://blog.golang.org/error-handling-and-go
type tileAppHandler func(w http.ResponseWriter, r *http.Request) error

// ServeHTTP logs as much useful information as possible in
// a field format for potential Json logging streams
// as well as returning HTTP error response codes on failure
// so clients can see what is going on
// TODO: return JSON document body for the HTTP error
func (fn tileAppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.WithFields(log.Fields{
		"method": r.Method,
		"url":    r.URL,
	}).Infof("%s %s", r.Method, r.URL)
	if err := fn(w, r); err != nil {
		if hdr, ok := r.Header["X-Correlation-Id"]; ok {
			log.WithField("correlation-id", hdr[0])
		}
		if e, ok := err.(tileAppError); ok {
			if e.HTTPCode == 0 {
				e.HTTPCode = 500
			}
			if e.Topic != "" {
				log.WithField("topic", e.Topic)
			}
			log.WithField("key", e.Message)
			log.WithField("src", e.SrcErr.Error())
			log.Error(err)
			http.Error(w, e.Error(), e.HTTPCode)
		} else {
			log.Error(err)
			http.Error(w, err.Error(), 500)
		}
	}
}

/******************************************************************************/

func setCacheControl(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ttl := getTTL()
		if ttl > 0 {
			ccVal := fmt.Sprintf("max-age=%d", ttl)
			w.Header().Set("Cache-Control", ccVal)
		}
		next.ServeHTTP(w, r)
	})
}

// applyCORS sets CORS response headers. When origins contains "*" the header
// Access-Control-Allow-Origin: * is written unconditionally, which keeps
// things working even when a reverse proxy has already stripped the Origin
// request header before forwarding to this server.
func applyCORS(origins []string, next http.Handler) http.Handler {
	allowAll := slices.Contains(origins, "*")
	allowed := make(map[string]bool, len(origins))
	for _, o := range origins {
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowAll {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		} else {
			origin := r.Header.Get("Origin")
			if origin != "" && allowed[origin] {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS, POST, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Origin, Authorization")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

/******************************************************************************/

func tileRouter() *mux.Router {
	// creates a new instance of a mux router
	r := mux.NewRouter().
		StrictSlash(true).
		PathPrefix(
			"/" +
				strings.TrimLeft(viper.GetString("BasePath"), "/"),
		).
		Subrouter()

	// Front page and layer list
	if viper.GetBool("ShowPreview") {
		r.Handle("/", tileAppHandler(requestListHTML))
		r.Handle("/index.html", tileAppHandler(requestListHTML))
		r.Handle("/index.json", tileAppHandler(requestListJSON))
		// Layer detail and demo pages
		r.Handle("/{name}.html", tileAppHandler(requestPreview))
		r.Handle("/{name}.json", tileAppHandler(requestDetailJSON))
	}
	// Tile requests
	r.Handle("/{name}/{z:[0-9]+}/{x:[0-9]+}/{y:[0-9]+}.{ext}", tileMetrics(tileAppHandler(requestTiles)))

	if viper.GetBool("EnableMetrics") {
		r.Handle("/metrics", promhttp.Handler())
	}

	// Cache invalidation endpoint
	r.Handle("/cache/invalidate", tileAppHandler(requestCacheInvalidate)).Methods(http.MethodPost, http.MethodDelete)

	r.Handle(viper.GetString("HealthEndpoint"), tileAppHandler(healthCheck)).Methods(http.MethodGet)
	return r
}

func handleRequests() {

	// Get a configured router
	r := tileRouter()

	// Allow CORS from anywhere
	corsOrigins := viper.GetStringSlice("CORSOrigins")

	// Set a writeTimeout for the http server.
	// This value is the application's DbTimeout config setting plus a
	// grace period. The additional time allows the application to gracefully
	// handle timeouts on its own, canceling outstanding database queries and
	// returning an error to the client, while keeping the http.Server
	// WriteTimeout as a fallback.
	writeTimeout := (time.Duration(viper.GetInt("DbTimeout") + 5)) * time.Second

	// more "production friendly" timeouts
	// https://blog.simon-frey.eu/go-as-in-golang-standard-net-http-config-will-break-your-production/#You_should_at_least_do_this_The_easy_path
	s := &http.Server{
		ReadTimeout:  1 * time.Second,
		WriteTimeout: writeTimeout,
		Addr:         fmt.Sprintf("%s:%d", viper.GetString("HttpHost"), viper.GetInt("HttpPort")),
		Handler:      setCacheControl(handlers.CompressHandler(applyCORS(corsOrigins, r))),
	}

	// start http service
	go func() {
		// ListenAndServe returns http.ErrServerClosed when the server receives
		// a call to Shutdown(). Other errors are unexpected.
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	tlsServerCert := viper.GetString("TlsServerCertificateFile")
	tlsServerPrivKey := viper.GetString("TlsServerPrivateKeyFile")
	var stls *http.Server
	doServeTLS := false
	// Attempt to use HTTPS only if server certificate and private key files specified
	if tlsServerCert != "" && tlsServerPrivKey != "" {
		doServeTLS = true
	}

	log.Infof("Serving HTTP  at %s:%d", viper.GetString("HttpHost"), viper.GetInt("HttpPort"))

	if doServeTLS {
		log.Infof("Serving HTTPS at %s:%d", viper.GetString("HttpHost"), viper.GetInt("HttpsPort"))
		stls = &http.Server{
			ReadTimeout:  1 * time.Second,
			WriteTimeout: writeTimeout,
			Addr:         fmt.Sprintf("%s:%d", viper.GetString("HttpHost"), viper.GetInt("HttpsPort")),
			Handler:      setCacheControl(handlers.CompressHandler(applyCORS(corsOrigins, r))),
			TLSConfig: &tls.Config{
				MinVersion: tls.VersionTLS12, // Secure TLS versions only
			},
		}

		// start https service
		go func() {
			// ListenAndServe returns http.ErrServerClosed when the server receives
			// a call to Shutdown(). Other errors are unexpected.
			if err := stls.ListenAndServeTLS(tlsServerCert, tlsServerPrivKey); err != nil && err != http.ErrServerClosed {
				log.Fatal(err)
			}
		}()
	}

	// wait here for interrupt signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	// Interrupt signal received:  Start shutting down
	log.Infoln("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	s.Shutdown(ctx)
	if doServeTLS {
		stls.Shutdown(ctx)
	}

	if globalDb != nil {
		log.Debugln("Closing DB connections")
		globalDb.Close()
	}
	log.Infoln("Server stopped.")
}

/******************************************************************************/
