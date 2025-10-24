// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporter

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type profileCollector struct {
	ctx               context.Context
	base              *baseCollector
	compatibleMode    bool
	topologyInfo      labelsGetter
	profiletimets     int
	maxStringSize     int
	lastInfoClear     time.Time
	infoClearInterval time.Duration
	mutex             sync.RWMutex
}

// Prometheus metrics matching the Python implementation exactly
var (
	slowQueriesCountTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_count_total",
			Help:      "Total number of slow queries",
		},
		[]string{"db", "ns", "query_hash"},
	)

	slowQueriesInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_info",
			Help:      "Information about slow query",
		},
		[]string{"db", "ns", "query_hash", "query_shape", "query_framework", "op", "plan_summary"},
	)

	slowQueriesDurationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_duration_total",
			Help:      "Total execution time of slow queries in milliseconds",
		},
		[]string{"db", "ns", "query_hash"},
	)

	slowQueriesKeysExaminedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_keys_examined_total",
			Help:      "Total number of examined keys",
		},
		[]string{"db", "ns", "query_hash"},
	)

	slowQueriesDocsExaminedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_docs_examined_total",
			Help:      "Total number of examined documents",
		},
		[]string{"db", "ns", "query_hash"},
	)

	slowQueriesNreturnedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mongodb",
			Subsystem: "profile",
			Name:      "slow_queries_nreturned_total",
			Help:      "Total number of returned documents",
		},
		[]string{"db", "ns", "query_hash"},
	)
)

// Keys to remove from query for normalization (matching Python implementation)
var keysToRemove = []string{"cursor", "lsid", "projection", "limit", "signature", "$readPreference", "$db", "$clusterTime"}

// newProfileCollector creates a collector for being processed queries.
func newProfileCollector(ctx context.Context, client *mongo.Client, logger *slog.Logger,
	compatible bool, topology labelsGetter, profileTimeTS int, maxStringSize int,
) *profileCollector {
	return &profileCollector{
		ctx:               ctx,
		base:              newBaseCollector(client, logger.With("collector", "profile")),
		compatibleMode:    compatible,
		topologyInfo:      topology,
		profiletimets:     profileTimeTS,
		maxStringSize:     maxStringSize,
		lastInfoClear:     time.Now(),
		infoClearInterval: 300 * time.Second, // 300 seconds like Python implementation
	}
}

func (d *profileCollector) Describe(ch chan<- *prometheus.Desc) {
	slowQueriesCountTotal.Describe(ch)
	slowQueriesInfo.Describe(ch)
	slowQueriesDurationTotal.Describe(ch)
	slowQueriesKeysExaminedTotal.Describe(ch)
	slowQueriesDocsExaminedTotal.Describe(ch)
	slowQueriesNreturnedTotal.Describe(ch)
}

func (d *profileCollector) Collect(ch chan<- prometheus.Metric) {
	d.collect()
	slowQueriesCountTotal.Collect(ch)
	slowQueriesInfo.Collect(ch)
	slowQueriesDurationTotal.Collect(ch)
	slowQueriesKeysExaminedTotal.Collect(ch)
	slowQueriesDocsExaminedTotal.Collect(ch)
	slowQueriesNreturnedTotal.Collect(ch)
}

func (d *profileCollector) collect() {
	client := d.base.client
	if client == nil {
		return
	}

	// Calculate time window (matching Python implementation)
	endTime := time.Now()
	startTime := endTime.Add(-time.Duration(d.profiletimets) * time.Second)

	// Get list of databases
	databases, err := client.ListDatabaseNames(d.ctx, bson.M{})
	if err != nil {
		d.base.logger.Debug("Failed to list databases", "error", err)
		return
	}

	// Remove excluded databases (matching Python implementation)
	excludedDbs := map[string]bool{"local": true, "admin": true, "config": true, "test": true}
	var validDbs []string
	for _, dbName := range databases {
		if !excludedDbs[dbName] {
			validDbs = append(validDbs, dbName)
		}
	}

	// Clear info metric every 300 seconds (matching Python implementation)
	d.mutex.Lock()
	if time.Since(d.lastInfoClear) >= d.infoClearInterval {
		slowQueriesInfo.Reset()
		d.lastInfoClear = time.Now()
	}
	d.mutex.Unlock()

	// Process each valid database
	for _, dbName := range validDbs {
		db := client.Database(dbName)

		// Get unique namespaces within time window
		nsValues, err := d.getNSValues(db, startTime, endTime)
		if err != nil {
			d.base.logger.Debug("Failed to get ns values", "database", dbName, "error", err)
			continue
		}

		for _, ns := range nsValues {
			// Get unique query hashes for this namespace within time window
			queryHashes, err := d.getQueryHashValues(db, ns, startTime, endTime)
			if err != nil {
				d.base.logger.Debug("Failed to get query hashes", "database", dbName, "ns", ns, "error", err)
				continue
			}

			for _, queryHash := range queryHashes {
				// Get count for this (db, ns, query_hash) combination
				count, err := d.getSlowQueriesCount(db, ns, queryHash, startTime, endTime)
				if err != nil {
					d.base.logger.Debug("Failed to get count", "database", dbName, "ns", ns, "query_hash", queryHash, "error", err)
					continue
				}

				// Update count metric
				slowQueriesCountTotal.WithLabelValues(dbName, ns, queryHash).Add(float64(count))

				// Get sum values for this (db, ns, query_hash) combination
				sums, err := d.getSlowQueriesValueSum(db, ns, queryHash, startTime, endTime)
				if err != nil {
					d.base.logger.Debug("Failed to get sums", "database", dbName, "ns", ns, "query_hash", queryHash, "error", err)
					continue
				}

				// Update sum metrics
				slowQueriesDurationTotal.WithLabelValues(dbName, ns, queryHash).Add(float64(sums["millis"]))
				slowQueriesKeysExaminedTotal.WithLabelValues(dbName, ns, queryHash).Add(float64(sums["keysExamined"]))
				slowQueriesDocsExaminedTotal.WithLabelValues(dbName, ns, queryHash).Add(float64(sums["docsExamined"]))
				slowQueriesNreturnedTotal.WithLabelValues(dbName, ns, queryHash).Add(float64(sums["nreturned"]))

				// Get query info for info metric
				queryInfo, err := d.getQueryInfoValues(db, ns, queryHash, startTime, endTime)
				if err != nil {
					d.base.logger.Debug("Failed to get query info", "database", dbName, "ns", ns, "query_hash", queryHash, "error", err)
					continue
				}

				// Update info metric if query shape is not empty
				if queryInfo.QueryShape != "" {
					slowQueriesInfo.WithLabelValues(
						dbName,
						ns,
						queryHash,
						d.truncateString(queryInfo.QueryShape),
						queryInfo.QueryFramework,
						queryInfo.Op,
						d.truncateString(queryInfo.PlanSummary),
					).Set(1)
				}
			}
		}
	}
}

// getNSValues gets unique ns values within time window (matching Python implementation)
func (d *profileCollector) getNSValues(db *mongo.Database, startTime, endTime time.Time) ([]string, error) {
	collection := db.Collection("system.profile")

	filter := bson.M{
		"ts": bson.M{
			"$gte": primitive.NewDateTimeFromTime(startTime),
			"$lt":  primitive.NewDateTimeFromTime(endTime),
		},
	}

	values, err := collection.Distinct(d.ctx, "ns", filter)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, val := range values {
		if str, ok := val.(string); ok {
			result = append(result, str)
		}
	}

	return result, nil
}

// getQueryHashValues gets unique queryHash values for a namespace within time window
func (d *profileCollector) getQueryHashValues(db *mongo.Database, ns string, startTime, endTime time.Time) ([]string, error) {
	collection := db.Collection("system.profile")

	filter := bson.M{
		"ns": ns,
		"ts": bson.M{
			"$gte": primitive.NewDateTimeFromTime(startTime),
			"$lt":  primitive.NewDateTimeFromTime(endTime),
		},
	}

	values, err := collection.Distinct(d.ctx, "queryHash", filter)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, val := range values {
		if str, ok := val.(string); ok {
			result = append(result, str)
		}
	}

	return result, nil
}

// getSlowQueriesCount gets count of documents for (db, ns, query_hash) within time window
func (d *profileCollector) getSlowQueriesCount(db *mongo.Database, ns, queryHash string, startTime, endTime time.Time) (int64, error) {
	collection := db.Collection("system.profile")

	filter := bson.M{
		"queryHash": queryHash,
		"ns":        ns,
		"ts": bson.M{
			"$gte": primitive.NewDateTimeFromTime(startTime),
			"$lt":  primitive.NewDateTimeFromTime(endTime),
		},
	}

	return collection.CountDocuments(d.ctx, filter)
}

// getSlowQueriesValueSum gets sum of specific fields within time window
func (d *profileCollector) getSlowQueriesValueSum(db *mongo.Database, ns, queryHash string, startTime, endTime time.Time) (map[string]int64, error) {
	collection := db.Collection("system.profile")

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"queryHash": queryHash,
				"ns":        ns,
				"ts": bson.M{
					"$gte": primitive.NewDateTimeFromTime(startTime),
					"$lt":  primitive.NewDateTimeFromTime(endTime),
				},
			},
		},
		{
			"$group": bson.M{
				"_id":          nil,
				"millis":       bson.M{"$sum": "$millis"},
				"keysExamined": bson.M{"$sum": "$keysExamined"},
				"docsExamined": bson.M{"$sum": "$docsExamined"},
				"nreturned":    bson.M{"$sum": "$nreturned"},
			},
		},
	}

	cursor, err := collection.Aggregate(d.ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(d.ctx)

	result := map[string]int64{
		"millis":       0,
		"keysExamined": 0,
		"docsExamined": 0,
		"nreturned":    0,
	}

	if cursor.Next(d.ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		if millis, ok := doc["millis"]; ok {
			if val, ok := millis.(int64); ok {
				result["millis"] = val
			} else if val, ok := millis.(int32); ok {
				result["millis"] = int64(val)
			}
		}
		if keysExamined, ok := doc["keysExamined"]; ok {
			if val, ok := keysExamined.(int64); ok {
				result["keysExamined"] = val
			} else if val, ok := keysExamined.(int32); ok {
				result["keysExamined"] = int64(val)
			}
		}
		if docsExamined, ok := doc["docsExamined"]; ok {
			if val, ok := docsExamined.(int64); ok {
				result["docsExamined"] = val
			} else if val, ok := docsExamined.(int32); ok {
				result["docsExamined"] = int64(val)
			}
		}
		if nreturned, ok := doc["nreturned"]; ok {
			if val, ok := nreturned.(int64); ok {
				result["nreturned"] = val
			} else if val, ok := nreturned.(int32); ok {
				result["nreturned"] = int64(val)
			}
		}
	}

	return result, nil
}

type QueryInfo struct {
	QueryShape     string
	QueryFramework string
	Op             string
	PlanSummary    string
}

// getQueryInfoValues gets query information for info metric (matching Python implementation)
func (d *profileCollector) getQueryInfoValues(db *mongo.Database, ns, queryHash string, startTime, endTime time.Time) (*QueryInfo, error) {
	collection := db.Collection("system.profile")

	filter := bson.M{
		"queryHash": queryHash,
		"ns":        ns,
		"ts": bson.M{
			"$gte": primitive.NewDateTimeFromTime(startTime),
			"$lt":  primitive.NewDateTimeFromTime(endTime),
		},
		"command.getMore": bson.M{"$exists": false},
		"command.explain": bson.M{"$exists": false},
	}

	var doc bson.M
	err := collection.FindOne(d.ctx, filter).Decode(&doc)
	if err != nil {
		return &QueryInfo{}, nil // Return empty if not found
	}

	info := &QueryInfo{}

	// Extract command and create query shape
	if command, ok := doc["command"].(bson.M); ok {
		info.QueryShape = d.createQueryShape(command)
	}

	// Extract other fields
	if queryFramework, ok := doc["queryFramework"].(string); ok {
		info.QueryFramework = queryFramework
	}
	if op, ok := doc["op"].(string); ok {
		info.Op = op
	}
	if planSummary, ok := doc["planSummary"].(string); ok {
		info.PlanSummary = planSummary
	}

	return info, nil
}

// createQueryShape creates a normalized query shape (matching Python implementation)
func (d *profileCollector) createQueryShape(command bson.M) string {
	normalized := d.removeKeysAndReplace(command, keysToRemove, "?")
	return d.bsonToString(normalized)
}

// removeKeysAndReplace removes keys and replaces values (matching Python implementation)
func (d *profileCollector) removeKeysAndReplace(query interface{}, keysToRemove []string, replaceValue string) interface{} {
	switch v := query.(type) {
	case bson.M:
		result := make(bson.M)
		for key, value := range v {
			// Skip keys in removal list
			skip := false
			for _, keyToRemove := range keysToRemove {
				if key == keyToRemove {
					skip = true
					break
				}
			}
			if !skip {
				result[key] = d.removeKeysAndReplace(value, keysToRemove, replaceValue)
			}
		}
		return result
	case bson.A:
		result := make(bson.A, len(v))
		for i, item := range v {
			result[i] = d.removeKeysAndReplace(item, keysToRemove, replaceValue)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = d.removeKeysAndReplace(item, keysToRemove, replaceValue)
		}
		return result
	default:
		return replaceValue
	}
}

// bsonToString converts BSON to string representation
func (d *profileCollector) bsonToString(v interface{}) string {
	switch val := v.(type) {
	case bson.M:
		var parts []string
		for k, v := range val {
			parts = append(parts, k+": "+d.bsonToString(v))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case bson.A:
		var parts []string
		for _, item := range val {
			parts = append(parts, d.bsonToString(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case []interface{}:
		var parts []string
		for _, item := range val {
			parts = append(parts, d.bsonToString(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case string:
		return "\"" + val + "\""
	default:
		return "?"
	}
}

// truncateString truncates string to maxStringSize (matching Python implementation)
func (d *profileCollector) truncateString(s string) string {
	if len(s) > d.maxStringSize {
		return s[:d.maxStringSize]
	}
	return s
}
