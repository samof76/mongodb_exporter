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
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type profileCollector struct {
	ctx            context.Context
	base           *baseCollector
	compatibleMode bool
	topologyInfo   labelsGetter
	profiletimets  int
	maxStringSize  int
}

// newProfileCollector creates a collector for being processed queries.
func newProfileCollector(ctx context.Context, client *mongo.Client, logger *slog.Logger,
	compatible bool, topology labelsGetter, profileTimeTS int, maxStringSize int,
) *profileCollector {
	return &profileCollector{
		ctx:            ctx,
		base:           newBaseCollector(client, logger.With("collector", "profile")),
		compatibleMode: compatible,
		topologyInfo:   topology,
		profiletimets:  profileTimeTS,
		maxStringSize:  maxStringSize,
	}
}

func (d *profileCollector) Describe(ch chan<- *prometheus.Desc) {
	d.base.Describe(d.ctx, ch, d.collect)
}

func (d *profileCollector) Collect(ch chan<- prometheus.Metric) {
	d.base.Collect(ch)
}

// profileDocument represents a MongoDB profile document structure.
type profileDocument struct {
	TS                 primitive.DateTime `bson:"ts"`
	T                  primitive.DateTime `bson:"t,omitempty"`
	Op                 string             `bson:"op"`
	NS                 string             `bson:"ns"`
	QueryHash          string             `bson:"queryHash,omitempty"`
	Command            primitive.M        `bson:"command,omitempty"`
	OriginatingCommand primitive.M        `bson:"originatingCommand,omitempty"`
	QueryPlanning      *struct {
		PlanSummary string `bson:"planSummary,omitempty"`
	} `bson:"queryPlanning,omitempty"`
	ExecutionStats *struct {
		TotalExaminedDocs int64 `bson:"totalExaminedDocs,omitempty"`
		TotalKeysExamined int64 `bson:"totalKeysExamined,omitempty"`
		NReturned         int64 `bson:"nReturned,omitempty"`
	} `bson:"executionStats,omitempty"`
	KeysExamined   int64  `bson:"keysExamined,omitempty"`
	DocsExamined   int64  `bson:"docsExamined,omitempty"`
	NReturned      int64  `bson:"nreturned,omitempty"`
	Millis         int64  `bson:"millis"`
	QueryFramework string `bson:"queryFramework,omitempty"`
}

// normalizeQueryShape sanitizes and normalizes MongoDB query shapes.
func (d *profileCollector) normalizeQueryShape(command primitive.M) string {
	if command == nil {
		return ""
	}

	// Convert to JSON-like string and sanitize
	shape := d.sanitizeQuery(command)

	// Truncate if too long
	if len(shape) > d.maxStringSize {
		shape = shape[:d.maxStringSize-3] + "..."
	}

	return shape
}

// sanitizeQuery recursively sanitizes query values.
func (d *profileCollector) sanitizeQuery(v interface{}) string {
	switch val := v.(type) {
	case primitive.M:
		var parts []string
		for k, v := range val {
			parts = append(parts, fmt.Sprintf("%s: %s", k, d.sanitizeQuery(v)))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case primitive.A:
		var parts []string
		for _, item := range val {
			parts = append(parts, d.sanitizeQuery(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case string:
		return "?"
	case int, int32, int64, float32, float64:
		return "?"
	case primitive.ObjectID:
		return "ObjectId(?)"
	case primitive.DateTime:
		return "ISODate(?)"
	case bool:
		return strconv.FormatBool(val)
	default:
		return "?"
	}
}

// truncateString truncates a string to maxSize if it's longer.
func (d *profileCollector) truncateString(s string, maxSize int) string {
	if maxSize <= 0 {
		maxSize = d.maxStringSize
	}
	if len(s) <= maxSize {
		return s
	}
	return s[:maxSize-3] + "..."
}

// extractDatabaseAndCollection splits namespace into database and collection.
func extractDatabaseAndCollection(ns string) (string, string) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) < 2 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func (d *profileCollector) collect(ch chan<- prometheus.Metric) {
	defer measureCollectTime(ch, "mongodb", "profile")()

	logger := d.base.logger
	client := d.base.client
	timeScrape := d.profiletimets

	databases, err := databases(d.ctx, client, nil, nil)
	if err != nil {
		logger.Warn("cannot get databases", "error", err)
		return
	}

	// Time threshold for profile document filtering
	ts := primitive.NewDateTimeFromTime(time.Now().Add(-time.Duration(time.Second * time.Duration(timeScrape))))

	// Aggregated metrics by query signature
	queryMetrics := make(map[string]*queryAggregation)

	// Legacy metric for backward compatibility
	legacyCountByDB := make(map[string]int64)

	// Process each database
	for _, db := range databases {
		profileCollection := client.Database(db).Collection("system.profile")

		// Query profile documents
		filter := bson.M{"ts": bson.M{"$gte": ts}}
		cursor, err := profileCollection.Find(d.ctx, filter)
		if err != nil {
			logger.Warn("cannot query profile collection", "database", db, "error", err)
			continue
		}

		var docs []profileDocument
		if err := cursor.All(d.ctx, &docs); err != nil {
			logger.Warn("cannot decode profile documents", "database", db, "error", err)
			cursor.Close(d.ctx)
			continue
		}
		cursor.Close(d.ctx)

		// Process each profile document
		for _, doc := range docs {
			database, collection := extractDatabaseAndCollection(doc.NS)
			if database == "" {
				database = db
			}

			// Create query signature for aggregation
			signature := d.createQuerySignature(doc, database)

			// Aggregate metrics
			if agg, exists := queryMetrics[signature]; exists {
				agg.count++
				agg.totalDuration += doc.Millis
				agg.totalKeysExamined += d.getKeysExamined(doc)
				agg.totalDocsExamined += d.getDocsExamined(doc)
				agg.totalNReturned += d.getNReturned(doc)
			} else {
				queryMetrics[signature] = &queryAggregation{
					labels:            d.createLabels(doc, database, collection),
					count:             1,
					totalDuration:     doc.Millis,
					totalKeysExamined: d.getKeysExamined(doc),
					totalDocsExamined: d.getDocsExamined(doc),
					totalNReturned:    d.getNReturned(doc),
				}
			}

			// Legacy count by database
			legacyCountByDB[database]++
		}
	}

	// Generate enhanced metrics
	d.generateEnhancedMetrics(ch, queryMetrics)

	// Generate legacy metrics for backward compatibility
	d.generateLegacyMetrics(ch, legacyCountByDB)
}

// queryAggregation holds aggregated metrics for a query signature.
type queryAggregation struct {
	labels            map[string]string
	count             int64
	totalDuration     int64
	totalKeysExamined int64
	totalDocsExamined int64
	totalNReturned    int64
}

// createQuerySignature creates a unique signature for query aggregation.
func (d *profileCollector) createQuerySignature(doc profileDocument, database string) string {
	return fmt.Sprintf("%s|%s|%s|%s",
		database,
		doc.NS,
		doc.Op,
		doc.QueryHash)
}

// createLabels creates labels for metrics based on profile document.
func (d *profileCollector) createLabels(doc profileDocument, database, collection string) map[string]string {
	labels := d.topologyInfo.baseLabels()
	labels["database"] = database
	labels["namespace"] = doc.NS
	labels["op_type"] = doc.Op

	if doc.QueryHash != "" {
		labels["query_hash"] = doc.QueryHash
	}

	if doc.QueryFramework != "" {
		labels["query_framework"] = doc.QueryFramework
	} else {
		labels["query_framework"] = "classic"
	}

	// Add plan summary if available
	if doc.QueryPlanning != nil && doc.QueryPlanning.PlanSummary != "" {
		labels["plan_summary"] = d.truncateString(doc.QueryPlanning.PlanSummary, d.maxStringSize)
	}

	// Add query shape
	var command primitive.M
	if doc.Command != nil {
		command = doc.Command
	} else if doc.OriginatingCommand != nil {
		command = doc.OriginatingCommand
	}

	if command != nil {
		queryShape := d.normalizeQueryShape(command)
		if queryShape != "" {
			labels["query_shape"] = queryShape
		}
	}

	return labels
}

// Helper functions to extract metrics from profile document.
func (d *profileCollector) getKeysExamined(doc profileDocument) int64 {
	if doc.ExecutionStats != nil && doc.ExecutionStats.TotalKeysExamined > 0 {
		return doc.ExecutionStats.TotalKeysExamined
	}
	return doc.KeysExamined
}

func (d *profileCollector) getDocsExamined(doc profileDocument) int64 {
	if doc.ExecutionStats != nil && doc.ExecutionStats.TotalExaminedDocs > 0 {
		return doc.ExecutionStats.TotalExaminedDocs
	}
	return doc.DocsExamined
}

func (d *profileCollector) getNReturned(doc profileDocument) int64 {
	if doc.ExecutionStats != nil && doc.ExecutionStats.NReturned > 0 {
		return doc.ExecutionStats.NReturned
	}
	return doc.NReturned
}

// generateEnhancedMetrics creates the new comprehensive metrics.
func (d *profileCollector) generateEnhancedMetrics(ch chan<- prometheus.Metric, queryMetrics map[string]*queryAggregation) {
	for _, agg := range queryMetrics {
		// Counter metrics
		d.createCounterMetric(ch, "mongodb_profile_slow_queries_count_total",
			"Total number of slow queries by query shape", agg.labels, float64(agg.count))

		d.createCounterMetric(ch, "mongodb_profile_slow_queries_duration_total",
			"Total execution time of slow queries in milliseconds", agg.labels, float64(agg.totalDuration))

		d.createCounterMetric(ch, "mongodb_profile_slow_queries_keys_examined_total",
			"Total number of keys examined by slow queries", agg.labels, float64(agg.totalKeysExamined))

		d.createCounterMetric(ch, "mongodb_profile_slow_queries_docs_examined_total",
			"Total number of documents examined by slow queries", agg.labels, float64(agg.totalDocsExamined))

		d.createCounterMetric(ch, "mongodb_profile_slow_queries_nreturned_total",
			"Total number of documents returned by slow queries", agg.labels, float64(agg.totalNReturned))

		// Info gauge metric
		d.createGaugeMetric(ch, "mongodb_profile_slow_queries_info",
			"Query metadata and shape information", agg.labels, 1.0)
	}
}

// generateLegacyMetrics creates backward-compatible metrics.
func (d *profileCollector) generateLegacyMetrics(ch chan<- prometheus.Metric, legacyCountByDB map[string]int64) {
	for db, count := range legacyCountByDB {
		labels := d.topologyInfo.baseLabels()
		labels["database"] = db

		m := primitive.M{"count": count}
		for _, metric := range makeMetrics("profile_slow_query", m, labels, d.compatibleMode) {
			ch <- metric
		}
	}
}

// createCounterMetric creates a counter metric.
func (d *profileCollector) createCounterMetric(ch chan<- prometheus.Metric, name, help string, labels map[string]string, value float64) {
	labelNames := make([]string, 0, len(labels))
	labelValues := make([]string, 0, len(labels))

	for k, v := range labels {
		labelNames = append(labelNames, k)
		labelValues = append(labelValues, v)
	}

	desc := prometheus.NewDesc(name, help, labelNames, nil)
	metric := prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labelValues...)
	ch <- metric
}

// createGaugeMetric creates a gauge metric.
func (d *profileCollector) createGaugeMetric(ch chan<- prometheus.Metric, name, help string, labels map[string]string, value float64) {
	labelNames := make([]string, 0, len(labels))
	labelValues := make([]string, 0, len(labels))

	for k, v := range labels {
		labelNames = append(labelNames, k)
		labelValues = append(labelValues, v)
	}

	desc := prometheus.NewDesc(name, help, labelNames, nil)
	metric := prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labelValues...)
	ch <- metric
}
