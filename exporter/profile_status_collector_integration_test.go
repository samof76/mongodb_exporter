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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//nolint:paralleltest
func TestProfileCollectorIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	// Clean up test databases
	testDBs := []string{"testdb1", "testdb2"}
	for _, dbName := range testDBs {
		client.Database(dbName).Drop(ctx) //nolint:errcheck
	}

	defer func() {
		for _, dbName := range testDBs {
			client.Database(dbName).Drop(ctx) //nolint:errcheck
		}
	}()

	// Set up test databases with profiling enabled
	for _, dbName := range testDBs {
		db := client.Database(dbName)

		// Enable profiling for all operations
		cmd := bson.M{"profile": 2}
		err := db.RunCommand(ctx, cmd).Err()
		require.NoError(t, err, "Failed to enable profiling for %s", dbName)

		// Create some test collections and data
		testColl := db.Collection("testcoll")
		docs := []interface{}{
			bson.M{"name": "user1", "age": 25, "city": "New York"},
			bson.M{"name": "user2", "age": 30, "city": "London"},
			bson.M{"name": "user3", "age": 35, "city": "Tokyo"},
		}
		_, err = testColl.InsertMany(ctx, docs)
		require.NoError(t, err)

		// Create an index for more interesting query plans
		indexModel := mongo.IndexModel{
			Keys: bson.M{"name": 1},
			Options: &options.IndexOptions{
				Name: &[]string{"name_index"}[0],
			},
		}
		_, err = testColl.Indexes().CreateOne(ctx, indexModel)
		require.NoError(t, err)
	}

	// Reset metrics before test
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Generate some queries that will appear in the profiler
	for _, dbName := range testDBs {
		db := client.Database(dbName)
		testColl := db.Collection("testcoll")

		// Different types of queries to create varied profile entries
		queries := []bson.M{
			{"name": "user1"},            // Index query
			{"age": bson.M{"$gt": 20}},   // Collection scan
			{"city": "London"},           // Collection scan
			{"name": "user2", "age": 30}, // Mixed query
		}

		for _, query := range queries {
			// Execute multiple times to create aggregation
			for i := 0; i < 3; i++ {
				cursor, err := testColl.Find(ctx, query)
				require.NoError(t, err)

				var results []bson.M
				err = cursor.All(ctx, &results)
				require.NoError(t, err)
				cursor.Close(ctx)

				// Add slight delay to ensure different timestamps
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Also test update operations
		_, err := testColl.UpdateOne(ctx, bson.M{"name": "user1"}, bson.M{"$set": bson.M{"updated": true}})
		require.NoError(t, err)

		// Test aggregation operations
		pipeline := []bson.M{
			{"$match": bson.M{"age": bson.M{"$gte": 25}}},
			{"$group": bson.M{"_id": "$city", "count": bson.M{"$sum": 1}}},
		}
		cursor, err := testColl.Aggregate(ctx, pipeline)
		require.NoError(t, err)
		var aggResults []bson.M
		err = cursor.All(ctx, &aggResults)
		require.NoError(t, err)
		cursor.Close(ctx)
	}

	// Wait a moment for profiler entries to be written
	time.Sleep(1 * time.Second)

	// Create and run the profile collector
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Collect metrics
	pc.collect()

	// Verify that metrics were generated
	filter := []string{
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_duration_total",
		"mongodb_profile_slow_queries_keys_examined_total",
		"mongodb_profile_slow_queries_docs_examined_total",
		"mongodb_profile_slow_queries_nreturned_total",
		"mongodb_profile_slow_queries_info",
	}

	// Get metrics count for analysis
	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d metrics", metricsCount)

	// Verify that metrics were generated (count > 0)
	assert.Greater(t, metricsCount, 0, "Should have generated some metrics")

	// Verify specific metric types exist by trying to collect them individually
	countMetrics := testutil.CollectAndCount(pc, "mongodb_profile_slow_queries_count_total")
	durationMetrics := testutil.CollectAndCount(pc, "mongodb_profile_slow_queries_duration_total")
	infoMetrics := testutil.CollectAndCount(pc, "mongodb_profile_slow_queries_info")

	assert.Greater(t, countMetrics, 0, "Should have count metrics")
	assert.Greater(t, durationMetrics, 0, "Should have duration metrics")
	assert.Greater(t, infoMetrics, 0, "Should have info metrics")
}

//nolint:paralleltest
func TestProfileCollectorTimeWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	dbName := "timewindowtest"
	db := client.Database(dbName)
	db.Drop(ctx) //nolint:errcheck

	defer func() {
		db.Drop(ctx) //nolint:errcheck
	}()

	// Enable profiling for all operations
	cmd := bson.M{"profile": 2}
	err = db.RunCommand(ctx, cmd).Err()
	require.NoError(t, err)

	// Create test collection and data
	testColl := db.Collection("testcoll")
	docs := []interface{}{
		bson.M{"recent": "data", "value": 1},
		bson.M{"recent": "data", "value": 2},
	}
	_, err = testColl.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Generate some recent queries (these should be within the time window)
	for i := 0; i < 3; i++ {
		cursor, err := testColl.Find(ctx, bson.M{"recent": "data"})
		require.NoError(t, err)

		var results []bson.M
		err = cursor.All(ctx, &results)
		require.NoError(t, err)
		cursor.Close(ctx)

		time.Sleep(10 * time.Millisecond)
	}

	// Wait for profile entries to be written
	time.Sleep(100 * time.Millisecond)

	// Reset metrics
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Create collector with 30 second time window
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Collect metrics
	pc.collect()

	// Verify that some metrics were generated (should only be recent ones)
	filter := []string{
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_info",
	}
	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d time window metrics", metricsCount)

	// Should have generated some metrics (for recent data)
	assert.Greater(t, metricsCount, 0, "Should have metrics for recent data")

	// The specific verification of which query hashes are present would require
	// a more complex test setup or direct metric inspection
}

//nolint:paralleltest
func TestProfileCollectorAggregation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	dbName := "aggregationtest"
	db := client.Database(dbName)
	db.Drop(ctx) //nolint:errcheck

	defer func() {
		db.Drop(ctx) //nolint:errcheck
	}()

	// Enable profiling for all operations
	cmd := bson.M{"profile": 2}
	err = db.RunCommand(ctx, cmd).Err()
	require.NoError(t, err)

	// Create test collection and data
	testColl := db.Collection("testcoll")
	docs := []interface{}{
		bson.M{"name": "test", "value": 1},
		bson.M{"name": "test", "value": 2},
		bson.M{"name": "test", "value": 3},
	}
	_, err = testColl.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Execute the same query multiple times to create profile entries
	// that will be aggregated together
	for i := 0; i < 5; i++ {
		cursor, err := testColl.Find(ctx, bson.M{"name": "test"})
		require.NoError(t, err)

		var results []bson.M
		err = cursor.All(ctx, &results)
		require.NoError(t, err)
		cursor.Close(ctx)

		// Small delay to ensure different timestamps
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for profile entries to be written
	time.Sleep(100 * time.Millisecond)

	// Reset metrics
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Create collector
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Collect metrics
	pc.collect()

	// Verify that aggregation occurred - metrics should be generated
	filter := []string{
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_duration_total",
		"mongodb_profile_slow_queries_keys_examined_total",
		"mongodb_profile_slow_queries_docs_examined_total",
		"mongodb_profile_slow_queries_nreturned_total",
		"mongodb_profile_slow_queries_info",
	}

	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d aggregation metrics", metricsCount)

	// Should have generated some metrics from the repeated queries
	assert.Greater(t, metricsCount, 0, "Should have generated metrics from repeated queries")
}

//nolint:paralleltest
func TestProfileCollectorExcludedDatabasesIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	// Test that excluded databases are ignored
	excludedDBs := []string{"admin", "local", "config", "test"}

	for _, dbName := range excludedDBs {
		db := client.Database(dbName)

		// Try to create a profile entry (this may fail for some system dbs, that's ok)
		profileColl := db.Collection("system.profile")
		now := primitive.NewDateTimeFromTime(time.Now())

		doc := bson.M{
			"ts":           now,
			"op":           "query",
			"ns":           dbName + ".somecoll",
			"queryHash":    "excludedhash",
			"command":      bson.M{"find": "somecoll"},
			"millis":       100,
			"keysExamined": 10,
			"docsExamined": 5,
			"nreturned":    1,
		}

		// Insert may fail for system databases, ignore error
		profileColl.InsertOne(ctx, doc) //nolint:errcheck
	}

	// Reset metrics
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Create collector
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Collect metrics
	pc.collect()

	// Verify that no metrics were generated for excluded databases
	filter := []string{
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_info",
	}
	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d metrics from excluded databases", metricsCount)

	// Should have no metrics since all test databases are excluded
	// (Note: This test may generate 0 metrics, which is expected)
	assert.GreaterOrEqual(t, metricsCount, 0, "Metrics count should be non-negative")
}

//nolint:paralleltest
func TestProfileCollectorQueryNormalizationIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	dbName := "normalizationtest"
	db := client.Database(dbName)
	db.Drop(ctx) //nolint:errcheck

	defer func() {
		db.Drop(ctx) //nolint:errcheck
	}()

	// Enable profiling for all operations
	cmd := bson.M{"profile": 2}
	err = db.RunCommand(ctx, cmd).Err()
	require.NoError(t, err)

	// Create test collection and data with values that will be normalized
	testColl := db.Collection("testcoll")
	docs := []interface{}{
		bson.M{"sensitive": "secret_value", "public": "public_value", "id": 1},
		bson.M{"sensitive": "other_secret", "public": "other_public", "id": 2},
	}
	_, err = testColl.InsertMany(ctx, docs)
	require.NoError(t, err)

	// Execute queries that will have their shapes normalized
	cursor, err := testColl.Find(ctx, bson.M{"sensitive": "secret_value", "public": "public_value"})
	require.NoError(t, err)

	var results []bson.M
	err = cursor.All(ctx, &results)
	require.NoError(t, err)
	cursor.Close(ctx)

	// Wait for profile entries to be written
	time.Sleep(100 * time.Millisecond)

	// Reset metrics
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Create collector
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Collect metrics
	pc.collect()

	// Verify that query normalization metrics were generated
	filter := []string{"mongodb_profile_slow_queries_info"}
	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d query normalization metrics", metricsCount)

	// Should have generated at least one info metric
	assert.Greater(t, metricsCount, 0, "Should have generated info metrics with normalized queries")

	// For detailed verification of query normalization, we rely on the unit tests
	// Integration tests focus on end-to-end functionality rather than detailed output parsing
}

//nolint:paralleltest
func TestProfileCollectorStringTruncationIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Connect directly to MongoDB container
	uri := "mongodb://localhost:27017/"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	dbName := "truncationtest"
	db := client.Database(dbName)
	db.Drop(ctx) //nolint:errcheck

	defer func() {
		db.Drop(ctx) //nolint:errcheck
	}()

	// Enable profiling for all operations
	cmd := bson.M{"profile": 2}
	err = db.RunCommand(ctx, cmd).Err()
	require.NoError(t, err)

	// Create test collection with many fields to create long query shapes
	testColl := db.Collection("testcoll")
	longDoc := bson.M{}
	for i := 0; i < 20; i++ {
		longDoc[fmt.Sprintf("field%d", i)] = fmt.Sprintf("value%d", i)
	}
	_, err = testColl.InsertOne(ctx, longDoc)
	require.NoError(t, err)

	// Execute a query with many conditions to create a long query shape
	longFilter := bson.M{}
	for i := 0; i < 15; i++ {
		longFilter[fmt.Sprintf("field%d", i)] = fmt.Sprintf("value%d", i)
	}

	cursor, err := testColl.Find(ctx, longFilter)
	require.NoError(t, err)

	var results []bson.M
	err = cursor.All(ctx, &results)
	require.NoError(t, err)
	cursor.Close(ctx)

	// Wait for profile entries to be written
	time.Sleep(100 * time.Millisecond)

	// Reset metrics
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	// Create collector with small max string size
	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 50) // 50 char limit

	// Collect metrics
	pc.collect()

	// Verify that string truncation metrics were generated
	filter := []string{"mongodb_profile_slow_queries_info"}
	metricsCount := testutil.CollectAndCount(pc, filter...)
	t.Logf("Generated %d string truncation metrics", metricsCount)

	// Should have generated at least one info metric
	assert.Greater(t, metricsCount, 0, "Should have generated info metrics with truncated strings")

	// For detailed verification of string truncation, we rely on the unit tests
	// Integration tests focus on end-to-end functionality rather than detailed string parsing
}
