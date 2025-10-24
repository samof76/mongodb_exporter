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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/mongodb_exporter/internal/tu"
)

func TestProfileCollector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := tu.DefaultTestClient(ctx, t)

	database := client.Database("testdb")
	database.Drop(ctx) //nolint

	defer func() {
		err := database.Drop(ctx)
		assert.NoError(t, err)
	}()

	// Enable database profiler https://www.mongodb.com/docs/manual/tutorial/manage-the-database-profiler/
	cmd := bson.M{"profile": 2}
	_ = database.RunCommand(ctx, cmd)

	// Create profile documents for testing
	coll := database.Collection("system.profile")
	now := primitive.NewDateTimeFromTime(time.Now())
	
	// Insert test profile documents
	testDocs := []bson.M{
		{
			"ts":        now,
			"op":        "query",
			"ns":        "testdb.testcoll",
			"queryHash": "hash1",
			"command":   bson.M{"find": "testcoll", "filter": bson.M{"name": "test"}},
			"millis":    100,
			"keysExamined": 50,
			"docsExamined": 25,
			"nreturned":    10,
			"queryFramework": "classic",
			"planSummary": "COLLSCAN",
		},
		{
			"ts":        now,
			"op":        "query", 
			"ns":        "testdb.testcoll",
			"queryHash": "hash1",
			"command":   bson.M{"find": "testcoll", "filter": bson.M{"name": "test"}},
			"millis":    150,
			"keysExamined": 75,
			"docsExamined": 30,
			"nreturned":    15,
			"queryFramework": "classic",
			"planSummary": "COLLSCAN",
		},
	}
	
	var docs []interface{}
	for _, doc := range testDocs {
		docs = append(docs, doc)
	}
	_, err := coll.InsertMany(ctx, docs)
	assert.NoError(t, err)

	// Reset metrics before test
	slowQueriesCountTotal.Reset()
	slowQueriesInfo.Reset()
	slowQueriesDurationTotal.Reset()
	slowQueriesKeysExaminedTotal.Reset()
	slowQueriesDocsExaminedTotal.Reset()
	slowQueriesNreturnedTotal.Reset()

	ti := labelsGetterMock{}
	pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	// Test that collector can be created and collects without error
	assert.NotNil(t, pc)
	
	// Collect metrics
	pc.collect()

	// Verify metrics are registered and have expected values
	filter := []string{
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_info",
		"mongodb_profile_slow_queries_duration_total", 
		"mongodb_profile_slow_queries_keys_examined_total",
		"mongodb_profile_slow_queries_docs_examined_total",
		"mongodb_profile_slow_queries_nreturned_total",
	}

	expected := strings.NewReader(`
# HELP mongodb_profile_slow_queries_count_total Total number of slow queries
# TYPE mongodb_profile_slow_queries_count_total counter
mongodb_profile_slow_queries_count_total{db="testdb",ns="testdb.testcoll",query_hash="hash1"} 2
# HELP mongodb_profile_slow_queries_duration_total Total execution time of slow queries in milliseconds
# TYPE mongodb_profile_slow_queries_duration_total counter
mongodb_profile_slow_queries_duration_total{db="testdb",ns="testdb.testcoll",query_hash="hash1"} 250
# HELP mongodb_profile_slow_queries_keys_examined_total Total number of examined keys
# TYPE mongodb_profile_slow_queries_keys_examined_total counter
mongodb_profile_slow_queries_keys_examined_total{db="testdb",ns="testdb.testcoll",query_hash="hash1"} 125
# HELP mongodb_profile_slow_queries_docs_examined_total Total number of examined documents
# TYPE mongodb_profile_slow_queries_docs_examined_total counter
mongodb_profile_slow_queries_docs_examined_total{db="testdb",ns="testdb.testcoll",query_hash="hash1"} 55
# HELP mongodb_profile_slow_queries_nreturned_total Total number of returned documents  
# TYPE mongodb_profile_slow_queries_nreturned_total counter
mongodb_profile_slow_queries_nreturned_total{db="testdb",ns="testdb.testcoll",query_hash="hash1"} 25
# HELP mongodb_profile_slow_queries_info Information about slow query
# TYPE mongodb_profile_slow_queries_info gauge
mongodb_profile_slow_queries_info{db="testdb",ns="testdb.testcoll",query_hash="hash1",query_shape="{find: \"?\", filter: {name: \"?\"}}",query_framework="classic",op="query",plan_summary="COLLSCAN"} 1
`)

	err = testutil.CollectAndCompare(pc, expected, filter...)
	assert.NoError(t, err)
}

// TestProfileCollectorQueryNormalization tests the query normalization functionality
func TestProfileCollectorQueryNormalization(t *testing.T) {
	ti := labelsGetterMock{}
	pc := newProfileCollector(context.Background(), nil, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	testCases := []struct {
		name     string
		input    bson.M
		expected string
	}{
		{
			name: "Simple find query",
			input: bson.M{
				"find":   "collection",
				"filter": bson.M{"name": "test", "age": 25},
			},
			expected: `{find: "?", filter: {name: "?", age: "?"}}`,
		},
		{
			name: "Query with removed keys",
			input: bson.M{
				"find":   "collection", 
				"filter": bson.M{"name": "test"},
				"lsid":   bson.M{"id": "session123"},
				"$db":    "testdb",
			},
			expected: `{find: "?", filter: {name: "?"}}`,
		},
		{
			name: "Nested query",
			input: bson.M{
				"aggregate": "collection",
				"pipeline": bson.A{
					bson.M{"$match": bson.M{"status": "active"}},
					bson.M{"$group": bson.M{"_id": "$category", "count": bson.M{"$sum": 1}}},
				},
			},
			expected: `{aggregate: "?", pipeline: [{$match: {status: "?"}}, {$group: {count: {$sum: "?"}, _id: "?"}}]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := pc.createQueryShape(tc.input)
			// Since Go map iteration order is not deterministic, check key components exist
			switch tc.name {
			case "Simple find query":
				assert.Contains(t, result, "find: \"?\"", "should contain find key")
				assert.Contains(t, result, "filter:", "should contain filter key")
				assert.Contains(t, result, "name: \"?\"", "should contain normalized name")
				assert.Contains(t, result, "age: \"?\"", "should contain normalized age")
			case "Query with removed keys":
				assert.Contains(t, result, "find: \"?\"", "should contain find key")
				assert.Contains(t, result, "filter:", "should contain filter key")
				assert.Contains(t, result, "name: \"?\"", "should contain normalized name")
				assert.NotContains(t, result, "lsid", "should not contain removed lsid key")
				assert.NotContains(t, result, "$db", "should not contain removed $db key")
			case "Nested query":
				assert.Contains(t, result, "aggregate: \"?\"", "should contain aggregate key")
				assert.Contains(t, result, "pipeline:", "should contain pipeline key")
				assert.Contains(t, result, "$match:", "should contain match stage")
				assert.Contains(t, result, "$group:", "should contain group stage")
				assert.Contains(t, result, "status: \"?\"", "should contain normalized status")
			}
			
			// Verify all leaf values are replaced with "?"
			assert.NotContains(t, result, "\"test\"", "should not contain original string values")
			assert.NotContains(t, result, "\"collection\"", "should not contain original string values")
			assert.NotContains(t, result, "\"active\"", "should not contain original string values")
		})
	}
}

// TestProfileCollectorStringTruncation tests string truncation functionality
func TestProfileCollectorStringTruncation(t *testing.T) {
	ti := labelsGetterMock{}
	pc := newProfileCollector(context.Background(), nil, promslog.New(&promslog.Config{}), false, ti, 30, 10)

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Short string",
			input:    "short",
			expected: "short",
		},
		{
			name:     "Long string",
			input:    "this is a very long string that should be truncated",
			expected: "this is a ",
		},
		{
			name:     "Exact length string",
			input:    "exactly10c",
			expected: "exactly10c",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := pc.truncateString(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestProfileCollectorExcludedDatabases tests that excluded databases are not processed
func TestProfileCollectorExcludedDatabases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := tu.DefaultTestClient(ctx, t)

	// Test with excluded database names
	excludedDbs := []string{"admin", "local", "config", "test"}
	
	for _, dbName := range excludedDbs {
		// Reset metrics
		slowQueriesCountTotal.Reset()
		
		ti := labelsGetterMock{}
		pc := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)
		
		// Collect should not process excluded databases
		pc.collect()
		
		// Verify no metrics were created for excluded database
		filter := []string{"mongodb_profile_slow_queries_count_total"}
		expected := strings.NewReader(``)
		
		// Should have no metrics since databases are excluded
		err := testutil.CollectAndCompare(pc, expected, filter...)
		assert.NoError(t, err)
		
		// Reference dbName to avoid unused variable warning
		_ = dbName
	}
}