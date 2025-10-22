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

	ti := labelsGetterMock{}

	c := newProfileCollector(ctx, client, promslog.New(&promslog.Config{}), false, ti, 30, 1000)

	expected := strings.NewReader(`
	# HELP mongodb_profile_slow_query_count profile_slow_query.count
	# TYPE mongodb_profile_slow_query_count counter
	mongodb_profile_slow_query_count{database="admin"} 0
	mongodb_profile_slow_query_count{database="config"} 0
	mongodb_profile_slow_query_count{database="local"} 0
	mongodb_profile_slow_query_count{database="testdb"} 0` +
		"\n")

	filter := []string{
		"mongodb_profile_slow_query_count",
		"mongodb_profile_slow_queries_count_total",
		"mongodb_profile_slow_queries_duration_total",
		"mongodb_profile_slow_queries_keys_examined_total",
		"mongodb_profile_slow_queries_docs_examined_total",
		"mongodb_profile_slow_queries_nreturned_total",
		"mongodb_profile_slow_queries_info",
	}

	err := testutil.CollectAndCompare(c, expected, filter...)
	assert.NoError(t, err)
}

func TestProfileCollectorQueryNormalization(t *testing.T) {
	ctx := context.Background()

	ti := labelsGetterMock{}
	c := newProfileCollector(ctx, nil, promslog.New(&promslog.Config{}), false, ti, 30, 100)

	// Test query normalization
	testCases := []struct {
		name     string
		input    primitive.M
		expected string
	}{
		{
			name: "simple find query",
			input: primitive.M{
				"find":   "users",
				"filter": primitive.M{"name": "John", "age": 25},
			},
			expected: "{find: ?, filter: {name: ?, age: ?}}",
		},
		{
			name: "nested query",
			input: primitive.M{
				"find":   "users",
				"filter": primitive.M{"address": primitive.M{"city": "NYC"}},
			},
			expected: "{find: ?, filter: {address: {city: ?}}}",
		},
		{
			name: "array query",
			input: primitive.M{
				"find":   "users",
				"filter": primitive.M{"tags": primitive.A{"tag1", "tag2"}},
			},
			expected: "{find: ?, filter: {tags: [?, ?]}}",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := c.normalizeQueryShape(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestProfileCollectorStringTruncation(t *testing.T) {
	ctx := context.Background()

	ti := labelsGetterMock{}
	c := newProfileCollector(ctx, nil, promslog.New(&promslog.Config{}), false, ti, 30, 10)

	longString := "this is a very long string that should be truncated"
	result := c.truncateString(longString, 10)

	assert.Equal(t, "this is...", result)
	assert.LessOrEqual(t, len(result), 10)
}
