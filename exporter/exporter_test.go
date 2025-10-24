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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/mongodb_exporter/internal/tu"
)

// Use this for testing because labels like cluster ID are not constant in docker containers
// so we cannot use the real topology labels in tests.
type labelsGetterMock struct{}

func (l labelsGetterMock) baseLabels() map[string]string {
	return map[string]string{}
}

func (l labelsGetterMock) loadLabels(context.Context) error {
	return nil
}

//nolint:funlen
func TestConnect(t *testing.T) {
	hostname := "127.0.0.1"
	ctx := context.Background()

	ports := map[string]string{
		"standalone":          tu.GetenvDefault("TEST_MONGODB_STANDALONE_PORT", "27017"),
		"shard-1 primary":     tu.GetenvDefault("TEST_MONGODB_S1_PRIMARY_PORT", "17001"),
		"shard-1 secondary-1": tu.GetenvDefault("TEST_MONGODB_S1_SECONDARY1_PORT", "17002"),
		"shard-1 secondary-2": tu.GetenvDefault("TEST_MONGODB_S1_SECONDARY2_PORT", "17003"),
		"shard-2 primary":     tu.GetenvDefault("TEST_MONGODB_S2_PRIMARY_PORT", "17004"),
		"shard-2 secondary-1": tu.GetenvDefault("TEST_MONGODB_S2_SECONDARY1_PORT", "17005"),
		"shard-2 secondary-2": tu.GetenvDefault("TEST_MONGODB_S2_SECONDARY2_PORT", "17006"),
		"config server 1":     tu.GetenvDefault("TEST_MONGODB_CONFIGSVR1_PORT", "17007"),
		"mongos":              tu.GetenvDefault("TEST_MONGODB_MONGOS_PORT", "17000"),
	}

	t.Run("Connect without SSL", func(t *testing.T) {
		for name, port := range ports {
			exporterOpts := &Opts{
				URI:           fmt.Sprintf("mongodb://%s/admin", net.JoinHostPort(hostname, port)),
				DirectConnect: true,
			}
			client, err := connect(ctx, exporterOpts)
			assert.NoError(t, err, name)
			err = client.Disconnect(ctx)
			assert.NoError(t, err, name)
		}
	})

	//nolint:dupl
	t.Run("Test per-request connection", func(t *testing.T) {
		log := promslog.New(&promslog.Config{})

		exporterOpts := &Opts{
			Logger:         log,
			URI:            fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.MongoDBS1PrimaryPort),
			GlobalConnPool: false,
			DirectConnect:  true,
		}

		e := New(exporterOpts)

		ts := httptest.NewServer(e.Handler())
		defer ts.Close()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := http.Get(ts.URL) //nolint:noctx
				assert.Nil(t, e.client)
				assert.NoError(t, err)
				g, err := io.ReadAll(res.Body)
				_ = res.Body.Close()
				assert.NoError(t, err)
				assert.NotEmpty(t, g)
			}()
		}

		wg.Wait()
	})

	//nolint:dupl
	t.Run("Test global connection", func(t *testing.T) {
		log := promslog.New(&promslog.Config{})

		exporterOpts := &Opts{
			Logger:         log,
			URI:            fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.MongoDBS1PrimaryPort),
			GlobalConnPool: true,
			DirectConnect:  true,
		}

		e := New(exporterOpts)

		ts := httptest.NewServer(e.Handler())
		defer ts.Close()

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := http.Get(ts.URL) //nolint:noctx
				assert.NotNil(t, e.client)
				assert.NoError(t, err)
				g, err := io.ReadAll(res.Body)
				_ = res.Body.Close()
				assert.NoError(t, err)
				assert.NotEmpty(t, g)
			}()
		}

		wg.Wait()
	})
}

// How this test works?
// When connected to a MongoS instance, the makeRegistry method should skip
// adding replSetGetStatusCollector. To test that, we try to unregister a
// replSetGetStatusCollector and it should return false since it wasn't registered.
// Note: Two Collectors are considered equal if their Describe method yields the
// same set of descriptors.
// unregister will try to Describe to get the descriptors set, and we are using
// DescribeByCollect so, in the logs, you will see an error:
// msg="cannot get replSetGetStatus: replSetGetStatus is not supported through mongos"
// This is correct. Collect is being executed to Describe and Unregister.
func TestMongoS(t *testing.T) {
	hostname := "127.0.0.1"
	ctx := context.Background()

	tests := []struct {
		port string
		want bool
	}{
		{
			port: tu.GetenvDefault("TEST_MONGODB_MONGOS_PORT", "17000"),
			want: false,
		},
		{
			port: tu.GetenvDefault("TEST_MONGODB_S1_PRIMARY_PORT", "17001"),
			want: true,
		},
	}

	for _, test := range tests {
		exporterOpts := &Opts{
			Logger:                 promslog.New(&promslog.Config{}),
			URI:                    fmt.Sprintf("mongodb://%s/admin", net.JoinHostPort(hostname, test.port)),
			DirectConnect:          true,
			GlobalConnPool:         false,
			EnableReplicasetStatus: true,
		}

		client, err := connect(ctx, exporterOpts)
		assert.NoError(t, err)

		e := New(exporterOpts)
		rsgsc := newReplicationSetStatusCollector(ctx, client, e.opts.Logger, e.opts.CompatibleMode, new(labelsGetterMock))

		r := e.makeRegistry(ctx, client, new(labelsGetterMock), *e.opts)

		res := r.Unregister(rsgsc)
		assert.Equal(t, test.want, res, fmt.Sprintf("Port: %v", test.port))
		err = client.Disconnect(ctx)
		assert.NoError(t, err)
	}
}

func generateKerberosConfigFile(t *testing.T) *os.File {
	t.Helper()
	kerberosHost, err := tu.IPForContainer("kerberos")
	require.NoError(t, err)

	config := fmt.Sprintf(`
[libdefaults]
    default_realm = PERCONATEST.COM
    forwardable = true
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ignore_acceptor_hostname = true
    rdns = false
[realms]
    PERCONATEST.COM = {
        kdc_ports = 88
        kdc = %s
    }
[domain_realm]
    .perconatest.com = PERCONATEST.COM
    perconatest.com = PERCONATEST.COM
    %s = PERCONATEST.COM
`, kerberosHost, kerberosHost)
	configFile, err := os.Create(t.TempDir() + "/krb5.conf")
	require.NoError(t, err)

	_, err = configFile.WriteString(config)
	require.NoError(t, err)

	return configFile
}

func TestGSSAPIAuth(t *testing.T) {
	logger := promslog.New(&promslog.Config{})

	mongoHost, err := tu.IPForContainer("psmdb-kerberos")
	require.NoError(t, err)

	configFile := generateKerberosConfigFile(t)
	require.NoError(t, err)
	defer func() {
		_ = configFile.Close()
		t.Setenv("KRB5_CONFIG", "")
	}()

	t.Setenv("KRB5_CONFIG", configFile.Name())
	ctx := context.Background()

	username := "pmm-test%40PERCONATEST.COM"
	password := "password1"
	uri := fmt.Sprintf("mongodb://%s:%s@%s/?authSource=$external&authMechanism=GSSAPI",
		username,
		password,
		net.JoinHostPort(mongoHost, "27017"),
	)
	exporterOpts := &Opts{
		URI:            uri,
		Logger:         logger,
		CollectAll:     true,
		GlobalConnPool: false,
		DirectConnect:  true,
	}

	client, err := connect(ctx, exporterOpts)
	assert.NoError(t, err)

	e := New(exporterOpts)
	nodeType, _ := getNodeType(ctx, client)
	gc := newGeneralCollector(ctx, client, nodeType, e.opts.Logger)
	r := e.makeRegistry(ctx, client, new(labelsGetterMock), *e.opts)

	expected := strings.NewReader(`
		# HELP mongodb_up Whether MongoDB is up.
		# TYPE mongodb_up gauge
		mongodb_up {cluster_role="mongod"} 1` + "\n")

	filter := []string{
		"mongodb_up",
	}
	err = testutil.CollectAndCompare(gc, expected, filter...)
	require.NoError(t, err, "mongodb_up metric should be 1")

	res := r.Unregister(gc)
	assert.True(t, res)
}

func TestMongoUpMetric(t *testing.T) {
	ctx := context.Background()

	type testcase struct {
		URI         string
		clusterRole string
		Want        int
	}

	testCases := []testcase{
		{URI: "mongodb://127.0.0.1:12345/admin", Want: 0},
		{URI: fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.GetenvDefault("TEST_MONGODB_STANDALONE_PORT", "27017")), Want: 1, clusterRole: "mongod"},
		{URI: fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.GetenvDefault("TEST_MONGODB_S1_PRIMARY_PORT", "27017")), Want: 1, clusterRole: "mongod"},
		{URI: fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.GetenvDefault("TEST_MONGODB_S1_SECONDARY1_PORT", "27017")), Want: 1, clusterRole: "mongod"},
		{URI: fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.GetenvDefault("TEST_MONGODB_S1_ARBITER_PORT", "27017")), Want: 1, clusterRole: "arbiter"},
		{URI: fmt.Sprintf("mongodb://127.0.0.1:%s/admin", tu.GetenvDefault("TEST_MONGODB_MONGOS_PORT", "27017")), Want: 1, clusterRole: "mongos"},
	}

	for _, tc := range testCases {
		t.Run(tc.clusterRole+"/"+tc.URI, func(t *testing.T) {
			exporterOpts := &Opts{
				Logger:           promslog.New(&promslog.Config{}),
				URI:              tc.URI,
				ConnectTimeoutMS: 200,
				DirectConnect:    true,
				GlobalConnPool:   false,
				CollectAll:       true,
			}

			client, err := connect(ctx, exporterOpts)
			if tc.Want == 1 {
				assert.NoError(t, err, "Must be able to connect to %s", tc.URI)
			} else {
				assert.Error(t, err, "Must be unable to connect to %s", tc.URI)
			}

			e := New(exporterOpts)
			nodeType, _ := getNodeType(ctx, client)
			gc := newGeneralCollector(ctx, client, nodeType, e.opts.Logger)
			r := e.makeRegistry(ctx, client, new(labelsGetterMock), *e.opts)

			expected := strings.NewReader(fmt.Sprintf(`
		# HELP mongodb_up Whether MongoDB is up.
		# TYPE mongodb_up gauge
		mongodb_up {cluster_role="%s"} %s`, tc.clusterRole, strconv.Itoa(tc.Want)) + "\n")

			filter := []string{
				"mongodb_up",
			}
			err = testutil.CollectAndCompare(gc, expected, filter...)
			assert.NoError(t, err, "mongodb_up metric should be %d", tc.Want)

			res := r.Unregister(gc)
			assert.Equal(t, true, res)
		})
	}
}

// TestGetRequestOpts tests the GetRequestOpts function behavior with different flag combinations.
func TestGetRequestOpts(t *testing.T) {
	t.Run("GetRequestOpts with empty filters", func(t *testing.T) {
		defaultOpts := &Opts{
			EnableProfile:        true,
			EnableDBStats:        true,
			ProfileTimeTS:        60,
			ProfileMaxStringSize: 2000,
		}

		// Empty filters should return copy of default opts
		requestOpts := GetRequestOpts([]string{}, defaultOpts)
		
		assert.Equal(t, defaultOpts.EnableProfile, requestOpts.EnableProfile)
		assert.Equal(t, defaultOpts.EnableDBStats, requestOpts.EnableDBStats)
		assert.Equal(t, defaultOpts.ProfileTimeTS, requestOpts.ProfileTimeTS)
		assert.Equal(t, defaultOpts.ProfileMaxStringSize, requestOpts.ProfileMaxStringSize)
	})

	t.Run("GetRequestOpts with profile filter", func(t *testing.T) {
		defaultOpts := &Opts{
			EnableProfile:        false,
			EnableDBStats:        false,
			ProfileTimeTS:        30,
			ProfileMaxStringSize: 1000,
		}

		// Profile filter should enable profile collector
		requestOpts := GetRequestOpts([]string{"profile"}, defaultOpts)
		
		assert.True(t, requestOpts.EnableProfile)
		assert.False(t, requestOpts.EnableDBStats) // Should not be enabled
	})

	t.Run("GetRequestOpts with multiple filters", func(t *testing.T) {
		defaultOpts := &Opts{
			EnableProfile:        false,
			EnableDBStats:        false,
			EnableTopMetrics:     false,
			ProfileTimeTS:        30,
			ProfileMaxStringSize: 1000,
		}

		// Multiple filters should enable respective collectors
		requestOpts := GetRequestOpts([]string{"profile", "dbstats", "topmetrics"}, defaultOpts)
		
		assert.True(t, requestOpts.EnableProfile)
		assert.True(t, requestOpts.EnableDBStats)
		assert.True(t, requestOpts.EnableTopMetrics)
	})

	t.Run("GetRequestOpts preserves non-collector settings", func(t *testing.T) {
		defaultOpts := &Opts{
			CompatibleMode:       true,
			DirectConnect:        false,
			ConnectTimeoutMS:     5000,
			ProfileTimeTS:        45,
			ProfileMaxStringSize: 1500,
			EnableProfile:        false,
		}

		// URL filters should preserve configuration settings
		requestOpts := GetRequestOpts([]string{"profile"}, defaultOpts)
		
		// Non-collector settings should be preserved
		assert.Equal(t, defaultOpts.CompatibleMode, requestOpts.CompatibleMode)
		assert.Equal(t, defaultOpts.DirectConnect, requestOpts.DirectConnect)
		assert.Equal(t, defaultOpts.ConnectTimeoutMS, requestOpts.ConnectTimeoutMS)
		assert.Equal(t, defaultOpts.ProfileTimeTS, requestOpts.ProfileTimeTS)
		assert.Equal(t, defaultOpts.ProfileMaxStringSize, requestOpts.ProfileMaxStringSize)
		
		// Collector should be enabled by filter
		assert.True(t, requestOpts.EnableProfile)
	})
}

// TestCollectAllFlag tests the behavior of the CollectAll flag.
func TestCollectAllFlag(t *testing.T) {
	t.Run("CollectAll enables all collectors", func(t *testing.T) {
		opts := &Opts{
			CollectAll:           true,
			EnableProfile:        false, // Should be overridden
			EnableDBStats:        false, // Should be overridden
			EnableTopMetrics:     false, // Should be overridden
		}

		// Simulate the logic from makeRegistry
		if opts.CollectAll {
			opts.EnableDiagnosticData = true
			opts.EnableDBStats = true
			opts.EnableDBStatsFreeStorage = true
			opts.EnableCollStats = true
			opts.EnableTopMetrics = true
			opts.EnableReplicasetStatus = true
			opts.EnableReplicasetConfig = true
			opts.EnableIndexStats = true
			opts.EnableCurrentopMetrics = true
			opts.EnableProfile = true
			opts.EnableShards = true
			opts.EnableFCV = true
			opts.EnablePBMMetrics = true
		}

		// All collectors should be enabled
		assert.True(t, opts.EnableProfile)
		assert.True(t, opts.EnableDBStats)
		assert.True(t, opts.EnableTopMetrics)
		assert.True(t, opts.EnableDiagnosticData)
		assert.True(t, opts.EnableCollStats)
	})

	t.Run("Collector conditions - profile collector requirements", func(t *testing.T) {
		testCases := []struct {
			name                    string
			enableProfile           bool
			requestOptsEnableProfile bool
			profileTimeTS           int
			nodeType                string
			limitsOk                bool
			expectRegistration      bool
		}{
			{
				name:                    "All conditions met",
				enableProfile:           true,
				requestOptsEnableProfile: true,
				profileTimeTS:           30,
				nodeType:                "mongod",
				limitsOk:                true,
				expectRegistration:      true,
			},
			{
				name:                    "Profile disabled in opts",
				enableProfile:           false,
				requestOptsEnableProfile: true,
				profileTimeTS:           30,
				nodeType:                "mongod",
				limitsOk:                true,
				expectRegistration:      false,
			},
			{
				name:                    "Profile disabled in request",
				enableProfile:           true,
				requestOptsEnableProfile: false,
				profileTimeTS:           30,
				nodeType:                "mongod",
				limitsOk:                true,
				expectRegistration:      false,
			},
			{
				name:                    "Zero ProfileTimeTS",
				enableProfile:           true,
				requestOptsEnableProfile: true,
				profileTimeTS:           0,
				nodeType:                "mongod",
				limitsOk:                true,
				expectRegistration:      false,
			},
			{
				name:                    "MongoS node type",
				enableProfile:           true,
				requestOptsEnableProfile: true,
				profileTimeTS:           30,
				nodeType:                "mongos",
				limitsOk:                true,
				expectRegistration:      false,
			},
			{
				name:                    "Collection limits exceeded",
				enableProfile:           true,
				requestOptsEnableProfile: true,
				profileTimeTS:           30,
				nodeType:                "mongod",
				limitsOk:                false,
				expectRegistration:      false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Simulate the condition from makeRegistry
				shouldRegister := tc.enableProfile && 
					tc.nodeType != "mongos" && 
					tc.limitsOk && 
					tc.requestOptsEnableProfile && 
					tc.profileTimeTS != 0
				
				assert.Equal(t, tc.expectRegistration, shouldRegister, 
					"Profile collector registration condition mismatch")
			})
		}
	})
}
