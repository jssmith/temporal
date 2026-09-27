//go:build memorydb_functional

// Package testcore (memorydb_functional build): registers the MemoryDB/Valkey
// persistence plugin as a selectable functional-test driver.
//
// Build with `-tags memorydb_functional` and select with
// `-persistenceDriver=memorydb`. See
// docs/proposals/custom-persistence-functional-tests.md for the seams this uses.
//
// Layout produced by this driver:
//   - default store  -> the plugin (Valkey at 127.0.0.1:6379 by default), a
//     CustomDataStoreConfig resolved by plugin.NewAbstractDataStoreFactory().
//   - visibility store -> an in-memory SQLite database (mode=memory,
//     cache=shared), NOT Elasticsearch. Registered with useSQLVisibility=true so
//     the harness uses this SQL visibility store as-is instead of provisioning ES.
//
// Isolation: the plugin store uses a per-run random keyPrefix so concurrent runs
// share one Valkey without colliding (no FLUSHALL). The SQLite visibility store
// uses a per-run random database name so each run gets its own in-memory DB.
package testcore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	plugin "github.com/jssmith/temporal-memorydb-plugin"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/primitives"
)

const (
	memorydbDriverName = "memorydb"

	// memorydbDefaultStore is the name of the plugin-backed default datastore.
	memorydbDefaultStore = "memorydb-default"
	// memorydbVisibilityStore is the name of the SQLite visibility datastore.
	memorydbVisibilityStore = "memorydb-visibility"

	// sqlitePluginName must match the registered sqlite sql plugin name. Hardcoded
	// to avoid importing the sqlite package here purely for a constant.
	sqlitePluginName = "sqlite"
)

// memorydbAddrs returns the Valkey/MemoryDB seed addresses. It mirrors the
// plugin's own testAddrs() helper so the harness can target the same fixtures:
//
//   - TEMPORAL_MEMORYDB_TEST_MODE=cluster selects cluster mode, seeded from a
//     comma-separated TEMPORAL_MEMORYDB_TEST_ADDRS (default the local
//     three-primary fixture 7001-7003). redis.NewUniversalClient only builds a
//     ClusterClient when given two or more addresses, so cluster mode must pass
//     the full seed list — a single address yields a standalone client and MOVED
//     errors against a real cluster.
//   - otherwise standalone, from TEMPORAL_MEMORYDB_TEST_ADDR (default
//     127.0.0.1:6379).
func memorydbAddrs() []string {
	if strings.EqualFold(os.Getenv("TEMPORAL_MEMORYDB_TEST_MODE"), "cluster") {
		raw := os.Getenv("TEMPORAL_MEMORYDB_TEST_ADDRS")
		if raw == "" {
			raw = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"
		}
		var addrs []string
		for _, a := range strings.Split(raw, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		return addrs
	}
	if a := os.Getenv("TEMPORAL_MEMORYDB_TEST_ADDR"); a != "" {
		return []string{a}
	}
	return []string{"127.0.0.1:6379"}
}

// randHex returns n random bytes as lowercase hex. Panics on failure: a failed
// crypto/rand read is not a recoverable test condition.
func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("testcore: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// memorydbTestCluster implements persistencetests.PersistenceTestCluster. It owns
// the config.Persistence (plugin default store + SQLite visibility store) and the
// setup/teardown of the plugin's backing Valkey. The SQLite in-memory DB needs no
// explicit setup: the sqlite plugin builds its schema on first connect for
// mode=memory, and the DB is discarded when its connections close.
type memorydbTestCluster struct {
	addrs     []string
	keyPrefix string
	sqliteDB  string
	// faultInjection, when non-nil, is attached to both datastores so the
	// harness's fault-injection wrapper is applied — matching what the built-in
	// SQL/Cassandra test clusters do (see sql.TestCluster.Config). Fault-injection
	// suites (e.g. dlq, acquire-shard) pass this via WithPersistenceFaultInjection.
	faultInjection *config.FaultInjection
}

func newMemorydbTestCluster(faultInjection *config.FaultInjection) *memorydbTestCluster {
	return &memorydbTestCluster{
		addrs: memorydbAddrs(),
		// "test:<32 hex>:" — same shape as the plugin's own isolation prefix.
		keyPrefix: "test:" + randHex(16) + ":",
		// Unique in-memory DB name per run; cache=shared makes every connection
		// with this name see the same DB (required for a multi-service server).
		sqliteDB:       "memorydb_vis_" + randHex(12),
		faultInjection: faultInjection,
	}
}

// Config returns the persistence config the harness feeds to both the TestBase's
// factory and the server: plugin default store + SQLite visibility store.
func (c *memorydbTestCluster) Config() config.Persistence {
	return config.Persistence{
		DefaultStore:    memorydbDefaultStore,
		VisibilityStore: memorydbVisibilityStore,
		// NumHistoryShards is overridden from HistoryConfig by the harness; a
		// nonzero value here satisfies the `validate:"nonzero"` tag.
		NumHistoryShards: 1,
		DataStores: map[string]config.DataStore{
			memorydbDefaultStore: {
				FaultInjection: c.faultInjection,
				CustomDataStoreConfig: &config.CustomDatastoreConfig{
					Name: memorydbDriverName,
					Options: map[string]any{
						"addrs":     c.addrs,
						"keyPrefix": c.keyPrefix,
					},
				},
			},
			memorydbVisibilityStore: {
				FaultInjection: c.faultInjection,
				SQL: &config.SQL{
					PluginName:         sqlitePluginName,
					DatabaseName:       c.sqliteDB,
					ConnectAddr:        "127.0.0.1:0",
					ConnectProtocol:    "tcp",
					TaskScanPartitions: 4,
					// mode=memory: schema auto-created on connect; cache=shared:
					// one in-memory DB shared across the server's connections.
					ConnectAttributes: map[string]string{
						"mode":  "memory",
						"cache": "shared",
					},
				},
			},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

// SetupTestDatabase verifies Valkey connectivity only. It never flushes: the
// instance is shared and isolation is by keyPrefix. The SQLite visibility DB needs
// no setup (schema is built on first connect).
func (c *memorydbTestCluster) SetupTestDatabase() {
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:          c.addrs,
		RouteByLatency: false,
		RouteRandomly:  false,
	})
	defer func() { _ = client.Close() }()
	if err := client.Ping(context.Background()).Err(); err != nil {
		// PersistenceTestCluster.SetupTestDatabase has no error return; a panic
		// surfaces as a failed setup.
		panic(fmt.Sprintf("testcore: memorydb SetupTestDatabase PING %v failed: %v", c.addrs, err))
	}
}

// TearDownTestDatabase scan-deletes only this run's keys from Valkey.
//
// The SQLite visibility DB is left to the sqlite plugin's connection pool. That
// pool intentionally keeps one open connection per DSN for the process lifetime
// (see common/persistence/sql/sqlplugin/sqlite/conn_pool.go), so an in-memory DB
// is not reclaimed until the test binary exits. This matches the built-in sqlite
// functional driver exactly (GetSQLiteMemoryTestClusterOption also mints a unique
// per-cluster in-memory DB), so there is no cleanup we can do here that the
// reference driver does not. In practice these DBs hold only the schema plus the
// short-lived rows for one run's workflows, and each `go test` invocation is its
// own process, so retention is bounded and small.
func (c *memorydbTestCluster) TearDownTestDatabase() {
	deleteMemorydbPrefix(c.addrs, c.keyPrefix)
}

// deleteMemorydbPrefix scan-deletes every key under prefix across all masters. It
// refuses an empty prefix so it can never wipe the shared instance. Best-effort:
// a leaked test key is harmless since the next run uses a fresh prefix.
func deleteMemorydbPrefix(addrs []string, prefix string) {
	if prefix == "" {
		panic("testcore: deleteMemorydbPrefix refusing to run with an empty prefix")
	}
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:          addrs,
		RouteByLatency: false,
		RouteRandomly:  false,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	pattern := prefix + "*"
	keys, err := memorydbScanAll(ctx, client, pattern)
	if err != nil {
		return
	}
	const batch = 500
	for i := 0; i < len(keys); i += batch {
		end := i + batch
		if end > len(keys) {
			end = len(keys)
		}
		pipe := client.Pipeline()
		for _, k := range keys[i:end] {
			pipe.Unlink(ctx, k)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			pipe := client.Pipeline()
			for _, k := range keys[i:end] {
				pipe.Del(ctx, k)
			}
			_, _ = pipe.Exec(ctx)
		}
	}
}

func memorydbScanAll(ctx context.Context, client redis.UniversalClient, pattern string) ([]string, error) {
	if cc, ok := client.(*redis.ClusterClient); ok {
		var (
			mu   sync.Mutex
			keys []string
		)
		err := cc.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
			found, err := memorydbScanOne(ctx, m, pattern)
			if err != nil {
				return err
			}
			mu.Lock()
			keys = append(keys, found...)
			mu.Unlock()
			return nil
		})
		return keys, err
	}
	return memorydbScanOne(ctx, client, pattern)
}

func memorydbScanOne(ctx context.Context, client redis.Cmdable, pattern string) ([]string, error) {
	var (
		out    []string
		cursor uint64
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return out, nil
}

// newMemorydbTestBase builds a TestBase whose DefaultTestCluster is the memorydb
// cluster above and whose AbstractDataStoreFactory is the real plugin adapter.
//
// options.FaultInjection carries any fault-injection config the harness resolved
// (from -enableFaultInjection or a suite's WithPersistenceFaultInjection); it is
// threaded into the datastores so the fault-injection wrapper is actually applied,
// matching the built-in cassandra/sql path.
func newMemorydbTestBase(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	logger := options.Logger
	if logger == nil {
		logger = log.NewTestLogger()
	}
	tb := persistencetests.NewTestBaseForCluster(newMemorydbTestCluster(options.FaultInjection), logger)
	tb.AbstractDataStoreFactory = plugin.NewAbstractDataStoreFactory()
	return tb
}

func init() {
	// The plugin serves the default store; SQLite serves visibility. Register with
	// useSQLVisibility=true so the harness uses the SQLite visibility store from
	// Config() as-is instead of provisioning Elasticsearch.
	RegisterCustomPersistenceDriver(
		memorydbDriverName,
		plugin.NewAbstractDataStoreFactory(),
		true, // useSQLVisibility
		newMemorydbTestBase,
	)
}
