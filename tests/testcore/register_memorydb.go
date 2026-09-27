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

// memorydbAddr returns the Valkey/MemoryDB seed address. Overridable via env so a
// run can target the cluster (7001-7003) instead of standalone.
func memorydbAddr() string {
	if a := os.Getenv("TEMPORAL_MEMORYDB_TEST_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
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
}

func newMemorydbTestCluster() *memorydbTestCluster {
	return &memorydbTestCluster{
		addrs: []string{memorydbAddr()},
		// "test:<32 hex>:" — same shape as the plugin's own isolation prefix.
		keyPrefix: "test:" + randHex(16) + ":",
		// Unique in-memory DB name per run; cache=shared makes every connection
		// with this name see the same DB (required for a multi-service server).
		sqliteDB: "memorydb_vis_" + randHex(12),
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
				CustomDataStoreConfig: &config.CustomDatastoreConfig{
					Name: memorydbDriverName,
					Options: map[string]any{
						"addrs":     c.addrs,
						"keyPrefix": c.keyPrefix,
					},
				},
			},
			memorydbVisibilityStore: {
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

// TearDownTestDatabase scan-deletes only this run's keys from Valkey. The SQLite
// in-memory DB is reclaimed when the server closes its connections.
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
func newMemorydbTestBase(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	logger := options.Logger
	if logger == nil {
		logger = log.NewTestLogger()
	}
	tb := persistencetests.NewTestBaseForCluster(newMemorydbTestCluster(), logger)
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
