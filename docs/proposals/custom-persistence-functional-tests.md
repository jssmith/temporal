# Running the functional test suite against a custom persistence plugin

## Summary

Temporal already supports custom datastores at runtime (`config.DataStore.Custom`
→ `persistenceClient.AbstractDataStoreFactory`), and the persistence
*conformance* suites (`common/persistence/tests`) are runnable from outside the
core repo. The *functional* (end-to-end) suite under `tests/` was not: there was
no way to select a custom datastore via `-persistenceDriver`, and no
`TestClusterOption` to hand a custom `AbstractDataStoreFactory` to the harness.

This proposal adds two small, additive extension points so an out-of-tree
persistence plugin can run the functional suite without hand-patching testcore
internals per plugin. Existing built-in drivers (`cassandra`, `mysql8`,
`postgres12`, `sqlite`) are unchanged.

## Background

The functional harness builds its entire persistence layer from a
`persistencetests.TestBase`:

- `tests/testcore/test_cluster.go` calls `tbFactory.NewTestBase(options)` to get
  a `TestBase`, then reads `TestBase.DefaultTestCluster.Config()` (the
  `config.Persistence`) and `TestBase.AbstractDataStoreFactory` (the resolver for
  a `CustomDataStoreConfig`) and forwards both to the server via
  `temporal.WithCustomDataStoreFactory`.
- For built-in drivers, `TestBase` is constructed by
  `defaultPersistenceTestBaseFactory.NewTestBase`, whose options come from
  `persistencetests.GetTestClusterOption(persistenceType, persistenceDriver)` —
  both hardcode cassandra/sql/sqlite and `panic` on anything else.
- `tests/testcore/flag.go` `UseSQLVisibility()` also hardcodes the built-in SQL
  drivers; it decides whether the harness provisions SQL visibility or
  Elasticsearch.

So a custom datastore needs three things the built-in path cannot produce:

1. a `PersistenceTestCluster` (`DefaultTestCluster`) whose `Config()` returns a
   `config.Persistence` with the default store pointing at a
   `CustomDataStoreConfig`, and whose `SetupTestDatabase`/`TearDownTestDatabase`
   manage the plugin's backing store;
2. `TestBase.AbstractDataStoreFactory` set to the plugin's factory;
3. `UseSQLVisibility()` returning the plugin's visibility mode.

Core cannot import a third-party plugin, so registration has to originate from
the plugin author's own checkout.

## Design

### 1. A driver registry (`-persistenceDriver=<name>`)

`tests/testcore/custom_persistence.go` adds a process-wide registry:

```go
func RegisterCustomPersistenceDriver(
    name string,
    factory persistenceclient.AbstractDataStoreFactory,
    useSQLVisibility bool,
    newTestBase func(options *persistencetests.TestBaseOptions) *persistencetests.TestBase,
)
```

The registry entry carries exactly the three pieces above. `newTestBase` has the
same shape as the internal `persistenceTestBaseFactory.NewTestBase`, so the
harness can substitute it for the built-in cassandra/sql construction.

Consulting the registry:

- `flag.go` `UseSQLVisibility()` returns the registered driver's
  `useSQLVisibility` for a custom driver (falling through to `false` — the
  Cassandra/Elasticsearch default — for an unregistered name).
- `test_cluster.go` `GetPersistenceTestDefaults()` returns empty options for a
  custom driver (the plugin's `newTestBase` owns the options), and
  `defaultPersistenceTestBaseFactory.NewTestBase()` delegates to the registered
  `newTestBase`.

Because core cannot import the plugin, `RegisterCustomPersistenceDriver` is
called from an `init()` in a small **build-tagged** file that the plugin author
drops into their copy of the core checkout (see below). Registration is
compile-time opt-in: with the tag absent, the file is not built and core behaves
exactly as before.

Failure mode: selecting an unregistered custom `-persistenceDriver` fails hard.
`GetTestClusterOption` still `panic`s for an unknown built-in driver, and the
registry path is skipped entirely when no driver is registered — there is no
silent fallback to a built-in store.

### 2. A `WithCustomDataStoreFactory` `TestClusterOption`

For authors who write their own testcore-based functional tests (rather than
running the stock `tests/` suite via the CLI flag),
`functional_test_base.go` adds:

```go
func WithCustomDataStoreFactory(
    main persistenceclient.AbstractDataStoreFactory,
    useSQLVisibility bool,
) TestClusterOption
```

It sets the factory and a per-cluster visibility override on `TestClusterConfig`.
At cluster build time the factory overrides `TestBase.AbstractDataStoreFactory`,
and the visibility override (captured on the `TestCluster` so teardown matches
setup) wins over the flag-derived `UseSQLVisibility()`. The author still supplies
the `PersistenceTestCluster` — typically by constructing the cluster with their
own `persistenceTestBaseFactory` (see `NewTestClusterFactory`) — because the
server needs a `config.Persistence` whose default store is a
`CustomDataStoreConfig`.

`#1` and `#2` are independent and both additive: `#1` is the zero-friction path
for running the existing suite unmodified; `#2` is for bespoke test binaries.

## Files changed

- `tests/testcore/custom_persistence.go` (new): registry +
  `RegisterCustomPersistenceDriver`.
- `tests/testcore/custom_persistence_test.go` (new): registry unit tests.
- `tests/testcore/flag.go`: `UseSQLVisibility()` consults the registry.
- `tests/testcore/test_cluster.go`: `GetPersistenceTestDefaults()` and
  `defaultPersistenceTestBaseFactory.NewTestBase()` consult the registry;
  `WithCustomDataStoreFactory` override threaded through `TestClusterConfig` and
  captured on `TestCluster` for teardown.
- `tests/testcore/functional_test_base.go`: `WithCustomDataStoreFactory` option
  and `testClusterParams` fields.

## Plugin author's build-tagged registration file

The motivating example is the MemoryDB/Valkey/Redis plugin at
`github.com/jssmith/temporal-memorydb-plugin`. The plugin already ships a
`plugin.Factory` (a `persistence.DataStoreFactory`) and a conformance harness
against `common/persistence/tests`. To run the functional suite, the author adds
**one file to their copy of the core checkout** and builds with a tag, e.g.
`go test -tags memorydb_functional ./tests/... -persistenceDriver=memorydb`.

The file supplies the two things the plugin does not already export to the
harness:

- an `AbstractDataStoreFactory` adapter (builds a `plugin.Factory` from
  `config.CustomDatastoreConfig`), and
- a `newTestBase` that returns a `TestBase` whose `DefaultTestCluster.Config()`
  points the default store at a `CustomDataStoreConfig` and whose
  setup/teardown flushes the backing Valkey.

```go
//go:build memorydb_functional

// File: tests/testcore/register_memorydb.go (in the plugin author's core checkout)
package testcore

import (
	"context"
	"os"

	"github.com/redis/go-redis/v9"
	memorydb "github.com/jssmith/temporal-memorydb-plugin"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

const (
	memorydbDriverName  = "memorydb"
	memorydbClusterName = "active" // must match the harness's current cluster name
)

func memorydbAddr() string {
	if a := os.Getenv("TEMPORAL_MEMORYDB_TEST_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:6379"
}

// abstractFactory adapts the plugin's Factory to AbstractDataStoreFactory.
type abstractFactory struct{}

func (abstractFactory) NewFactory(
	_ config.CustomDatastoreConfig,
	_ resolver.ServiceResolver,
	clusterName string,
	_ log.Logger,
	_ metrics.Handler,
	_ serialization.Serializer,
) persistence.DataStoreFactory {
	f, err := memorydb.NewFactory(
		context.Background(),
		memorydb.Options{Addrs: []string{memorydbAddr()}},
		clusterName,
	)
	if err != nil {
		panic(err)
	}
	return f
}

// memorydbTestCluster implements persistencetests.PersistenceTestCluster: it owns
// the config.Persistence (default store = CustomDataStoreConfig) and the
// setup/teardown of the backing store.
type memorydbTestCluster struct{ client redis.UniversalClient }

func (c *memorydbTestCluster) SetupTestDatabase() {
	c.client = redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{memorydbAddr()}})
	if err := c.client.FlushAll(context.Background()).Err(); err != nil {
		panic(err)
	}
}

func (c *memorydbTestCluster) TearDownTestDatabase() {
	if c.client != nil {
		_ = c.client.FlushAll(context.Background())
		_ = c.client.Close()
	}
}

func (c *memorydbTestCluster) Config() config.Persistence {
	const store = "memorydb-default"
	return config.Persistence{
		DefaultStore:    store,
		VisibilityStore: store, // only used when useSQLVisibility=false path is bypassed; ES is wired by the harness
		NumHistoryShards: 4,
		DataStores: map[string]config.DataStore{
			store: {
				CustomDataStoreConfig: &config.CustomDatastoreConfig{
					Name:      memorydbDriverName,
					IndexName: "memorydb-functional",
				},
			},
		},
	}
}

func newMemorydbTestBase(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	logger := options.Logger
	if logger == nil {
		logger = log.NewTestLogger()
	}
	tb := persistencetests.NewTestBaseForCluster(&memorydbTestCluster{}, logger)
	tb.AbstractDataStoreFactory = abstractFactory{}
	return tb
}

func init() {
	// memorydb serves visibility through Elasticsearch (like Cassandra), so
	// useSQLVisibility=false and the harness provisions ES for visibility.
	RegisterCustomPersistenceDriver(
		memorydbDriverName,
		abstractFactory{},
		false, // useSQLVisibility
		newMemorydbTestBase,
	)
}
```

Run it:

```bash
# Elasticsearch + a Valkey/Redis at 127.0.0.1:6379 must be reachable.
go test -tags "test_dep memorydb_functional" ./tests/... -persistenceDriver=memorydb
```

## Notes / what the plugin author still owns

- The example above is illustrative. The plugin author must make
  `memorydbTestCluster.Config()` return a `config.Persistence` consistent with
  what the harness expects (the default store must be a `CustomDataStoreConfig`;
  the exported factory is what resolves it). The harness overrides the visibility
  store with Elasticsearch when `useSQLVisibility=false`, and overrides
  `NumHistoryShards` from `HistoryConfig`.
- `memorydbClusterName` / cluster naming and the visibility mode must match the
  plugin's own assumptions; a Redis-backed plugin that cannot serve SQL
  visibility should register `useSQLVisibility=false` and rely on Elasticsearch.
- Core ships neither the plugin nor the build-tagged file; both live in the
  author's checkout. This proposal only adds the seams (`#1` registry, `#2`
  option) that make such a file possible.
```
