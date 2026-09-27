package testcore

import (
	"fmt"
	"sort"
	"sync"

	persistenceclient "go.temporal.io/server/common/persistence/client"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

// customPersistenceDriver holds everything the functional harness needs to run
// against a datastore that lives outside the Temporal core. Because core cannot
// import a third-party plugin, an out-of-tree plugin author registers one of
// these from a small build-tagged file they add to their own core checkout (see
// docs/proposals/custom-persistence-functional-tests.md).
type customPersistenceDriver struct {
	// factory is the plugin's AbstractDataStoreFactory. It is passed to the
	// server (via WithCustomDataStoreFactory) and to the persistence TestBase so
	// that a DataStore with a CustomDataStoreConfig resolves to the plugin.
	factory persistenceclient.AbstractDataStoreFactory
	// useSQLVisibility reports whether the plugin serves visibility through the
	// SQL visibility store. When false, the harness provisions Elasticsearch for
	// visibility, matching the Cassandra behavior.
	useSQLVisibility bool
	// newTestBase builds a persistence TestBase whose DefaultTestCluster returns
	// a config.Persistence with the default store pointing at a
	// CustomDataStoreConfig, and whose SetupTestDatabase/TearDownTestDatabase
	// manage the plugin's backing store. It has the same shape as
	// persistenceTestBaseFactory.NewTestBase, so the harness can use it in place
	// of the built-in cassandra/sql construction.
	newTestBase func(options *persistencetests.TestBaseOptions) *persistencetests.TestBase
}

var (
	customPersistenceDriversMu sync.RWMutex
	customPersistenceDrivers   = map[string]customPersistenceDriver{}
)

// RegisterCustomPersistenceDriver makes an out-of-tree persistence plugin
// selectable by the functional test harness via `-persistenceDriver=<name>`.
//
// It is meant to be called from an init() in a build-tagged file that a plugin
// author adds to their copy of the Temporal core checkout; core itself never
// imports the plugin. See docs/proposals/custom-persistence-functional-tests.md
// for a copy-paste example.
//
//   - name is the value passed to `-persistenceDriver`.
//   - factory is the plugin's AbstractDataStoreFactory.
//   - useSQLVisibility reports whether the plugin serves visibility through SQL
//     (false means the harness provisions Elasticsearch, as it does for Cassandra).
//   - newTestBase builds a persistence TestBase configured for the plugin (see
//     customPersistenceDriver.newTestBase).
//
// Registering the same name twice panics; this is a programming error, not a
// recoverable condition.
func RegisterCustomPersistenceDriver(
	name string,
	factory persistenceclient.AbstractDataStoreFactory,
	useSQLVisibility bool,
	newTestBase func(options *persistencetests.TestBaseOptions) *persistencetests.TestBase,
) {
	if name == "" {
		panic("testcore: custom persistence driver name must not be empty")
	}
	if factory == nil {
		panic(fmt.Sprintf("testcore: custom persistence driver %q registered with nil factory", name))
	}
	if newTestBase == nil {
		panic(fmt.Sprintf("testcore: custom persistence driver %q registered with nil newTestBase", name))
	}

	customPersistenceDriversMu.Lock()
	defer customPersistenceDriversMu.Unlock()
	if _, ok := customPersistenceDrivers[name]; ok {
		panic(fmt.Sprintf("testcore: custom persistence driver %q already registered", name))
	}
	customPersistenceDrivers[name] = customPersistenceDriver{
		factory:          factory,
		useSQLVisibility: useSQLVisibility,
		newTestBase:      newTestBase,
	}
}

// lookupCustomPersistenceDriver returns the driver registered under name, if any.
func lookupCustomPersistenceDriver(name string) (customPersistenceDriver, bool) {
	customPersistenceDriversMu.RLock()
	defer customPersistenceDriversMu.RUnlock()
	d, ok := customPersistenceDrivers[name]
	return d, ok
}

// registeredCustomPersistenceDriverNames returns the sorted names of registered
// custom drivers, for error messages.
func registeredCustomPersistenceDriverNames() []string {
	customPersistenceDriversMu.RLock()
	defer customPersistenceDriversMu.RUnlock()
	names := make([]string, 0, len(customPersistenceDrivers))
	for name := range customPersistenceDrivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
