package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

type fakeAbstractDataStoreFactory struct{}

func (fakeAbstractDataStoreFactory) NewFactory(
	config.CustomDatastoreConfig,
	resolver.ServiceResolver,
	string,
	log.Logger,
	metrics.Handler,
	serialization.Serializer,
) persistence.DataStoreFactory {
	return nil
}

func TestRegisterCustomPersistenceDriver(t *testing.T) {
	const name = "unit-test-custom-driver"
	factory := fakeAbstractDataStoreFactory{}
	called := false
	newTestBase := func(*persistencetests.TestBaseOptions) *persistencetests.TestBase {
		called = true
		return nil
	}

	RegisterCustomPersistenceDriver(name, factory, true, newTestBase)
	t.Cleanup(func() {
		customPersistenceDriversMu.Lock()
		delete(customPersistenceDrivers, name)
		customPersistenceDriversMu.Unlock()
	})

	d, ok := lookupCustomPersistenceDriver(name)
	require.True(t, ok)
	require.True(t, d.useSQLVisibility)
	require.Equal(t, factory, d.factory)
	require.NotNil(t, d.newTestBase)
	d.newTestBase(nil)
	require.True(t, called)

	_, ok = lookupCustomPersistenceDriver("no-such-driver")
	require.False(t, ok)
}

func TestUseSQLVisibilityHonorsRegisteredDriver(t *testing.T) {
	const name = "unit-test-visibility-driver"
	RegisterCustomPersistenceDriver(name, fakeAbstractDataStoreFactory{}, true, func(*persistencetests.TestBaseOptions) *persistencetests.TestBase {
		return nil
	})
	t.Cleanup(func() {
		customPersistenceDriversMu.Lock()
		delete(customPersistenceDrivers, name)
		customPersistenceDriversMu.Unlock()
	})

	prev := cliFlags.persistenceDriver
	t.Cleanup(func() { cliFlags.persistenceDriver = prev })

	cliFlags.persistenceDriver = name
	require.True(t, UseSQLVisibility())

	// An unregistered driver falls through to false (Cassandra-style ES visibility).
	cliFlags.persistenceDriver = "no-such-driver"
	require.False(t, UseSQLVisibility())
}

func TestRegisterCustomPersistenceDriverRejectsDuplicate(t *testing.T) {
	const name = "unit-test-dup-driver"
	newTestBase := func(*persistencetests.TestBaseOptions) *persistencetests.TestBase { return nil }
	RegisterCustomPersistenceDriver(name, fakeAbstractDataStoreFactory{}, false, newTestBase)
	t.Cleanup(func() {
		customPersistenceDriversMu.Lock()
		delete(customPersistenceDrivers, name)
		customPersistenceDriversMu.Unlock()
	})

	require.Panics(t, func() {
		RegisterCustomPersistenceDriver(name, fakeAbstractDataStoreFactory{}, false, newTestBase)
	})
}

func TestRegisterCustomPersistenceDriverRejectsNil(t *testing.T) {
	newTestBase := func(*persistencetests.TestBaseOptions) *persistencetests.TestBase { return nil }
	require.Panics(t, func() {
		RegisterCustomPersistenceDriver("", fakeAbstractDataStoreFactory{}, false, newTestBase)
	})
	require.Panics(t, func() {
		RegisterCustomPersistenceDriver("nil-factory", nil, false, newTestBase)
	})
	require.Panics(t, func() {
		RegisterCustomPersistenceDriver("nil-testbase", fakeAbstractDataStoreFactory{}, false, nil)
	})
}
