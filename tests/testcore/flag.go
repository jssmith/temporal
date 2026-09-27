package testcore

import (
	"flag"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
)

// cliFlags contains the feature flags for functional tests
var cliFlags struct {
	persistenceType      string
	persistenceDriver    string
	enableFaultInjection string
}

func init() {
	flag.StringVar(&cliFlags.persistenceType, "persistenceType", "sql", "type of persistence - [nosql or sql]")
	flag.StringVar(&cliFlags.persistenceDriver, "persistenceDriver", "sqlite", "driver of nosql/sql - [cassandra, mysql8, postgres12, sqlite, or a registered custom driver]")
	flag.StringVar(&cliFlags.enableFaultInjection, "enableFaultInjection", "", "enable global fault injection")
}

func UseSQLVisibility() bool {
	switch cliFlags.persistenceDriver {
	case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
		return true
	// If the main storage is Cassandra, Elasticsearch is used for visibility.
	default:
		// A custom persistence driver (registered via RegisterCustomPersistenceDriver)
		// declares its own visibility mode. Unknown drivers fall through to false,
		// matching Cassandra; NewTestBase fails hard if the driver is not registered.
		if d, ok := lookupCustomPersistenceDriver(cliFlags.persistenceDriver); ok {
			return d.useSQLVisibility
		}
		return false
	}
}

func UseCassandraPersistence() bool {
	return cliFlags.persistenceType == config.StoreTypeNoSQL && cliFlags.persistenceDriver == "cassandra"
}
