package cassandra

import (
	"fmt"
	"time"

	"github.com/Eomaxl/RaurPay/internal/config"
	"github.com/gocql/gocql"
)

// ConnectionManager manages Cassandra database connections
type ConnectionManager struct {
	session *gocql.Session
	config  config.CassandraConfig
}

// NewConnectionManager creates a new Cassandra Connection manager
func NewConnectionManager(cfg config.CassandraConfig) *ConnectionManager {
	return &ConnectionManager{
		config: cfg,
	}
}

// Connect establishes connection to cassandra cluster
func (cm *ConnectionManager) Connect() error {
	cluster := gocql.NewCluster(cm.config.Hosts...)
	cluster.Keyspace = cm.config.Keyspace
	cluster.Timeout = cm.config.Timeout
	cluster.ConnectTimeout = cm.config.ConnectTimeout
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cm.config.MaxRetries}

	// Set consistency level
	switch cm.config.Consistency {
	case "ONE":
		cluster.Consistency = gocql.One
	case "QUORUM":
		cluster.Consistency = gocql.Quorum
	case "ALL":
		cluster.Consistency = gocql.All
	case "LOCAL_QUORUM":
		cluster.Consistency = gocql.LocalQuorum
	default:
		cluster.Consistency = gocql.Quorum
	}

	// Connection pooling configuration
	cluster.NumConns = 2
	cluster.SocketKeepalive = time.Second * 60

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create cassndra session : %w", err)
	}

	cm.session = session
	return nil
}

// Session returns the active Cassandra session
func (cm *ConnectionManager) Session() *gocql.Session {
	return cm.session
}

// Close closes the Cassandra session
func (cm *ConnectionManager) Close() {
	if cm.session != nil {
		cm.session.Close()
	}
}

// Health check performs a health check on the cassandra connection
func (cm *ConnectionManager) HealthCheck() error {
	if cm.session == nil {
		return fmt.Errorf("Cassandra session is nil")
	}

	// Simple query to check connectivity
	var count int
	err := cm.session.Query("SELECT COUNT(*) FROM system.local").Scan(&count)
	if err != nil {
		return fmt.Errorf("Cassandra health check failed: %w", err)
	}

	return nil
}

// CreateKeySpace creates the keyspace if it doesn't exist
func (cm *ConnectionManager) CreateKeySpace() error {
	// Create a session without keyspace to create the keyspace
	cluster := gocql.NewCluster(cm.config.Hosts...)
	cluster.Timeout = cm.config.ConnectTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("Failed to create session for keyspace creation: %w", err)
	}
	defer session.Close()

	// Create keyspace with SimpleStrategy
	createKeyspaceQuery := fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS %s
		WITH REPLICATION = {
			'class': 'SimpleStrategy',
			'replication_factor':%d
		}`,
		cm.config.Keyspace,
		cm.config.ReplicationFactor,
	)

	err = session.Query(createKeyspaceQuery).Exec()
	if err != nil {
		return fmt.Errorf("Failed to create keyspace: %w", err)
	}

	return nil
}
