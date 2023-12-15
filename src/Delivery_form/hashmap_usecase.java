package Delivery_form;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class hashmap_usecase {
    private Map<String, HikariDataSource> connectionPoolMap;

    public hashmap_usecase() {
        connectionPoolMap = new HashMap<>();
    }

    // Method to add a database connection to the HashMap
    public void addConnection(String databaseName, String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/db1");
		config.setUsername("root");
		config.setPassword("Pooja@240494");

        HikariDataSource dataSource = new HikariDataSource(config);
        connectionPoolMap.put(databaseName, dataSource);
    }

    // Method to get a database connection from the HashMap
    public Connection getConnection(String db1) throws SQLException {
        HikariDataSource dataSource = connectionPoolMap.get(db1);
        if (dataSource == null) {
            throw new SQLException("Database connection not found: " + db1);
        }
        return dataSource.getConnection();
    }

    // Method to close all database connections in the HashMap
    public void closeAllConnections() {
        for (HikariDataSource dataSource : connectionPoolMap.values()) {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    public static void main(String[] args) {
        hashmap_usecase dbManager = new hashmap_usecase();

        // Add database connections to the HashMap
        dbManager.addConnection("Database1", "jdbc:mysql://localhost:3306/db1", "root", "Pooja@240494");
        dbManager.addConnection("Database2", "jdbc:mysql://localhost:3306/db2", "root", "Pooja@240494");

        try {
            // Get a connection and perform database operations
            Connection connection1 = dbManager.getConnection("Database1");
            // ... (use connection1 for database operations)

            Connection connection2 = dbManager.getConnection("Database2");
            // ... (use connection2 for database operations)

            // Close all connections when done
            dbManager.closeAllConnections();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

