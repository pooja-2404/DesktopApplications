package connective_detail;

import javax.swing.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectButton {
	HikariDataSource dataSource;
	HikariConfig config;
	JLabel statusLabel;
	JTextArea resultTextArea;

	public ConnectButton(HikariConfig config, JTextArea resultTextArea, JLabel statusLabel) {
		this.resultTextArea = resultTextArea;
		this.statusLabel = statusLabel;
		resultTextArea.setText(""); // Clear previous results
		connectToDatabase();
	}

	public void connectToDatabase() {
		config = new HikariConfig();
		config.setJdbcUrl("jdbc:mysql://localhost:3306/db1");
		config.setUsername("root");
		config.setPassword("Pooja@240494");

		try {
			// Attempt to establish the database connection
			dataSource = new HikariDataSource(config);
			if (dataSource == null) {
				
				connectToDatabase();
			}
			config.setMinimumIdle(5);
			config.setMaximumPoolSize(20);

			SwingUtilities.invokeLater(() -> {
				statusLabel.setText("<html><font color='green'>Connection Status: Connected</font></html>");
			});
		} catch (Exception ex) {
			ex.printStackTrace();
			// Handle connection error (e.g., log the error)
			SwingUtilities.invokeLater(() -> {
				statusLabel.setText("<html><font color='red'>Connection Status: Error</font></html>");
			});
		}
	}

	public HikariDataSource getDataSource() {
		return dataSource;
	}

	public HikariConfig getConfig() {
		return config;
	}
}
