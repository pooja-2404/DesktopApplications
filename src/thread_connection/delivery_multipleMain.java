package thread_connection;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Scanner;
import java.time.LocalDate;
import com.toedter.calendar.JDateChooser;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class delivery_multipleMain {
	private static HikariDataSource dataSource;
	private static HikariConfig config;
	private static LocalDate localDateend;
	public static LocalDate localDatestart;
	private static JDateChooser dateChooserend;
	private static JDateChooser dateChooser;
	private static JLabel endDateLabel;
	private static JLabel startDateLabel;
	private static int batchSize;
	private static Scanner sc;

	public static void main(String[] args) {
		SwingUtilities.invokeLater(() -> {
			sc = new Scanner(System.in);
			System.out.println("Enter Batch Size");
			batchSize = sc.nextInt();

			ExecutorService threadPool = Executors.newFixedThreadPool(20);

			JFrame frame = new JFrame("Database Connection Example");
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.setLayout(new FlowLayout());

			// Create a button
			JButton connectButton = new JButton("Connect to Database");
			frame.add(connectButton);

			JButton submitButton = new JButton("Submit");
			frame.add(submitButton);

			JButton closeButton = new JButton("Disconnect to Database");
			frame.add(closeButton);

			// Create a label to display the connection status
			JLabel statusLabel = new JLabel("Connection Status: Not Connected");
			frame.add(statusLabel);
			startDateLabel = new JLabel("Start Date:");
			frame.add(startDateLabel);
			JPanel panel1 = new JPanel();
			frame.add(panel1);
			dateChooser = new JDateChooser();
			panel1.add(dateChooser);

			endDateLabel = new JLabel("End Date:");
			frame.add(endDateLabel);
			JPanel panel2 = new JPanel();
			frame.add(panel2);
			dateChooserend = new JDateChooser();
			panel2.add(dateChooserend);

			frame.pack();
			frame.setVisible(true);

			JTextArea resultTextArea = new JTextArea(10, 40);

			frame.add(resultTextArea);

			connectButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					resultTextArea.setText(""); // Clear previous results
					try {

						// Database connection code (similar to your previous example)
						config = new HikariConfig();
						config.setJdbcUrl("jdbc:mysql://localhost:3306/db1");
						config.setUsername("");
						config.setPassword("");
						SwingUtilities.invokeLater(new Runnable() {
							@Override
							public void run() {
								statusLabel.setText("Connection Status: Connected");

							}
						});
					} catch (Exception ex) {
						ex.printStackTrace();
						// Handle connection error
						SwingUtilities.invokeLater(new Runnable() {
							@Override
							public void run() {
								statusLabel.setText("Connection Status: Error");
							}
						});
					}

					dataSource = new HikariDataSource(config);
					config.setMinimumIdle(5);
					config.setMaximumPoolSize(20);
				}
			});

			submitButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					threadPool.submit(new Runnable() {
						@Override
						public void run() {

							try (Connection dbConnection = dataSource.getConnection()) {

								Date selectedStartDate = dateChooser.getDate();
								localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault())
										.toLocalDate();
								System.out.println("Start date:" + localDatestart);

								Date selectedEndDate = dateChooserend.getDate();
								localDateend = selectedEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
								System.out.println("End date:" + localDateend);

								{
									while ((localDatestart.isBefore(localDateend)
											|| localDatestart.isEqual(localDateend))) {
										DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
										String formattedDate = localDatestart.format(formatter);
										try {
											GetDatafromTable selectobj = new GetDatafromTable(
													dataSource.getConnection(), batchSize, formattedDate);
											selectobj.start();
										} catch (SQLException e1) {
											// TODO Auto-generated catch blockF
											e1.printStackTrace();
										}
//
//										
										localDatestart = localDatestart.plusDays(1);
									}

								}

							}

							catch (Exception ex) {
								ex.printStackTrace();
								SwingUtilities.invokeLater(new Runnable() {
									@Override
									public void run() {
										statusLabel.setText("Connection Status: Error");
									}
								});

							}

						}
					});
				}
			});

			closeButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {

					try {
						dataSource.close();
						resultTextArea.setText("");
						threadPool.shutdown();
						SwingUtilities.invokeLater(new Runnable() {
							@Override
							public void run() {
								statusLabel.setText("Connection Status: Disconnected");
							}
						});

					} catch (Exception ex) {
						ex.printStackTrace();
						SwingUtilities.invokeLater(new Runnable() {
							@Override
							public void run() {
								statusLabel.setText("Disconnection Status: Error");
							}
						});
					}

				}
			});

		});
	}
}
