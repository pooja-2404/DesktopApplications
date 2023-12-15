package connective_detail;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.time.LocalDate;
import com.toedter.calendar.JDateChooser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.ArrayList;

public class Deliveryoperator_Main {
	private static Statement stmt, stmt2, stmt3;
	private static ConcurrentHashMap<Integer, List<String>> operator_circle = new ConcurrentHashMap<>();
	private static List<String> values;
	private static int Series;
	private static HikariDataSource dataSource;
	private static HikariConfig config;
	private static LocalDate localDateend;
	private static LocalDate localDatestart;
	private static JDateChooser dateChooserend;
	private static JDateChooser dateChooser;
	private static JLabel endPanelLabel;
	private static JLabel endDateLabel;
	private static JLabel counterLabel;
	private static JLabel startDateLabel;
	private static JLabel startPanelLabel;
	private static int countfailed = 0, countdelivered = 0;
	private static int batchsize = 500, limit;
	private static String sql_delivered = " ", sql_failed = " ";
	private static Scanner sc;
	private static int queryProcessed = 0;
	private static String Delivery_status;
	private static int Sr;
	private static String Mobile;
	private static String Delivery_time;
	private static Connection dbConnection;
	private static int limitStart, limitEnd;
	private static int lastRecord;
	private static LocalDate lastDate;
	private static String Operator, Circle;

	public static void handleCatch(int lastRecord, LocalDate lastDate) throws SQLException {

		if (dbConnection.isClosed()) {
			System.out.println("wait for connection");
			int retry = 0, maxtry = 3;
			while (retry < maxtry) {

				try {
					dbConnection = dataSource.getConnection();
					if (dbConnection.isClosed()) {
						config.setConnectionTimeout(5000);
					}
					stmt3 = dbConnection.createStatement();
					stmt2 = dbConnection.createStatement();
					stmt = dbConnection.createStatement();
					limitEnd = lastRecord + limit;
					MainExecution(dbConnection, stmt2, stmt3, lastDate, lastRecord, limitEnd);
					break;
				} catch (SQLException e) {
					retry++;
					e.printStackTrace();
				}

			}

		}
	}

	public static void MainExecution(Connection dbConnection, Statement stmt2, Statement stmt3,
			LocalDate localDatestart, int limitStart, int limitEnd) {

		try {
			while ((localDatestart.isBefore(localDateend) || localDatestart.isEqual(localDateend))) {

				int rowCount = 0;
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
				String formattedDate = localDatestart.format(formatter);
				rowCount = countQuery(formattedDate, dbConnection);
				while (rowCount > limitStart) {

					String sql_main = "Select sr,Mobile,Delivery_status,Delivery_time from Delivery_details_"
							+ formattedDate + " where Sr between " + limitStart + " and " + limitEnd + "";

					ResultSet rs = stmt.executeQuery(sql_main);
					while (rs.next()) {
						Delivery_status = rs.getString("Delivery_status");
						Sr = rs.getInt("Sr");
						Mobile = rs.getString("Mobile");
						Delivery_time = rs.getString("Delivery_time");
						int mobile_series = Integer.parseInt(Mobile.substring(0, 4));

						if (Delivery_status.equals("Delivered")) {

							if (operator_circle.containsKey(mobile_series)) {
								List<String> values = operator_circle.get(mobile_series);
								Operator = values.get(0);
								Circle = values.get(1);
							} else {
								Operator = "Other";
								Circle = "Other";
							}

							queryProcessed += 1;
							counterLabel.setText("Records Processed: " + queryProcessed);

							Object[] result1 = queryDeliveredFailed(sql_delivered, Sr, Mobile, Delivery_time,
									Delivery_status, countdelivered, localDatestart, Operator, Circle);
							sql_delivered = (String) result1[0];
							countdelivered = (int) result1[1];

							if (!sql_delivered.equals(" ") && (countdelivered == batchsize)) {
								String sql_query = "insert  into delivered_operator values" + "" + sql_delivered
										+ "on duplicate key UPDATE Delivered_date=VALUES(Delivered_date), delivery_time=VALUES(delivery_time)";
								try {
									stmt3.executeUpdate(sql_query);
									countdelivered = 0;
									sql_delivered = " ";
									lastDate = localDatestart;
									lastRecord = Sr;
								} catch (SQLException e) {
									handleCatch(lastRecord, lastDate);
								}

							}

						} else {

							if (operator_circle.containsKey(mobile_series)) {
								List<String> values = operator_circle.get(mobile_series);
								Operator = values.get(0);
								Circle = values.get(1);
							} else {
								Operator = "Other";
								Circle = "Other";
							}

							queryProcessed += 1;
							counterLabel.setText("Records Processed: " + queryProcessed);
							Object[] result1 = queryDeliveredFailed(sql_failed, Sr, Mobile, Delivery_time,
									Delivery_status, countfailed, localDatestart, Operator, Circle);
							sql_failed = (String) result1[0];
							countfailed = (int) result1[1];

							if (!sql_failed.equals(" ") && (countfailed == batchsize)) {

								String sql_query = "insert  into failed_operator values" + "" + sql_failed
										+ "on duplicate key UPDATE Delivered_date=VALUES(Delivered_date), delivery_time=VALUES(delivery_time)";
								try {
									stmt2.executeUpdate(sql_query);
									countfailed = 0;
									sql_failed = " ";
									lastDate = localDatestart;
									lastRecord = Sr;

								} catch (SQLException ex) {
									handleCatch(lastRecord, lastDate);

									ex.printStackTrace();
								}
							}

						}

					}
					limitStart = limitEnd + 1;
					limitEnd = limitEnd + limit;
				}

				if (!sql_delivered.equals(" ")) {
					String sql_query = "insert into delivered_operator values" + "" + sql_delivered
							+ "on duplicate key UPDATE Delivered_date=VALUES(Delivered_date), delivery_time=VALUES(delivery_time)";
					try {
						stmt3.executeUpdate(sql_query);
						lastDate = localDatestart;
						lastRecord = Sr;

					} catch (SQLException ex) {
						handleCatch(lastRecord, lastDate);

						ex.printStackTrace();
					}
				}

				if (!sql_failed.equals(" ")) {
					String sql_query = "insert into failed_operator values" + "" + sql_failed
							+ "on duplicate key UPDATE Delivered_date=VALUES(Delivered_date), delivery_time=VALUES(delivery_time)";
					try {
						stmt2.executeUpdate(sql_query);
						lastDate = localDatestart;
						lastRecord = Sr;

					} catch (SQLException ex) {
						handleCatch(lastRecord, lastDate);

						ex.printStackTrace();
					}
				}
				localDatestart = localDatestart.plusDays(1);
				limitStart = 1;
				limitEnd = limit;
			}

		} catch (SQLException e) {

			try {
				handleCatch(lastRecord, lastDate);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			e.printStackTrace();
		}

	}

	public static int countQuery(String formattedDate, Connection dbConnection) {
		String query = "SELECT COUNT(*) FROM Delivery_details_" + formattedDate + "";

		int rowCount = 0;
		try (PreparedStatement preparedStatement = dbConnection.prepareStatement(query);
				ResultSet resultSet = preparedStatement.executeQuery()) {
			if (resultSet.next()) {
				rowCount = resultSet.getInt(1);

			}
		} catch (SQLException e) {
			try {
				handleCatch(lastRecord, lastDate);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			e.printStackTrace();
		}
		return rowCount;

	}

	public static Object[] queryDeliveredFailed(String sql, int Sr, String Mobile, String Delivery_time,
			String Delivery_status, int count, LocalDate localDatestart, String Operator, String Circle)
			throws SQLException {
		try {
			if (sql == " ") {
				sql = "(" + Sr + ",'" + Mobile + "','" + Delivery_status + "', '" + Delivery_time + "','"
						+ localDatestart + "','" + Operator + "','" + Circle + "')";
				count += 1;

			} else {
				sql = sql + "," + "(" + Sr + ",'" + Mobile + "','" + Delivery_status + "', '" + Delivery_time + "','"
						+ localDatestart + "','" + Operator + "','" + Circle + "')";
				count += 1;
			}
			lastRecord = Sr;
			lastDate = localDatestart;

		} catch (Exception ex) {
			handleCatch(lastRecord, lastDate);

			ex.printStackTrace();
		}
		return new Object[] { sql, count };
	}

	public static void main(String[] args) {
		SwingUtilities.invokeLater(() -> {

			sc = new Scanner(System.in);
			System.out.println("Enter Selection Batch Size");
			limit = sc.nextInt();

			ExecutorService threadPool = Executors.newFixedThreadPool(20);

			JFrame frame = new JFrame("Delivery Details Form");
			frame.setBounds(300, 100, 850, 550);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

			JButton connectButton = new JButton("Connect to Database");
			connectButton.setBounds(50, 220, 200, 40);
			frame.add(connectButton);

			JButton closeButton = new JButton("Disconnect to Database");
			closeButton.setBounds(50, 280, 200, 40);
			frame.add(closeButton);

			JButton closeConnection = new JButton("Close Connection");
			closeConnection.setBounds(50, 340, 200, 40);
			frame.add(closeConnection);

			JLabel statusLabel = new JLabel("Connection Status: Not Connected");
			statusLabel.setBounds(420, 20, 200, 40);
			frame.add(statusLabel);

			startPanelLabel = new JLabel("Start Date:");
			startPanelLabel.setBounds(50, 40, 200, 40);
			frame.add(startPanelLabel);

			startDateLabel = new JLabel("Selected Start Date:");
			startDateLabel.setBounds(50, 70, 200, 40);
			frame.add(startDateLabel);

			JPanel panel1 = new JPanel();
			panel1.setBounds(60, 45, 200, 40);
			frame.add(panel1);
			dateChooser = new JDateChooser();
			panel1.add(dateChooser);

			endPanelLabel = new JLabel("End Date:");
			endPanelLabel.setBounds(50, 120, 200, 40);
			frame.add(endPanelLabel);

			endDateLabel = new JLabel("Selected End Date:");
			endDateLabel.setBounds(50, 150, 200, 40);
			frame.add(endDateLabel);

			JPanel panel2 = new JPanel();
			panel2.setBounds(60, 125, 200, 40);
			frame.add(panel2);
			dateChooserend = new JDateChooser();
			panel2.add(dateChooserend);

			JButton submitButton = new JButton("Submit");
			submitButton.setBounds(50, 400, 200, 40);
			frame.add(submitButton);

			counterLabel = new JLabel("Records Processed: " + queryProcessed);
			counterLabel.setBounds(450, 450, 200, 40);
			frame.add(counterLabel);

			frame.setLayout(null);
			frame.setVisible(true);

			JTextArea resultTextArea = new JTextArea();
			resultTextArea.setBounds(350, 80, 360, 360);
			frame.add(resultTextArea);

			connectButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					ConnectButton conobj = new ConnectButton(config, resultTextArea, statusLabel);
					dataSource = conobj.getDataSource();
					config = conobj.getConfig();
					connectButton.setEnabled(false);
				}
			});

			submitButton.addActionListener(new ActionListener() {

				@Override
				public void actionPerformed(ActionEvent e) {
					threadPool.submit(new Runnable() {
						@Override
						public void run() {

							try {
								submitButton.setEnabled(false);
								closeButton.setEnabled(true);
								dbConnection = dataSource.getConnection();
								// creating a hash map for the table
								String operator_query = "Select Series, Operator, Circle from Operator_table";
								Statement stmt4 = dbConnection.createStatement();
								ResultSet rs2 = stmt4.executeQuery(operator_query);
								while (rs2.next()) {
									Series = rs2.getInt("Series");
									Operator = rs2.getString("Operator");
									Circle = rs2.getString("Circle");
									values = operator_circle.getOrDefault(Series, new ArrayList<>());
									values.add(Operator);
									values.add(Circle);
									operator_circle.put(Series, values);

								}

								Date selectedStartDate = dateChooser.getDate();
								localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault())
										.toLocalDate();

								startDateLabel.setText("Selected Start date:" + localDatestart);

								Date selectedEndDate = dateChooserend.getDate();
								localDateend = selectedEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
								endDateLabel.setText("Selected End date:" + localDateend);
								stmt = dbConnection.createStatement();
								stmt3 = dbConnection.createStatement();
								stmt2 = dbConnection.createStatement();

								long startTime = System.currentTimeMillis();

								{
									MainExecution(dbConnection, stmt2, stmt3, localDatestart, limitStart, limitEnd);
								}
								long endTime = System.currentTimeMillis();
								long elapsedTime = (endTime - startTime);
								resultTextArea.setText("Records Inserted Successfully \n \n" + "Total Time Spent:"
										+ elapsedTime + " milli seconds");

							} catch (Exception ex) {
//
								try {
									handleCatch(lastRecord, lastDate);

								} catch (SQLException e) {
									e.printStackTrace();
								}

								ex.printStackTrace();
								SwingUtilities.invokeLater(new Runnable() {

									@Override
									public void run() {
										statusLabel.setText(
												"<html><font color='red'>Connection Status: Error</font></html>");
									}
								});

							}

						}
					});

				}
			});

			closeConnection.addActionListener(new ActionListener() {

				@Override
				public void actionPerformed(ActionEvent e) {
					if (dbConnection != null) {
						try {
							dbConnection.close();
							closeConnection.setEnabled(false);

						} catch (Exception ex) {
							ex.printStackTrace();
						}

					}
				}
			});

			closeButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					new CloseButton(dataSource, resultTextArea, statusLabel);
					closeButton.setEnabled(false);
					connectButton.setEnabled(true);
					submitButton.setEnabled(true);
				}
			});

		});
	}
}