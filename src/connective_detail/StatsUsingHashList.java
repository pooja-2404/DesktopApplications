package connective_detail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import com.toedter.calendar.JDateChooser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StatsUsingHashList {
	static List<Integer> key;
	static int deliveredCount, failedCount;
	static LocalDate processedDate;
	static Connection dbConnection;
	static ExecutorService threadPool;
	static Statement stmt, stmt2, stmt3;
	String Delivery_status, Delivery_time;
	int hourNum, minNum, _5minNum, _15minNum;
	int dcount, fcount;
	static LocalDate localDateend;
	static LocalDate localDatestart;
	static JDateChooser dateChooserend;
	static JDateChooser dateChooser;
	static JLabel endPanelLabel;
	static JLabel endDateLabel;
	static JLabel counterLabel;
	static JLabel startDateLabel;
	static String formattedDate;
	static ConcurrentHashMap<List<Integer>, CounterDF> hourMap = new ConcurrentHashMap<>();
	
	public StatsUsingHashList(HikariDataSource dataSource, JTextArea resultTextArea, HikariConfig config,
			JLabel statusLabel, JDateChooser dateChooser, JDateChooser dateChooserend, JLabel endDateLabel,
			JLabel startDateLabel) {
		threadPool = Executors.newFixedThreadPool(20);

		threadPool.submit(new Runnable() {
			@Override
			public void run() {

				try {

					dbConnection = dataSource.getConnection();

					Date selectedStartDate = dateChooser.getDate();
					localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					startDateLabel.setText("Selected Start date:" + localDatestart);

					Date selectedEndDate = dateChooserend.getDate();
					localDateend = selectedEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					endDateLabel.setText("Selected End date:" + localDateend);

					stmt = dbConnection.createStatement();
					stmt3 = dbConnection.createStatement();
					stmt2 = dbConnection.createStatement();
					processedDate = localDatestart;

					long startTime = System.currentTimeMillis();

					mainExecution();
					long endTime = System.currentTimeMillis();
					long elapsedTime = (endTime - startTime);
					resultTextArea.setText("Records Inserted Successfully \n \n" + "Total Time Spent:" + elapsedTime
							+ " milli seconds");

				} catch (Exception ex) {
					ex.printStackTrace();
					SwingUtilities.invokeLater(new Runnable() {

						@Override
						public void run() {
							statusLabel.setText("<html><font color='red'>Connection Status: Error</font></html>");
						}
					});

				}

			}
		});

	}

	public void mainExecution() {
		try {
			while ((localDatestart.isBefore(localDateend) || localDatestart.isEqual(localDateend))) {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
				formattedDate = localDatestart.format(formatter);

				String tableName = "statsDetail_" + formattedDate;
				System.out.println(tableName);
				String createQuery = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + "ProcessedDate Date ,"
						+ "Hour int," + "Min int," + "Min_5 int ," + "Min_15 int," + "Del_count int," + "Fail_count int)";
				stmt = dbConnection.createStatement();
				stmt.executeUpdate(createQuery);
				stmt.close();
				// create table command
				String sql_main = "Select  Delivery_time, Delivery_status from Delivery_details_" + formattedDate + "";
				stmt = dbConnection.createStatement();
				ResultSet rs = stmt.executeQuery(sql_main);
				while (rs.next()) {
					Delivery_time = rs.getString("Delivery_time");
					Delivery_status = rs.getString("Delivery_status");
					String[] parts = Delivery_time.split(":");

					if (parts.length >= 1) {
						// Parse the hour part as an integer

						hourNum = Integer.parseInt(parts[0]);
						minNum = Integer.parseInt(parts[1]);
						_5minNum = minNum / 5;
						_15minNum = minNum / 15;
						key = new ArrayList<>();
						key.add(hourNum);
						key.add(minNum);
						key.add(_5minNum);
						key.add(_15minNum);

					}
					//////////////////////////////////////////////////////
					if (Delivery_status.equals("Delivered")) {
						if (hourMap.containsKey(key)) {
							CounterDF current = hourMap.get(key);
							if (current != null) {

								int updatedValue = current.getdcount() + 1;
								CounterDF updated = new CounterDF(updatedValue, current.getfcount(), current.getDate());
								hourMap.put(key, updated);
							}

						} else {
							CounterDF updated = new CounterDF(1, 0, localDatestart);
							hourMap.put(key, updated);

						}
					} else {
						if (hourMap.containsKey(key)) {
							CounterDF current = hourMap.get(key);
							if (current != null) {
								int updatedValue = current.getfcount() + 1;
								CounterDF updated = new CounterDF(current.getdcount(), updatedValue, current.getDate());
								hourMap.put(key, updated);
							}
						} else {

							CounterDF updated = new CounterDF(0, 1, localDatestart);
							hourMap.put(key, updated);

						}

					}
				}
				for (Map.Entry<List<Integer>, CounterDF> entry : hourMap.entrySet()) {
					int Hour = 0, Min = 0, _5Min = 0, _15Min = 0;
					List<Integer> hour = entry.getKey();
					Hour = hour.get(0);
					Min = hour.get(1);
					_5Min = hour.get(2);
					_15Min = hour.get(3);					
					CounterDF counter = entry.getValue();
					deliveredCount = counter.getdcount();
					failedCount = counter.getfcount();
					processedDate = counter.getDate();
					String sql = "INSERT  INTO " + tableName + " VALUES('" + processedDate + "'," + Hour + "," + Min
							+ "," + _5Min + "," + _15Min + "," + deliveredCount + "," + failedCount + ")";
					stmt.executeUpdate(sql);
				}
				localDatestart = localDatestart.plusDays(1);
				hourMap.clear();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class CounterDF {
	private AtomicInteger dcount;
	private AtomicInteger fcount;
	private LocalDate localDate;

	public CounterDF(int dcount, int fcount, LocalDate date) {
		this.dcount = new AtomicInteger(dcount);
		this.fcount = new AtomicInteger(fcount);
		this.localDate = date;

	}

	public int getdcount() {
		return dcount.get();
	}

	public int getfcount() {
		return fcount.get();
	}

	public LocalDate getDate() {
		return localDate;
	}
}