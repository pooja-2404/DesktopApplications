package connective_detail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import com.toedter.calendar.JDateChooser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;

public class Submit {
	static String key;
	static int deliveredCount, failedCount;
	static LocalDate processedDate;
	static Connection dbConnection;
	static ExecutorService threadPool;
	static Statement stmt, stmt2, stmt3;
	String Delivery_status, Delivery_time;
	int hourNum, minNum,_5minNum,_15minNum;
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
	static HashMap<String, DFcount> hourMap = new HashMap<>();
	public Submit(HikariDataSource dataSource, JTextArea resultTextArea, HikariConfig config, JLabel statusLabel,
			JDateChooser dateChooser, JLabel startDateLabel) {
		threadPool = Executors.newFixedThreadPool(20);
		threadPool.submit(new Runnable() {
			@Override
			public void run() {

				try {

					dbConnection = dataSource.getConnection();
					Date selectedStartDate = dateChooser.getDate();
					localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

					startDateLabel.setText("Selected Start date:" + localDatestart);
					stmt = dbConnection.createStatement();
					stmt3 = dbConnection.createStatement();
					stmt2 = dbConnection.createStatement();
					processedDate=localDatestart;
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
						formattedDate = localDatestart.format(formatter);

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
			String sql_main = "Select  Delivery_time, Delivery_status from Delivery_details_" + formattedDate + "";
			stmt = dbConnection.createStatement();
			ResultSet rs = stmt.executeQuery(sql_main);
			while (rs.next()) {
				Delivery_time = rs.getString("Delivery_time");
				Delivery_status = rs.getString("Delivery_status");
				String[] parts = Delivery_time.split(":");

				if (parts.length >= 1) {
					hourNum = Integer.parseInt(parts[0]);
					minNum = Integer.parseInt(parts[1]);
					_5minNum=minNum/5;
					_15minNum=minNum/15;
					key=parts[0]+":"+parts[1]+":"+_5minNum+":"+_15minNum;
				}
				if (Delivery_status.equals("Delivered")) {
					if (hourMap.containsKey(key)) {
						DFcount current = hourMap.get(key);
						if (current != null) {

							int updatedValue = current.getdcount() + 1;
							DFcount updated = new DFcount(updatedValue, current.getfcount(),current.getDate());
							hourMap.put(key, updated);
						}

					} else {
						DFcount updated = new DFcount(1, 0,localDatestart);
						hourMap.put(key, updated);


					}
				} else {
					if (hourMap.containsKey(key) ) {
						DFcount current = hourMap.get(key);
						if (current != null) {
							int updatedValue = current.getfcount() + 1;
							DFcount updated = new DFcount(current.getdcount(), updatedValue,current.getDate());
							hourMap.put(key, updated);
						}
					} else {

						DFcount updated = new DFcount(0, 1, localDatestart);
						hourMap.put(key, updated);
					}

				}
			}
			for (Map.Entry<String, DFcount> entry : hourMap.entrySet()) {
				int Hour=0, Min=0, _5Min=0, _15Min=0;
				String hour = entry.getKey();
				String[] parts = hour.split(":");

				if (parts.length >= 1) {
					Hour = Integer.parseInt(parts[0]);
					Min= Integer.parseInt(parts[1]);
					_5Min = Integer.parseInt(parts[2]);
					_15Min = Integer.parseInt(parts[3]);
				}
				DFcount counter = entry.getValue();
				 deliveredCount = counter.getdcount();
				 failedCount = counter.getfcount();
				 processedDate=counter.getDate();
				 String sql="INSERT  INTO hourCount VALUES('"+processedDate+"',"+Hour+","+Min+","+_5Min+","+_15Min+","+deliveredCount+","+failedCount+")";
				 stmt.executeUpdate(sql);
			}

		} catch (SQLException e) {
			e.printStackTrace();

		}

	}
}

class DFcount {
	private  int dcount;
	private  int fcount;
	private LocalDate localDate;

	public DFcount(int dcount, int fcount, LocalDate date) {
		this.dcount = dcount;
		this.fcount = fcount;
		this.localDate=date;
		
	}

	public int getdcount() {
		return dcount;
	}

	public int getfcount() {
		return fcount;
	}
	public LocalDate getDate() {
		return localDate;
	}
}
