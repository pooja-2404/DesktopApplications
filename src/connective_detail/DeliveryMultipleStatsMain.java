package connective_detail;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;
import com.toedter.calendar.JDateChooser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DeliveryMultipleStatsMain {

	public static JLabel startPanelLabel, endPanelLabel, startDateLabel, endDateLabel;
	public static JDateChooser dateChooser, dateChooserend;
	public static HikariDataSource dataSource;
	public static HikariConfig config;
	static Connection dbConnection;
	static Statement stmt, stmt2, stmt3;
	static JTextArea resultTextArea;

	public static void main(String[] args) {
		SwingUtilities.invokeLater(() -> {

			JFrame frame = new JFrame("Stats Form");
			frame.setBounds(300, 100, 850, 550);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

			JButton connectButton = new JButton("Connect to Database");
			connectButton.setBounds(50, 280, 200, 40);
			frame.add(connectButton);

			JButton closeButton = new JButton("Disconnect to Database");
			closeButton.setBounds(50, 340, 200, 40);
			frame.add(closeButton);

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

			frame.setLayout(null);
			frame.setVisible(true);

			resultTextArea = new JTextArea();
			resultTextArea.setBounds(350, 80, 360, 360);
			frame.add(resultTextArea);

			connectButton.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					ConnectButton conobj = new ConnectButton(config, resultTextArea, statusLabel);
					dataSource = conobj.getDataSource();
					config = conobj.getConfig();
					connectButton.setEnabled(false);
					closeButton.setEnabled(true);
					submitButton.setEnabled(true); 
				}
			});

			submitButton.addActionListener(new ActionListener() {

				@Override
				public void actionPerformed(ActionEvent e) {
//					new SubmitMultipleStats(dataSource, resultTextArea, config, statusLabel, dateChooser,
//							dateChooserend, endDateLabel, startDateLabel);
					new StatsUsingHashList(dataSource,resultTextArea,config,statusLabel,dateChooser,
					dateChooserend,endDateLabel, startDateLabel);
					submitButton.setEnabled(false);
					closeButton.setEnabled(true);

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
