
package Jform_project;

import com.toedter.calendar.JDateChooser;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.sql.*;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Scanner;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.io.PrintWriter;
import java.io.StringWriter;

class Myframe extends JFrame implements ActionListener {
	private Container c;
	private Connection con;
	private Statement stmt, stmt2, stmt3;
	private JButton Connectdb;
	private JButton Close;
	private JButton Start;
	private JButton End;
	private JButton Submit;
	private JTextArea tout;
	private JDateChooser dateChooser;
	private JDateChooser dateChooserend;
	private LocalDate localDatestart;
	private LocalDate localDateend;
	private int countfailed = 0, countdelivered = 0;
	private boolean flag = true, failflag = true;
	private ArrayList<String> Delivered = new ArrayList<String>();
	private ArrayList<String> failed = new ArrayList<String>();
	private int batchsize;
	private String sql = " ", failsql = " ";
	private Scanner sc;

	public Myframe() {
		sc = new Scanner(System.in);
		System.out.println("Enter Batch Size");
		batchsize = sc.nextInt();

		setBounds(100, 90, 800, 400);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		c = getContentPane();
		c.setLayout(new FlowLayout());

		Connectdb = new JButton("Connect");
		Connectdb.setFont(new Font("Arial", Font.PLAIN, 15));
		Connectdb.setSize(100, 20);
		Connectdb.setLocation(100, 100);
		Connectdb.addActionListener(this);
		c.add(Connectdb);

		Start = new JButton("Start Date");
		Start.addActionListener(this);
		c.add(Start);
		JLabel label = new JLabel("Start Date:");
		c.add(label);
		JPanel panel = new JPanel();
		c.add(panel);
		dateChooser = new JDateChooser();
		panel.add(dateChooser);

		End = new JButton("End Date");
		End.addActionListener(this);
		c.add(End);
		JLabel labelend = new JLabel("End Date:");
		c.add(labelend);
		JPanel panel1 = new JPanel();
		c.add(panel1);
		dateChooserend = new JDateChooser();
		panel1.add(dateChooserend);

		Submit = new JButton("Submit");
		Submit.setFont(new Font("Arial", Font.PLAIN, 15));
		Submit.setSize(100, 20);
		Submit.setLocation(150, 150);
		Submit.addActionListener(this);
		c.add(Submit);

		Close = new JButton("Stop");
		Close.setFont(new Font("Arial", Font.PLAIN, 15));
		Close.setSize(100, 20);
		Close.setLocation(100, 200);
		Close.addActionListener(this);
		c.add(Close);

		tout = new JTextArea();
		tout.setFont(new Font("Arial", Font.PLAIN, 15));
		tout.setSize(300, 800);
		tout.setLocation(500, 100);
		tout.setLineWrap(true);
		tout.setEditable(false);
		c.add(tout);

		setVisible(true);
	}

	private void handleException(Exception ex) {
		// Convert the exception stack trace to a string
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		ex.printStackTrace(pw);
		String exceptionText = sw.toString();
		tout.append(exceptionText);
		tout.setCaretPosition(tout.getDocument().getLength());
	}

	public void actionPerformed(ActionEvent e) {

		if (e.getSource() == Connectdb) {
			try {
				Class.forName("com.mysql.cj.jdbc.Driver");
				con = DriverManager.getConnection("jdbc:mysql://localhost:3306/db1", "root", "Pooja@240494");
				tout.setText("success");
			} catch (ClassNotFoundException | SQLException e1) {
				e1.printStackTrace();
				handleException(e1);// Print exception details
			}
			// update into jlabel and datechooser event and default date
		} else if (e.getSource() == Start) {

			Date selectedStartDate = dateChooser.getDate();
			localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			tout.setText("Start date:" + localDatestart);

			// update into jlabel and datechooserend event and default date
		} else if (e.getSource() == End) {

			Date selectedEndDate = dateChooserend.getDate();
			localDateend = selectedEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			tout.setText("End date:" + localDateend);
// apply java threading for delivered and failure concurrently
		} else if ((e.getSource() == Submit) && (localDatestart != null && localDateend != null)) {

			long startTime = System.currentTimeMillis();
			try {
				stmt = con.createStatement();
				stmt2 = con.createStatement();
				stmt3 = con.createStatement();
				{
					while ((localDatestart.isBefore(localDateend) || localDatestart.isEqual(localDateend))) {

						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
						String formattedDate = localDatestart.format(formatter);

						ResultSet rs = stmt
								.executeQuery("Select Sr,Mobile,Delivery_status, Delivery_time from Delivery_details_"
										+ formattedDate + "");
						while (rs.next()) {
							String Delivery_status = rs.getString("Delivery_status");
							int Sr = rs.getInt("Sr");
							String Mobile = rs.getString("Mobile");
							String Delivery_time = rs.getString("Delivery_time");

							boolean check = true, failcheck = true, find = false, failfind = false;
							if (Delivery_status.equals("Delivered")) {
//								String createTableSQL = "CREATE TABLE IF NOT EXISTS delivery_masters (" + "Sr INT ,"
//										+ "Mobile VARCHAR(10)," + "Delivery_status varchar(15),"
//										+ "Delivery_time varchar(15)," + "Delivered_date Date)";
//								try {
//									stmt3.executeUpdate(createTableSQL);
//								} catch (Exception e1) {
//									e1.printStackTrace();
//									handleException(e1);
//								}
								ResultSet rs1 = stmt2.executeQuery(
										"Select Sr,Mobile,Delivery_status, Delivery_time from Delivery_mastersstatus");

								while (rs1.next()) {
									String Mobilecust = rs1.getString("Mobile");
									if (Mobile.equals(Mobilecust)) {
										find = true;
										break;
									}
								}

								if (!find) {
									if (!Delivered.contains(Mobile)) {
										if (flag) {
											sql = "(" + Sr + ",'" + Mobile + "','" + Delivery_status + "', '"
													+ Delivery_time + "','" + localDatestart + "')";
											countdelivered += 1;
											flag = false;
											Delivered.add(Mobile);
										} else if (check) {
											sql = sql + "," + "(" + Sr + ",'" + Mobile + "','" + Delivery_status
													+ "', '" + Delivery_time + "','" + localDatestart + "')";
											countdelivered += 1;
											Delivered.add(Mobile);
										}
									}
								}
								if (!sql.equals(" ")) {
									if (countdelivered == batchsize) {
										String sql_query = "insert into delivery_mastersstatus values" + "" + sql + "";
										System.out.println(sql_query);

										stmt3.executeUpdate(sql_query);
										countdelivered = 0;
										sql = " ";
										flag = true;
										Delivered.clear();
									}
								}

							} else {
//								String createTableSQL = "CREATE TABLE IF NOT EXISTS delivery_failed (" + "Sr INT ,"
//										+ "Mobile VARCHAR(10)," + "Delivery_status varchar(15),"
//										+ "Delivery_time varchar(15)," + "Delivered_date Date)";
//								try {
//									stmt3.executeUpdate(createTableSQL);
//								} catch (Exception e1) {
//									e1.printStackTrace();
//									handleException(e1);
//								}
								ResultSet rs2 = stmt2.executeQuery(
										"Select Sr,Mobile,Delivery_status, Delivery_time,Delivered_date from Delivery_failedstatus");
								while (rs2.next()) {
									String Mobilecust = rs2.getString("Mobile");
									if (Mobile.equals(Mobilecust)) {
										failfind = true;
										break;
									}
								}
								if (!failfind) {
									if (!failed.contains(Mobile)) {
										if (failflag) {
											failsql = "(" + Sr + ",'" + Mobile + "','" + Delivery_status + "', '"
													+ Delivery_time + "','" + localDatestart + "')";
											countfailed += 1;
											failflag = false;
											failed.add(Mobile);
										} else if (failcheck) {
											failsql = failsql + "," + "(" + Sr + ",'" + Mobile + "','" + Delivery_status
													+ "', '" + Delivery_time + "','" + localDatestart + "')";
											countfailed += 1;
											failed.add(Mobile);
										}
									}
								}
								if (!failsql.equals(" ")) {
									if (countfailed == batchsize) {
										String sql_query = "insert into delivery_failedstatus values" + "" + failsql + "";
										// System.out.println(sql_query);
										stmt3.executeUpdate(sql_query);
										countfailed = 0;
										failsql = " ";
										failflag = true;
										failed.clear();
									}
								}
							}
						}
						tout.setText("Working on table: delivery_details_" + formattedDate);
						localDatestart = localDatestart.plusDays(1);
					}
					if (!sql.equals(" ")) {
						String sql_query = "insert into delivery_mastersstatus values" + "" + sql + "";

						stmt3.executeUpdate(sql_query);
						Delivered.clear();
					}

					if (!failsql.equals(" ")) {
						String sql_query = "insert into delivery_failedstatus values" + "" + failsql + "";
						stmt3.executeUpdate(sql_query);
						failed.clear();
					}

				}
				System.out.println("\n" + "data inserted successfully");
			} catch (SQLException e1) {
				e1.printStackTrace();
				handleException(e1);
			}
			long endTime = System.currentTimeMillis();
			long elapsedTime = endTime - startTime;
			System.out.println("elapsedtime:" + elapsedTime + " milliseconds");
		} else if (e.getSource() == Close) {
			try {
				con.close();
				stmt.close();
				stmt2.close();
				stmt3.close();
				sc.close();
				tout.setText("connection closed");
			} catch (SQLException e1) {
				e1.printStackTrace();
				handleException(e1);
			}
		}
	}
}

public class Jform_con {

	public static void main(String[] args) throws Exception {
		 new Myframe();
	}

}