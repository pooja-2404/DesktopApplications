//package Jform_project;
import com.toedter.calendar.JDateChooser;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Scanner;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;
class Myframe
	extends JFrame
	implements ActionListener {
	private Container c;
	private Connection con;
	private JButton Connectdb;
	private JButton Close;
	private JButton Start;
	private JButton End;
	private JButton Fetch;
	private JTextArea tout;
	private JDateChooser dateChooser;
	private JDateChooser dateChooserend;
	private String formattedStartDate;
	private String formattedEndDate;
    private LocalDate localDatestart=null;
    private LocalDate localDateend=null;
    private int countfailed=0, countdelivered=0;
    private boolean flag=true, failflag=true;
    
    private int batchsize;
    private String sql="",failsql=" ";
	public Myframe()
	{
		Scanner sc=new Scanner(System.in);
	    System.out.println("Enter Batch Size");
	   batchsize=sc.nextInt();
		
		setBounds(100, 90, 800, 200);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		c = getContentPane();
		c.setLayout(new FlowLayout());
		
		Connectdb = new JButton("Connect");
		Connectdb.setFont(new Font("Arial", Font.PLAIN, 15));
		Connectdb.setSize(100, 20);
		Connectdb.setLocation(100, 100);
		Connectdb.addActionListener(this);
		c.add(Connectdb);
		
		Fetch = new JButton("FetchData");
		Fetch.setFont(new Font("Arial", Font.PLAIN, 15));
		Fetch.setSize(100, 20);
		Fetch.setLocation(150, 150);
		Fetch.addActionListener(this);
		c.add(Fetch);

		Close = new JButton("Stop");
		Close.setFont(new Font("Arial", Font.PLAIN, 15));
		Close.setSize(100, 20);
		Close.setLocation(100, 200);
		Close.addActionListener(this);
		c.add(Close);
		
		Start = new JButton("Start Date");
		Start.setFont(new Font("Arial", Font.PLAIN, 15));
		Start.setSize(100, 20);
		Start.setLocation(100, 250);
		Start.addActionListener(this);
		c.add(Start);
		JPanel panel = new JPanel();
        c.add(panel);
        dateChooser = new JDateChooser();
        panel.add(dateChooser);
		
		
		End = new JButton("End Date");
		End.setFont(new Font("Arial", Font.PLAIN, 15));
		End.setSize(100, 20);
		End.setLocation(200, 250);
		End.addActionListener(this);
		c.add(End);
		JPanel panel1 = new JPanel();
        c.add(panel1);
         dateChooserend = new JDateChooser();
        panel1.add(dateChooserend);

		tout = new JTextArea();
		tout.setFont(new Font("Arial", Font.PLAIN, 15));
		tout.setSize(300, 400);
		tout.setLocation(500, 100);
		tout.setLineWrap(true);
		tout.setEditable(false);
		c.add(tout);
		setVisible(true);
	}
	
	public void actionPerformed(ActionEvent e)
	{
		
		
			if (e.getSource() == Connectdb) {
		        try {
		            Class.forName("com.mysql.cj.jdbc.Driver");
		            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/db1", "root", "Pooja@240494");
		            System.out.println("success");
		        }
		        catch(ClassNotFoundException | SQLException e1) {
		            e1.printStackTrace(); // Print exception details
		        }
		    }
			else if(e.getSource()==Start) {
				
				Date selectedStartDate = dateChooser.getDate();
			    localDatestart = selectedStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			        System.out.println("Sart date:"+localDatestart);
				 
			}
			else if(e.getSource()==End) {
				
				Date selectedEndDate = dateChooserend.getDate();
				  localDateend = selectedEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		        System.out.println("End date:"+localDateend);

			}
			else if((e.getSource()==Fetch) && (localDatestart!=null && localDateend!=null) ) {
				long startTime = System.currentTimeMillis();
				try {
					Statement stmt=con.createStatement();
					Statement stmt2=con.createStatement();
					Statement stmt4=con.createStatement();
					Statement stmt3=con.createStatement();
					{

						while((localDatestart.isBefore(localDateend) ||localDatestart.isEqual(localDateend))) {
						
						 DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
					        String formattedDate = localDatestart.format(formatter);
					ResultSet rs= stmt.executeQuery("Select Sr,Mobile,Delivery_status, Delivery_time from Delivery_details_"+formattedDate+"");
					while (rs.next()) {
				        String Delivery_status = rs.getString("Delivery_status");
				        int Sr = rs.getInt("Sr");
				       String Mobile=rs.getString("Mobile");
				        Time Delivery_time = rs.getTime("Delivery_time");
				        
				      boolean check=true, failcheck=true;
				        if(Delivery_status.equals("Delivered"))	
				        {	  
				        	String createTableSQL = "CREATE TABLE IF NOT EXISTS delivery_masters ("
				        		    + "Sr INT ,"
				        		    + "Mobile VARCHAR(10),"
				        		    + "Delivery_status varchar(15),"
				        		    +"Delivery_time varchar(15),"+"Delivered_date varchar(15))";
				        	try {
				        	    stmt3.executeUpdate(createTableSQL);
				        	} catch (Exception e1) {
				        	    e1.printStackTrace();
				        	}
				        	ResultSet rs1= stmt2.executeQuery("Select Sr,Mobile,Delivery_status, Delivery_time,Delivered_date from delivery_masters");

				        	while(rs1.next()) {
				        		String Mobilecust=rs1.getString("Mobile");
//				        		int serial=rs1.getInt("sr");
				        		if(Mobilecust.equals(Mobile)) 
				        		{
				        			check=false;
				        			break;
				        		}
				        	}
				        	if(flag) {
				        			 sql= "("+Sr+","+Mobile+",'"+Delivery_status+"', '"+Delivery_time+"','"+localDatestart+"')";
				        			countdelivered+=1; flag=false;
				        			 } 
				        	else if(check) {
				        		sql= sql+","+"("+Sr+","+Mobile+",'"+Delivery_status+"', '"+Delivery_time+"','"+localDatestart+"')";
			        			countdelivered+=1; 
				        	}
				        	if(!sql.equals(" "))  {
				        	if(countdelivered==batchsize) {String sql_query="insert into delivery_masters values"+""+sql+"";
//						System.out.println(sql_query);
							stmt3.executeUpdate(sql_query);
							countdelivered=0;sql=" "; flag=true;}}
				        }
				        
				        
//				      if(Delivery_status.equals("Failed")) {
				        else {
//				        	// if status is failed
				        	String createTableSQL = "CREATE TABLE IF NOT EXISTS delivery_failed ("
				        		    + "Sr INT ,"
				        		    + "Mobile VARCHAR(10),"
				        		    + "Delivery_status varchar(15),"
				        		    +"Delivery_time varchar(15),"+"Delivered_date varchar(15))";
				        	try {
				        	    stmt3.executeUpdate(createTableSQL);
				        	} catch (Exception e1) {
				        	    e1.printStackTrace();
				        	}
////				        	
				        	ResultSet rs2= stmt2.executeQuery("Select Sr,Mobile,Delivery_status, Delivery_time, Delivered_date from delivery_failed");
				        	while(rs2.next()) {
				        		String Mobilefail=rs2.getString("Mobile");
				        		if(Mobilefail.equals(Mobile)) {failcheck=false;break; }
				        
				        	}// master while close
				        	if(failflag) {
		        			 failsql= "("+Sr+","+Mobile+",'"+Delivery_status+"', '"+Delivery_time+"','"+localDatestart+"')";
//		        			
		        			countfailed+=1; failflag=false;
		        			 } 
		        	else if(failcheck) {
		        		failsql= failsql+","+"("+Sr+","+Mobile+",'"+Delivery_status+"', '"+Delivery_time+"','"+localDatestart+"')";
//		        	
		        		countfailed+=1; 
		        	}
		        	if(!failsql.equals(" ")) {
		        	if(countfailed==batchsize) {String sql_query="insert into delivery_failed values"+""+failsql+"";

					stmt3.executeUpdate(sql_query);
					countfailed=0;failsql=""; failflag=true;}
		        	}
				        	}   
				        }
				
					
					tout.setText("Working on table: delivery_details_"+formattedDate);
					localDatestart = localDatestart.plusDays(1);
				      }// close date while
						if(!sql.equals(" ")) 
			        	{String sql_query="insert into delivery_masters values"+""+sql+"";
//			        	System.out.println(sql_query);
			        	stmt3.executeUpdate(sql_query);}
						
						
						if(!failsql.equals(" "))
			        	{String sql_query="insert into delivery_failed values"+""+failsql+"";
			        	stmt3.executeUpdate(sql_query); }
					}//close statement 
					
					System.out.println("\n"+"data inserted successfully");
				}
				catch(SQLException e1) {
					e1.printStackTrace();
				}
				long endTime = System.currentTimeMillis();
				long elapsedTime = endTime - startTime;
				System.out.println("elapsedtime:" + elapsedTime + " milliseconds");
			}
		

		else if (e.getSource() == Close) {
			try {
				con.close();
				System.out.println("connection closed");
			}
			catch(SQLException e1) {
				e1.printStackTrace();
			}
		}
	}
}

public class CalendarDemo {

	public static void main(String[] args) throws Exception
	{
		Myframe f = new Myframe();
	}

}