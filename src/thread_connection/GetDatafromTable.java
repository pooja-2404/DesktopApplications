package thread_connection;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GetDatafromTable extends Thread {
	String formattedDate;
	int limit = 100;
	int offset = 0;
	int batchsize;
	int serial = 0;
	private static Statement stmt3;
	boolean keepRunning = true, isrecordFound = false;
	private static String sql = " ", failsql = " ";
	private static PreparedStatement preparedStatement;
	private static ResultSet rs = null;
	ExecutorService threadPool = Executors.newFixedThreadPool(50);
	Connection conn;
	private static int deliveredCounter=0, failedCounter=0;

	public GetDatafromTable(Connection conn, int batchsize, String formattedDate) {
		this.conn = conn;
		this.batchsize=batchsize;
		this.formattedDate=formattedDate;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		long starttime=System.currentTimeMillis();
		while (keepRunning) {
			try {
				Thread.sleep(1000);
				try {

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					inits();
					if(!isrecordFound) {
						keepRunning=false;
						long endtime=System.currentTimeMillis()-starttime;
						System.out.println("Total Estimated Time:"+endtime);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	private void inits() {

		// TODO Auto-generated method stub
		try {
			
			String sql_main = "Select sr,Mobile,Delivery_status,Delivery_time from Delivery_details_" + formattedDate
					+ "  where Sr>" + serial + " Limit 10000 ";
			preparedStatement = conn.prepareStatement(sql_main);
			System.out.println(sql_main);
			rs = preparedStatement.executeQuery();
			isrecordFound = false;
			stmt3=conn.createStatement();
			boolean flag = true, check = true, failflag = true, failcheck = true;
			while (rs.next()) {
				// No more records, break out of the loop
				isrecordFound = true;
				String Delivery_status = rs.getString("Delivery_status");
				serial = rs.getInt("Sr");
				String Mobile = rs.getString("Mobile");
				String Delivery_time = rs.getString("Delivery_time");

				// Process the data as needed
				//System.out.println("Sr: " + serial + ", Mobile: " + Mobile);
				if (Delivery_status.equals("Delivered")) {
				
					try {
						if (flag) {
							sql = "(" + serial + ",'" + Mobile + "','" + Delivery_status + "', '" + Delivery_time
									+ "')";
							flag = false;
						} else if (check) {
							sql = sql + "," + "(" + serial + ",'" + Mobile + "','" + Delivery_status + "', '"
									+ Delivery_time + "')";

						}
						deliveredCounter+=1;
						if(deliveredCounter>500) {
							deliveredCounter=0;
							String sql_query = "insert ignore into delivery_masters values" + "" + sql + "";

//							System.out.println(sql_query);

							BulkInsertion obj = new BulkInsertion(conn, sql_query);

							threadPool.execute(obj);
							// stmt3.executeUpdate(sql_query);

							sql = " ";
							flag = true;
						}

					} // try
					catch (Exception ex) {
						ex.printStackTrace();
					}
				} else if (Delivery_status.equals("Failed")) {
					try {

						if (failflag) {
							failsql = "(" + serial + ",'" + Mobile + "','" + Delivery_status + "', '" + Delivery_time
									+ "')";

							failflag = false;
						} else if (failcheck) {
							failsql = failsql + "," + "(" + serial + ",'" + Mobile + "','" + Delivery_status + "', '"
									+ Delivery_time + "')";

						}
						failedCounter+=1;
						if(failedCounter>500) {
							failedCounter=0;
							String sql_query = "insert ignore into delivery_failed values" + "" + failsql + "";
							

							BulkInsertion obj = new BulkInsertion(conn, sql_query);

							threadPool.execute(obj);

							failsql = " ";
							failflag = true;
						}
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}

			}

			if (!sql.equals(" ")) {
				
					deliveredCounter=0;
					String sql_query = "insert  ignore into delivery_masters values" + "" + sql + "";

//					System.out.println(sql_query);

					BulkInsertion obj = new BulkInsertion(conn, sql_query);

					threadPool.execute(obj);
					// stmt3.executeUpdate(sql_query);

					sql = " ";
					flag = true;
				}
			
			if (!failsql.equals(" ")) {

				failedCounter=0;
				String sql_query = "insert ignore into delivery_failed values" + "" + failsql + "";

				BulkInsertion obj = new BulkInsertion(conn, sql_query);

				threadPool.execute(obj);

				failsql = " ";
				failflag = true;

			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
