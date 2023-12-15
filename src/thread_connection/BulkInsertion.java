package thread_connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class BulkInsertion implements Runnable {
	Connection conn;
	String batchQuery;
	Statement stmt;

	public BulkInsertion(Connection conn, String batchQuery) {
		this.conn = conn;
		this.batchQuery = batchQuery;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {

			stmt = conn.createStatement();

			stmt.executeUpdate(batchQuery);

			System.out.println("Successfully inserted");
		} catch (Exception e) {
			Class<? extends Exception> exceptionType = e.getClass();
		    System.out.println("Exception Type1: " + exceptionType.getName());}
			
//		} finally {
//			if (stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}

	}

}
