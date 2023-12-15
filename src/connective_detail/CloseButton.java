package connective_detail;
import javax.swing.JLabel;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import com.zaxxer.hikari.HikariDataSource;

public class CloseButton {
	HikariDataSource dataSource;
	JLabel statusLabel;
	JTextArea resultTextArea;
	

	public CloseButton(HikariDataSource dataSource, JTextArea resultTextArea, JLabel statusLabel) {
		this.dataSource = dataSource;
		this.resultTextArea = resultTextArea;
		this.statusLabel = statusLabel;

		try {
			if (dataSource != null) {
				dataSource.close();
			}
			

			resultTextArea.setText("");
			
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
					statusLabel.setText("<html><font color='red'>Disconnection Status: Error</font></html>");
				}
			});
		}
	}

}
