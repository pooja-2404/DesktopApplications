
import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnectionApp {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Database Connection App");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(300, 200);

            JPanel panel = new JPanel();
            frame.add(panel);

            JButton connectButton = new JButton("Connect to Database");
            panel.add(connectButton);

            connectButton.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    // Database connection code goes here
                    try {
                        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/db1","root","Pooja@240494");
                        // Perform database operations here
                        // Don't forget to close the connection when done
                        connection.close();
                        JOptionPane.showMessageDialog(frame, "Connected to the database!");
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                        JOptionPane.showMessageDialog(frame, "Error connecting to the database: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
                    }
                }
            });

            frame.setVisible(true);
        });
    }
}

