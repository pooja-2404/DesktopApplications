import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.sql.*;;

public class Dropdownlist_Main extends JFrame {
    private Container container;

    public Dropdownlist_Main() {
        container = getContentPane();
        container.setLayout(new FlowLayout());

        String[] items = {"Option 1", "Option 2", "Option 3", "Option 4"};
        JComboBox<String> comboBox = new JComboBox<>(items);
        container.add(comboBox);

        comboBox.addActionListener(e -> {
            String selectedValue = (String) comboBox.getSelectedItem();
            JOptionPane.showMessageDialog(this, "Selected: " + selectedValue);
        });
        setTitle("Dropdown List Example");
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
           Dropdownlist_Main form = new Dropdownlist_Main();
            form.setSize(400, 200);
           form.setVisible(true);
            form.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        });
    }
}
