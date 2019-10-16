/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package iisc.dsl.codd.client.gui;

import java.awt.Color;
import javax.swing.JTextField;

/**
 *
 * @author Deepali Nemade
 */
public class UIUtils {
    
    public static void enableTextField(JTextField... tf){
        for(JTextField jtf : tf){
            jtf.setText("");
            jtf.setBackground(Color.WHITE);
            jtf.setEditable(true);
            jtf.setFocusable(true);
        }
        
    }
    
    public static void disableTextField(JTextField... tf){
        for(JTextField jtf : tf){
            jtf.setBackground(Color.GRAY);
            jtf.setEditable(false);
            jtf.setFocusable(false);
        }
        
    }
}
