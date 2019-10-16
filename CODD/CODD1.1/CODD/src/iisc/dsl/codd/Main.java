/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package iisc.dsl.codd;

import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.client.*;
import javax.swing.UIManager;


/**
 * @author db2admin
 */
public class Main {

    /*
     * Important: 
     * Please set the next variable "isJdbcDriver" to following values:
     * "true" - If working on databases other than HP NonStopSQLMX, eg. DB2, Oracle etc.
     * "true" - If working on HP NonStopSQLMX, and using the official JDBC driver.
     * "false" - If working on HP NonStopSQLMX, but not using the official JDBC driver.
     */
    public static boolean isJdbcDriver = true;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel("javax.swing.plaf.metal.MetalLookAndFeel");
        } catch (Exception e) {
            Constants.CPrintToConsole("Unable to load Windows look and feel", Constants.DEBUG_FIRST_LEVEL_Information);
            Constants.CPrintErrToConsole(e);
        }
        new SplashFrame().setVisible(true);
    }
}
