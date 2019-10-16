/*
#
# COPYRIGHT INFORMATION
#
# Copyright (C) 2013 Indian Institute of Science
# Bangalore 560012, INDIA
#
# This program is part of the CODD Metadata Processor
# software distribution invented at the Database Systems Lab,
# Indian Institute of Science. The use of the software is governed
# by the licensing agreement set up between the copyright owner,
# Indian Institute of Science, and the licensee.
#
# This program is distributed WITHOUT ANY WARRANTY;
# without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# The public URL of the CODD project is
# http://dsl.serc.iisc.ernet.in/projects/CODD/index.html
#
# For any issues, contact
# Prof. Jayant R. Haritsa
# SERC
# Indian Institute of Science
# Bangalore 560012, India.
# 

# Email: haritsa@dsl.serc.iisc.ernet.in
# 
#
*/
package iisc.dsl.codd.db;

import iisc.dsl.codd.ds.Constants;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import javax.swing.JOptionPane;

/**
 * This class specifies all the Global Constants (Static Members) related to the databases, which will be used across the source code of this project.
 * @author dsladmin
 */
public class DBConstants {
    /**
     * String Constant representing the DB2 database.
     */
    public static final String DB2 = "DB2";
    /**
     * String Constant representing the ORACLE database.
     */
    public static final String ORACLE = "ORACLE";
    /**
     * String Constant representing the POSTGRES database.
     */
    public static final String POSTGRES = "POSTGRES";
    /**
     * String Constant representing the SYBASE database.
     */
    public static final String SYBASE = "SYBASE";
    /**
     * String Constant representing the MSSQL database.
     */
    public static final String MSSQL = "SQL SERVER";
    /*
     *  String Constant representing the NonStopSQL database
     */
    public static final String NONSTOPSQL = "NON STOP SQL";             // Added by - Deepali Nemade

    /**
     * Returns true, if the specified relation belonging to the specified vendor is a system created relation.
     * If the user wants some of the relations to be ignore in GetRelationFrame, it has to be added here.
     * @param vendor Database Vendor name
     * @param relation Relation to check for system created
     * @return true, if the relation can be ignored
     *         false, otherwise
     */
    public static boolean isSystemCreatedRelationToIgnore(String vendor, String relation) {
        
        if ( (relation.contains("Picasso")) || (relation.contains("picasso")) || (relation.contains("PICASSO")) )
            return true;
        
        if (vendor.equals(DBConstants.DB2)) {
            if( (relation.contains("ADVISE")) || (relation.contains("EXPLAIN")) || (relation.contains("OBJECT")) ) {
                return true;
            }
        } else if (vendor.equals(DBConstants.ORACLE)) {
            if ( (relation.contains("$")) || (relation.contains("_")) || (relation.equals("HELP")) ) {
                return true;
            }
        } else if (vendor.equals(DBConstants.MSSQL)) {
        } else if (vendor.equals(DBConstants.SYBASE)) {
        } else if (vendor.equals(DBConstants.POSTGRES)) {
        } else if (vendor.equals(DBConstants.NONSTOPSQL)) {             // Added by - Deepali Nemade
            /* See what to write here */
        }
        return false;
    }

    /**
     * Reads the stored connection parameters from the Configuration file and returns them in a hashmap.
     * @return Stored connection parameters.
     */
    public static HashMap<String, String> readStoredConnectionParameters() {
        HashMap<String, String> connParams = new HashMap();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(Constants.ConfigFileName)));
            String line = reader.readLine();
            while (line != null) {
                Constants.CPrintToConsole(line, Constants.DEBUG_SECOND_LEVEL_Information);
                String[] temp = line.split("::");
                connParams.put(temp[0], temp[1]);
                line = reader.readLine();
            }
            reader.close();
        } catch (FileNotFoundException e) {
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            JOptionPane.showMessageDialog(null, "Exception Caught: Could not get stored Database Connection Configuration.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
            return null;
        }
        return connParams;
    }
}