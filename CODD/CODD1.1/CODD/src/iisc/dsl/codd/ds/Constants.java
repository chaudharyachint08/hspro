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
package iisc.dsl.codd.ds;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Calendar;
/**
 * This class specifies all the Global Constants (Static Members) which will be used across the source code of this project.
 * @author dsladmin
 */
public class Constants implements Serializable{
    /**
     * Variable to determine whether warning should be shown for Mapping of Histograms in Postgres and Oracle.
     * Default vale is 0 (YES OPTION). Map.
     * Before transfer, this is set to -1, so that the user is asked for input once.
     */
    public static int status = 0;
    /**
     * Boolean variable which specifies whether the High Level Debug information
     * (Operational mode level information) should be printed to the console or not.
     */
    public static final boolean DEBUG_FIRST_LEVEL = true;
    // Low Level Debug information - Prints
    /**
     * Boolean variable which specifies whether the Low Level Debug information
     * (CODD-Database interaction level information) should be printed to the console or not.
     */
    public static final boolean DEBUG_SECOND_LEVEL = false;
    /**
     * Boolean variable which specifies whether the Very Low Level Debug information
     * (CODD-Optimizer interaction level information) should be printed to the console or not.
     */
    public static final boolean DEBUG_THIRD_LEVEL = false;
    
    /** Informational messages that is always printed and cannot be turned off
     * 
     */
    public static int DEBUG_ZERO_LEVEL_MANDATATORY_Information = 0;
    
    /**
     * Integer Constant to be used as an argument to CPrintErrToConsole function,
     * so that the debug message is prefixed with appropriate space.
     */
    public static final int DEBUG_FIRST_LEVEL_Information = 1;
    /**
     * Integer Constant to be used as an argument to CPrintErrToConsole function,
     * so that the debug message is prefixed with appropriate space.
     * Second level debug information are prefixed with a tab space.
     */
    public static final int DEBUG_SECOND_LEVEL_Information = 2;
    /**
     * Integer Constant to be used as an argument to CPrintErrToConsole function,
     * so that the debug message is prefixed with appropriate space.
     * Third level debug information are prefixed with two tab space.
     */
    public static final int DEBUG_THIRD_LEVEL_Information = 3;
    /**
     * String Constant which specifies the path separator.
     */
    public static String  PathSeparator = System.getProperty("file.separator");
    /**
     * String Constant which specifies the current working directory.
     */
    public static String  WorkingDirectory = System.getProperty("user.dir");


    // Log file name - Timestamp is taken into account
    static final Calendar c = Calendar.getInstance();
    /**
     * String Constant which specifies the log file name. Timestamp information
     * is used in the file name.
     */
    public static String FileName = Constants.WorkingDirectory+Constants.PathSeparator+"Logs"+Constants.PathSeparator+"CODD_"
            + c.get(Calendar.DAY_OF_MONTH) + "." + (c.get(Calendar.MONTH) + 1) + "."
            + c.get(Calendar.YEAR) + "."
            + c.get(Calendar.HOUR_OF_DAY) + "." + c.get(Calendar.MINUTE)
            + ".log";

    /**
     * String Constant which specifies the Configuration file name, where DB
     * Connection settings are stored and retrieved for later use.
     */
    public static String ConfigFileName = Constants.WorkingDirectory+Constants.PathSeparator+"DatabaseConnectionConfig";


    /** Optimization Solver - Constants. **/

    // scalefactor*thresholdTimes are considered to be the corner points of the n-dimensional
    public static final int thresholdTimes = 5;

    // Obj fn tolerance
    public static final double tolerance =  1e-8;

    // number of iterations
    public static final int iterations = 10000;

    // Value for SQLArg2
    public static final double SQLArg2_ElementValue = -1;

    // Value for SQLArg3
    public static final double SQLArg3_ElementValue = 1;

    /**
     * Function to get the current working directory.
     * @return Returns the current working directory.
     */
    public static String getWorkingDirectory() {
     return System.getProperty("user.dir");
    }

    /**
     * Logs the error information into the log file.
     * @param str - Error String
     */
    public static final void CPrintErrToConsole(Exception e) {
        try {
            FileWriter fis = new FileWriter(Constants.FileName, true);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String str = sw.toString();
            fis.write(str + "\n");
            fis.flush();
            fis.close();
            e.printStackTrace();
        } catch (FileNotFoundException fnfe) {
            System.out.println("File not found: " + fnfe);
        } catch (IOException ioe) {
            System.out.println("IOException: " + ioe);
        }
    }

    /**
     * Logs the debug information into the log file. Second level
     * information are prefixed with a tab space.
     * @param str - debug information
     * @param debugLevel - debug level.
     */
    public static final void CPrintToConsole(String str, int debugLevel) {
      try {
          // If Logs directory is not present, create it.
          String logDir = FileName.substring(0, FileName.lastIndexOf(Constants.PathSeparator));
          File f = new File(logDir);
            if(!f.exists()) {
                f.mkdir();
            }
            FileWriter fis = new FileWriter(Constants.FileName, true);
            if (debugLevel == Constants.DEBUG_FIRST_LEVEL_Information && DEBUG_FIRST_LEVEL) {
                fis.write(str + "\n");
            } else if (debugLevel == Constants.DEBUG_SECOND_LEVEL_Information) {
                fis.write("\t"+str + "\n");
            }
            fis.flush();
            fis.close();
        } catch (FileNotFoundException fnfe) {
            System.out.println("File not found: " + FileName);
        } catch (IOException ioe) {
            System.out.println("IOException: " + ioe);
        }
        if ((debugLevel == Constants.DEBUG_ZERO_LEVEL_MANDATATORY_Information)) {
          System.out.println(str);
        } else if ((debugLevel == Constants.DEBUG_FIRST_LEVEL_Information) && DEBUG_FIRST_LEVEL ) {
            System.out.println(str);
        } else if ((debugLevel == Constants.DEBUG_SECOND_LEVEL_Information) && DEBUG_SECOND_LEVEL) {
            System.out.println("\t" + str);
        } else if ((debugLevel == Constants.DEBUG_THIRD_LEVEL_Information) && DEBUG_THIRD_LEVEL) {
            System.out.println("\t \t"+str);
        }
    }
}
