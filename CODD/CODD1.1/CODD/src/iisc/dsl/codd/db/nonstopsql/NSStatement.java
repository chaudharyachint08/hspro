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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package iisc.dsl.codd.db.nonstopsql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 *
 * @author Administrator
 */
public class NSStatement implements Statement {

    /* Comment Added By : Deepali Nemade
     * CUSTOM IMPLEMENTATION FOR JDBC DRIVER. DO NOT MODIFY ON YOUR OWN !
     * 
     */
    private Process process;
    private BufferedWriter writer;
    private BufferedReader reader;
    public static int count = 0;
    private static BufferedWriter writer2;

    public NSStatement(String command) throws SQLException {
        try {
            count++;
            /*if(count > 1){
            throw new SQLException("Cannot generate more than one process");
            } 
             */
            process = Runtime.getRuntime().exec(command);

            System.out.println("Connected to SQL/MX.");

            writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            writer.append("CONTROL QUERY DEFAULT OPTIMIZATION_LEVEL '5';");
            writer.flush();

            writer.write("CONTROL QUERY DEFAULT CACHE_HISTOGRAMS 'OFF';");
            writer.flush();

            writer.write("CONTROL QUERY DEFAULT INFER_CHARSET 'ON';");
            writer.flush();

            //writer1 = new BufferedWriter(new FileWriter("c:\\commands.txt"));
            writer2 = new BufferedWriter(new FileWriter("c:\\coddoutput.txt"));

            writer.write("CONTROL QUERY DEFAULT QUERY_CACHE '0';");
            writer.flush();

        } catch (Exception e) {
            throw new SQLException("Cannot Link To Process.");
        }
    }

    public synchronized List<String> hitCommand(String command) throws SQLException {
        List<String> retValue = new Vector<String>();


        /*
         * 
         * Following Code is being added to start SQL/MX each time it wants to hit a command  .
         * Also replace breader and bwriter by writer and reader for recovering from the changes.
         */
        try {
            //count++;
            /*if(count > 1){
            throw new SQLException("Cannot generate more than one process");
            } 
             */
            /*   process1 = Runtime.getRuntime().exec("sqlci");
            bwriter = new BufferedWriter(new OutputStreamWriter(process1.getOutputStream()));
            breader = new BufferedReader(new InputStreamReader(process1.getInputStream()));
             */
            //writer1 = new BufferedWriter(new FileWriter("c:\\commands.txt"));
            //writer2 = new BufferedWriter(new FileWriter("c:\\fulloutput.txt"));





            if (!command.trim().endsWith(";")) {
                command = command + ";";
                command = command.replace('\r', ' ').replace('\n', ' ');
            }
            //System.out.println("COMMAND : " + command);


            //        try{

            //writer.write("CONTROL QUERY DEFUALT SIMILARITY_CHECK 'OFF';" + "\r\n\r\n");
            //writer.flush();

            writer.write(command + "\r\n\r\n");
            writer.flush();

            //writer1.write(command + "\r\n\r\n");
            //writer1.flush();

            writer2.write(command + "\r\n\r\n");
            writer2.flush();


            //fileWriter = new BufferedWriter(new FileWriter("c:\\output.txt"));
            //fileWriter.append(command + "\r\n\r\n");

            String str = reader.readLine();
            while (str != null && (!str.equals(">>"))) {
                retValue.add(str);

                writer2.write(str + "\r\n");
                writer2.flush();

                str = reader.readLine();

            }

            // Closes the current SQLCI session
            //writer.write("EXIT \r\n");

        } catch (Exception e) {
            throw new SQLException("Exception in executing command at SQLCI");
        }


        return retValue;
    }

    public String hitsCommand(String command) throws SQLException {
        System.out.println("Hit Command.");
        String retValue = "";
        if (!command.trim().endsWith(";")) {
            command = command + ";";
            command = command.replace('\r', ' ').replace('\n', ' ');
        }
        //System.out.println("COMMAND : " + command);
        try {
            writer.write(command + "\r\n\r\n");
            writer.flush();

            String str = reader.readLine();
            while (str != null && (!str.equals(">>"))) {
                retValue.concat(str);
                str = reader.readLine();
            }
        } catch (Exception e) {
            throw new SQLException("Exception in executing command at SQLCI");
        }
        System.out.println("");
        return retValue;
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        return extractTableFromSQLOutput(hitCommand(query));
    }

    /* Return the indices of spaces in the string */
    int[] findBreaksInHeader(String str) {
        StringTokenizer stok = new StringTokenizer(str);
        int[] retValue = new int[stok.countTokens()];
        int position = 0;
        boolean isSpace = true;
        for (int i = 0; i < str.length(); i++) {
            if (isSpace) {
                if (str.charAt(i) != ' ') {
                    retValue[position] = i;
                    position++;
                    isSpace = false;
                }
            } else {
                if (str.charAt(i) == ' ') {
                    isSpace = true;
                }
            }
        }
        return retValue;
    }

    public NSResultset extractTableFromSQLOutput(List<String> value) {
        int index_of_header_line = 0;
        for (String str : value) {
            if ((str.startsWith("-")) && (str.replace("-", " ").trim().length() == 0)) {
                break;
            }
            index_of_header_line++;
        }

        if (index_of_header_line >= value.size()) {
            return new NSResultset(0, 0);
        }
        int[] tokenPositions = findBreaksInHeader(value.get(index_of_header_line));

        List<List<String>> table = new LinkedList<List<String>>();

        for (int i = index_of_header_line + 1; i < value.size(); i++) {
            if (value.get(i).startsWith("---")) {
                break;
            }
            if (value.get(i).trim().length() == 0) {
                continue;
            }
            List<String> row = new LinkedList<String>();

            int index = 1;
            for (int j=0; j<tokenPositions.length; j++) {
                if (index == tokenPositions.length) {
                    row.add(value.get(i).substring(tokenPositions[index - 1]));
                    break;
                }
                row.add(value.get(i).substring(tokenPositions[index - 1], tokenPositions[index]).trim());
                index++;
            }
            table.add(row);
        }
        NSResultset mxtable = new NSResultset(table.size(), table.get(0).size());
        mxtable.importData(table);
        return mxtable;
        /*
        for(int i = 0; i< table.size() ; i++){
        for(int j = 0 ; j < table.get(i).size() ; j++ ){
        System.out.print(" -|- " + table.get(i).get(j));
        }
        System.out.println("");
        }*/

    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        List<String> value = hitCommand(sql);
        for (String str : value) {

            if ((str.startsWith("*** ERROR "))) {
                throw new SQLException(str);
            }
            if ((str.startsWith("--- ")) && (str.replace("-", " ").trim().length() == 0)) {
                str = str.replace('-', ' ').trim();

                str = new StringTokenizer(str).nextToken();
                return Integer.parseInt(str);
            }
        }
        return -1;
    }

    @Override
    public void close() throws SQLException {
        count--;

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancel() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        List<String> value = hitCommand(sql);
        for (String str : value) {
            if ((str.startsWith("*** ERROR "))) {
                throw new SQLException(str);
            }
        }
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
