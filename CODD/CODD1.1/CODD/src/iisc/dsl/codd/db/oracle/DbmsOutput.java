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
package iisc.dsl.codd.db.oracle;

/**
 * Implements the Oracle Procedure Output Reader.
 * @author dsladmin
 */
import java.sql.*;

class DbmsOutput
{
    // Copied from http://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:45027262935845
    /*
     * our instance variables. It is always best to
     * use callable or prepared statements and prepare (parse)
     * them once per program execution, rather then one per
     * execution in the program.  The cost of reparsing is
     * very high.  Also -- make sure to use BIND VARIABLES!
     *
     * we use three statments in this class. One to enable
     * dbms_output - equivalent to SET SERVEROUTPUT on in SQL*PLUS.
     * another to disable it -- like SET SERVEROUTPUT OFF.
     * the last is to "dump" or display the results from dbms_output
     * using system.out
     *
     */

    private CallableStatement enable_stmt;
    private CallableStatement disable_stmt;
    private CallableStatement show_stmt;


    /*
     * our constructor simply prepares the three
     * statements we plan on executing.
     *
     * the statement we prepare for SHOW is a block of
     * code to return a String of dbms_output output.  Normally,
     * you might bind to a PLSQL table type but the jdbc drivers
     * don't support PLSQL table types -- hence we get the output
     * and concatenate it into a string.  We will retrieve at least
     * one line of output -- so we may exceed your MAXBYTES parameter
     * below. If you set MAXBYTES to 10 and the first line is 100
     * bytes long, you will get the 100 bytes.  MAXBYTES will stop us
     * from getting yet another line but it will not chunk up a line.
     *
     */
    public DbmsOutput(Connection conn) throws SQLException {
        enable_stmt = conn.prepareCall("begin dbms_output.enable(:1); end;");
        disable_stmt = conn.prepareCall("begin dbms_output.disable; end;");

        show_stmt = conn.prepareCall(
                "declare "
                + "    l_line varchar2(255); "
                + "    l_done number; "
                + "    l_buffer long; "
                + "begin "
                + "  loop "
                + "    exit when length(l_buffer)+255 > :maxbytes OR l_done = 1; "
                + "    dbms_output.get_line( l_line, l_done ); "
                + "    l_buffer := l_buffer || l_line || chr(10); "
                + "  end loop; "
                + " :done := l_done; "
                + " :buffer := l_buffer; "
                + "end;");
    }

    /*
     * enable simply sets your size and executes
     * the dbms_output.enable call
     *
     */
    public void enable(int size) throws SQLException {
        enable_stmt.setInt(1, size);
        enable_stmt.executeUpdate();
    }

    /*
     * disable only has to execute the dbms_output.disable call
     */
    public void disable() throws SQLException {
        disable_stmt.executeUpdate();
    }

    /*
     * show does most of the work.  It loops over
     * all of the dbms_output data, fetching it in this
     * case 32,000 bytes at a time (give or take 255 bytes).
     * It will print this output on stdout by default (just
     * reset what System.out is to change or redirect this
     * output).
     */
    public String getResults() throws SQLException {
        int done = 0;

        show_stmt.registerOutParameter(2, java.sql.Types.INTEGER);
        show_stmt.registerOutParameter(3, java.sql.Types.VARCHAR);
        StringBuffer result = new StringBuffer();
        for (;;) {
            show_stmt.setInt(1, 32000);
            show_stmt.executeUpdate();
            result.append(show_stmt.getString(3).trim());
            if ((done = show_stmt.getInt(2)) == 1) {
                break;
            }
        }
        return result.toString();
    }

    /*
     * close closes the callable statements associated with
     * the DbmsOutput class. Call this if you allocate a DbmsOutput
     * statement on the stack and it is going to go out of scope --
     * just as you would with any callable statement, result set
     * and so on.
     */
    public void close() throws SQLException {
        enable_stmt.close();
        disable_stmt.close();
        show_stmt.close();
    }
}