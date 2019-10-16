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

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DatabaseAbstract;
import iisc.dsl.codd.db.mssql.*;
import iisc.dsl.codd.db.db2.*;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.ds.DataType;
import iisc.dsl.codd.plan.Node;
import iisc.dsl.codd.plan.Plan;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.swing.JOptionPane;

/**
 *
 * @author dsladmin
 */
public class OracleDatabase extends DatabaseAbstract {

    // qno used by getPlan
    private int qno;
    /**
     * Constructs a OracleDatabase instance with default values
     * @param settings Database DBSettings object
     * @throws DatalessException
     */
    public OracleDatabase(DBSettings settings) throws DatalessException {
        super(settings);
        qno = 0;
    }

    @Override
    public String getQuery_stopAutoUpdateStats() {
        if(settings.getUserName().compareToIgnoreCase("system") == 0)
        {
            return "begin "+
                "DBMS_AUTO_TASK_ADMIN.DISABLE(client_name => 'auto optimizer stats collection', operation => NULL, window_name => NULL); "+
                "end; ";
        }
        else
        {
            JOptionPane.showMessageDialog(null,"Warning : The user of the database is not a DBA. We could not able to stop auto update of statistics.", "CODD - Warning ",JOptionPane.WARNING_MESSAGE);
            return null;
        }
    }

    @Override
    public String getQuery_SelectRelations() {
        return "select TABLE_NAME from ALL_TABLES where OWNER = '" + settings.getSchema() + "'";
    }

    @Override
    public String getQuery_dependentRelations(String relation) {
        return "SELECT TABLE_NAME FROM ALL_CONSTRAINTS WHERE R_CONSTRAINT_NAME = ( "
                + " SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE OWNER='"+settings.getSchema()+"' AND CONSTRAINT_TYPE='P' AND TABLE_NAME='"+relation+"'"
                + ")";
    }

    @Override
    public String getQuery_columnDataType(String relation, String column) {
        return "select DATA_TYPE from ALL_TAB_COLUMNS where TABLE_NAME='" + relation + "' AND COLUMN_NAME='" + column + "' AND OWNER='"+settings.getSchema()+"'";
    }

    @Override
    public String getQuery_getIndexedColumns(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME, INDEX_NAME  FROM USER_IND_COLUMNS WHERE TABLE_NAME='"+relation+"'";
    }

    @Override
    public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
        String query = "SELECT INDEX_NAME  FROM USER_IND_COLUMNS WHERE TABLE_NAME='"+relation+"' AND (";
        for(int i=0;i<cols.size();i++)
        {

            if(i == cols.size() -1 ) {
                query = query + " COLUMN_NAME='"+cols.get(i)+"' )";
            } else {
                query = query + " COLUMN_NAME='"+cols.get(i)+"' OR ";
            }
        }
        return query;
    }

    @Override
    public String getQuery_getAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME  FROM USER_TAB_COLUMNS WHERE TABLE_NAME='"+relation+"'";
    }

    @Override
    public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ALL_CONSTRAINTS cons, ALL_CONS_COLUMNS cols WHERE cols.TABLE_NAME = '"+relation+"' AND cons.CONSTRAINT_TYPE = 'P' "
                + "AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME AND cons.OWNER = '"+getSchema()+"'";
    }

    @Override
    public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
        return "SELECT a.TABLE_NAME, c.COLUMN_NAME FROM ALL_CONSTRAINTS a, ALL_CONSTRAINTS b, ALL_CONS_COLUMNS c "
                    + "WHERE b.OWNER='"+getSchema()+"' AND a.OWNER=b.OWNER AND b.CONSTRAINT_TYPE = 'R' and b.R_CONSTRAINT_NAME = a.CONSTRAINT_NAME "
                    + "AND b.TABLE_NAME='"+relation+"' AND c.CONSTRAINT_NAME = b.CONSTRAINT_NAME";
    }

    @Override
    public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ALL_CONSTRAINTS cons, ALL_CONS_COLUMNS cols WHERE cols.TABLE_NAME = '"+relation+"' AND cons.CONSTRAINT_TYPE = 'R' "
                + "AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME AND cons.OWNER = '"+getSchema()+"'";
    }

    @Override
    public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ALL_CONSTRAINTS cons, ALL_CONS_COLUMNS cols WHERE cols.TABLE_NAME = '"+relation+"' AND cons.CONSTRAINT_TYPE = 'U' "
                + "AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME AND cons.OWNER = '"+getSchema()+"'";
    }

    @Override
    public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
        // N - NOT NULL, Y - NULLABLE
        return "SELECT NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME='"+relation+"' AND COLUMN_NAME='"+column+"' ";
    }

    @Override
    public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
        return "SELECT a.TABLE_NAME, d.COLUMN_NAME"
                + " FROM ALL_CONSTRAINTS a, ALL_CONSTRAINTS b, ALL_CONS_COLUMNS c, ALL_CONS_COLUMNS d "
                + " WHERE b.OWNER='"+getSchema()+"' AND a.OWNER=b.OWNER AND b.CONSTRAINT_TYPE = 'R' and b.R_CONSTRAINT_NAME = a.CONSTRAINT_NAME AND b.TABLE_NAME='"+relation+"' "
                + "AND c.CONSTRAINT_NAME = b.CONSTRAINT_NAME and c.COLUMN_NAME='"+column+"' AND a.CONSTRAINT_NAME = d.CONSTRAINT_NAME AND c.POSITION=d.POSITION";
    }

    @Override
    public boolean connect(DBSettings settings) throws DatalessException{
        String connectString;
        if(isConnected())
            return true;
        this.settings = settings;
        try
        {
            connectString = "jdbc:oracle:thin:@//" + settings.getServerName() + ":" + settings.getServerPort()
                    + "/" + settings.getDbName() ;
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            con = DriverManager.getConnection(connectString, settings.getUserName(), settings.getPassword());

        } catch (Exception e)	{
            //Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database Engine "+settings.getDbName()+" is not accepting connections");
        }
        if(con != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean retain(String [] dropRelations, String[] dependentRelations) throws DatalessException {
        /*The process goes as follows:
         * Lock the Statistcs
         * Disable all FK Constraints
         * Truncate the relations.
         * Enable all FK Constraints
         * Unlock the Statistics??
        */
        int k = 0;
        int len = dropRelations.length+dependentRelations.length;
        String[] allRelations = new String[len];
        for(int b=0;b<dropRelations.length;b++)
        {
            allRelations[k] = dropRelations[b];
            k++;
        }
        for(int b=0;b<dependentRelations.length;b++)
        {
            allRelations[k] = dependentRelations[b];
            k++;
        }
        // Table Name, Constraint Name
        ArrayList<String[]> FKConstraints = new ArrayList();
        try{

            // 1) Lock the statistics
            for (k = 0; k < len; k++) {
                String relation = allRelations[k];
                String query = "begin "+
                "DBMS_STATS.LOCK_TABLE_STATS('"+settings.getSchema()+"','"+relation+"'); "+
                "end; ";
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeQuery(query);
                stmt.close();
            }

            // 2) Disable all FK Constraints
            for (k = 0; k < len; k++) {
                String relation = allRelations[k];
                String query = "SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE OWNER='" + settings.getSchema() + "' AND TABLE_NAME = '" + relation + "' AND CONSTRAINT_TYPE='R'";
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                ResultSet rset = stmt.executeQuery(query);
                while (rset.next()) {
                    String constraint = rset.getString(1);
                    String[] cons = new String[2];
                    cons[0] = relation;
                    cons[1] = constraint;
                    FKConstraints.add(cons);
                    Constants.CPrintToConsole(constraint+", ", Constants.DEBUG_SECOND_LEVEL_Information);
                }
                Constants.CPrintToConsole("", Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.close();
            }

            for (int l = 0; l < FKConstraints.size(); l++) {
                String[] cons = FKConstraints.get(l);
                String query = "ALTER TABLE " + cons[0] + " DISABLE CONSTRAINT " + cons[1];
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeQuery(query);
                stmt.close();
            }

            // 3) Truncate the relations
            for (k = 0; k < len; k++) {
                String relation = allRelations[k];
                String query = "TRUNCATE TABLE " + relation;
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeQuery(query);
                stmt.close();
            }

            // 4) Enable all FK Constraints
            for (int l = 0; l < FKConstraints.size(); l++) {
                String[] cons = FKConstraints.get(l);
                String query = "ALTER TABLE " + cons[0] + " ENABLE CONSTRAINT " + cons[1];
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeQuery(query);
                stmt.close();
            }

            // 6) UnLock the statistics
            for (k = 0; k < len; k++) {
                String relation = allRelations[k];
                String query = "begin "+
                "DBMS_STATS.UNLOCK_TABLE_STATS('"+settings.getSchema()+"','"+relation+"'); "+
                "end; ";
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeQuery(query);
                stmt.close();
            }
        }
        catch(Exception e)
        {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in dropping Relations of "+settings.getDbName());
        }
        return true;
    }

    @Override
    public boolean transfer(String[] relation, Database destDatabase) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void construct(String[] relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void collectStatistics(String relation) throws DatalessException {
        try {
            Statement stmt = createStatement();
            String command = "begin "+
                "DBMS_STATS.GATHER_TABLE_STATS ('"+getSchema()+"','"+relation+"', method_opt => 'FOR ALL COLUMNS'); "+
                "end; ";
            Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(command);
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating Statistics for " + relation + " of " + settings.getDbName());
        }
    }

    private String getProcedure_GetRelationStats(String relation)
    {
        return "DECLARE "+
               "numrows number; "+
               "numblocks number; "+
               "avgrowlen number; "+
               "begin "+
               "DBMS_STATS.GET_TABLE_STATS(ownname => '"+ settings.getSchema() +"',tabname => '"+relation+"',numrows => numrows, numblks => numblocks, avgrlen => avgrowlen); "+
               "DBMS_OUTPUT.put_line ( numrows || '::' || numblocks || '::' || avgrowlen);"+
               "end; ";
    }

    @Override
    public RelationStatistics getRelationStatistics(String relation) throws DatalessException {
        OracleRelationStatistics relStat = new OracleRelationStatistics(relation, this.getSchema());
        /*
        String command = "SELECT NUM_ROWS, BLOCKS, AVG_ROW_LEN FROM ALL_TABLES WHERE TABLE_NAME = '" + relation + "' AND OWNER = '" + this.getSchema() + "'";
         */
        try
        {
            // Using DBMS_GET_TABLE_STATS Procedure
            Statement stmt = createStatement();
            DbmsOutput dbmsOutput = new DbmsOutput( con );
            dbmsOutput.enable(1000000);
            String getRelStatsProcedure = getProcedure_GetRelationStats(relation);
            Constants.CPrintToConsole(getRelStatsProcedure, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.execute(getRelStatsProcedure);
            String result = dbmsOutput.getResults();
            dbmsOutput.close();
            stmt.close();
            String[] results =  result.split("::");
            BigDecimal card = new BigDecimal(results[0]);
            BigDecimal blocks = new BigDecimal(results[1]);
            BigDecimal avgRowLen = new BigDecimal(results[2]);
            relStat.setCardinality(card);
            relStat.setBlocks(blocks);
            relStat.setAvgRowLen(avgRowLen);
            Constants.CPrintToConsole("card|blocks|avg_row_len", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole(card + "|" + blocks + "|" + avgRowLen , Constants.DEBUG_SECOND_LEVEL_Information);
        }
        catch(Exception e)
        {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in reading Relation "+relation+" Statistics of "+settings.getDbName());
        }
        return relStat;
    }

    private String getProcedure_GetColumnStats(String relation, String column, String dataType)
    {
        String sql_block;
        sql_block = "DECLARE "+
                    "m_distcnt number; "+
                    "m_density number; "+
                    "m_nullcnt number; "+
                    "srec dbms_stats.statrec; "+
                    "m_avgclen number; ";
        if (dataType.equalsIgnoreCase(DataType.NUMBER)) {
            sql_block = sql_block
                    + "n_array dbms_stats.numarray; "
                    + "minval number; "
                    + "maxval number; "
                    + "tempnoval number; ";
        } else if (dataType.equalsIgnoreCase(DataType.DATE)) {
            sql_block = sql_block
                    + "n_array dbms_stats.datearray; "
                    + "minval date; "
                    + "maxval date; "
                    + "tempnoval date; ";
            /*
        } else if (dataType.equalsIgnoreCase(DataType.FLOAT)) {
            sql_block = sql_block
                    + "n_array dbms_stats.fltarray; "
                    + "minval binary_float; "
                    + "maxval binary_float; "
                    + "tempnoval binary_float; ";
        } else if (dataType.equalsIgnoreCase(DataType.DOUBLE)) {
            sql_block = sql_block
                    + "n_array dbms_stats.dblarray; "
                    + "minval binary_double; "
                    + "maxval binary_double; "
                    + "tempnoval binary_double; ";
            */
        } else {
            sql_block = sql_block
                    + "n_array dbms_stats.chararray; "
                    + "minval varchar2(32); "
                    + "maxval varchar2(32); "
                    + "tempnoval varchar2(32); ";
        }
        sql_block = sql_block +
                    "begin "+
                    "dbms_stats.get_column_stats(ownname => '"+ settings.getSchema()+"',tabname => '"+ relation +"',colname => '"+column+"', distcnt => m_distcnt, density => m_density, "
                    + "nullcnt => m_nullcnt, srec => srec, avgclen => m_avgclen); "+
                    "DBMS_OUTPUT.put_line (m_distcnt || '::' || m_density || '::' || m_nullcnt || '::' || m_avgclen);"+
                    "DBMS_STATS.convert_raw_value(srec.minval,minval); "+
                    "DBMS_STATS.convert_raw_value(srec.maxval,maxval); "+
                    "DBMS_OUTPUT.put_line (srec.epc || '::' || minval || '::' || maxval);"+
                     "FOR i IN 1 .. srec.novals.COUNT "
                     + "LOOP "
                        //+"DBMS_STATS.convert_raw_value(srec.novals(i),tempnoval); "
                        //+ "DBMS_OUTPUT.put_line ( i || '::' || tempnoval || '( ' || srec.novals(i) || ')' );"
                        + "DBMS_OUTPUT.put_line ( i || '::' || srec.novals(i) || '::' || srec.bkvals(i) || '::' || srec.chvals(i));"
                        + "END LOOP;"
                    + "end; ";

        return sql_block;

    }

    @Override
    public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard) throws DatalessException {
        String type = getType(relation,column);
        OracleColumnStatistics colStat = null;
        try
        {
            colStat = new OracleColumnStatistics(relation, column, type, getConstraint(relation, column));
        }
        catch(Exception e)
        {
             Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception intialzing Column Statisitcs "+column+" ("+relation+")  of "+settings.getDbName());
        }
        /**
         * Column Statistics from USER_TAB_COL_STATISTICS
         */
        String histogramType = OracleColumnStatistics.None;
        try
        {
            /*
            String command = "SELECT NUM_DISTINCT, DENSITY, NUM_NULLS, AVG_COL_LEN, NUM_BUCKETS, HISTOGRAM FROM USER_TAB_COL_STATISTICS WHERE table_name = '"+relation+"' AND column_name = '"+column+"'";
             */
            String command = "SELECT HISTOGRAM FROM USER_TAB_COL_STATISTICS WHERE table_name = '"+relation+"' AND column_name = '"+column+"'";
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(command);
            while (rset.next()) {
                histogramType = rset.getString(1);
            }
            colStat.setHistogramType(histogramType);
            rset.close();
            if(!histogramType.equals(OracleColumnStatistics.None))
            {
                DbmsOutput dbmsOutput = new DbmsOutput(con);
                dbmsOutput.enable(1000000);
                String getColumnStatsProcedure = getProcedure_GetColumnStats(relation, column, type);
                Constants.CPrintToConsole(getColumnStatsProcedure, Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.execute(getColumnStatsProcedure);
                String result = dbmsOutput.getResults();
                dbmsOutput.close();
                stmt.close();
                String[] results = result.split("\n");
                String line;
                String[] params;
                // first Line - (m_distcnt || '::' || m_density || '::' || m_nullcnt || '::' || m_avgclen)
                line = results[0];
                params = line.split("::");
                BigDecimal col_card = new BigDecimal(params[0]);
                Double density = new Double(params[1]);
                BigDecimal null_cnt = new BigDecimal(params[2]);
                Integer avg_col_len = new Integer(params[3]);
                colStat.setColCard(col_card);
                colStat.setDensity(density);
                colStat.setNumNulls(null_cnt);
                colStat.setAvgColLen(avg_col_len);
                Constants.CPrintToConsole("col_card|null_cnt|avg_col_len|histogramType", Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole(col_card + "|" + null_cnt + "|" + avg_col_len + "|" + histogramType, Constants.DEBUG_SECOND_LEVEL_Information);
                // second Line - (srec.epc || '::' || minval || '::' || maxval)
                line = results[1];
                params = line.split("::");
                Integer num_buckets = new Integer(params[0]);
                DataType minVal = new DataType(type, params[1]);
                DataType maxVal = new DataType(type, params[2]);
                colStat.setNumBuckets(num_buckets);
                colStat.setMinValue(minVal);
                colStat.setMaxValue(maxVal);
                Constants.CPrintToConsole("num_bukcets|minVal|maxVal", Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole(num_buckets + "|" + minVal.getString() + "|" + maxVal.getString(), Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole("no|endpointnumber|endpointvalue|endpointactualvalue", Constants.DEBUG_SECOND_LEVEL_Information);
                OracleHistObject[] hist = new OracleHistObject[num_buckets];
                boolean actualPresent = false;
                for (int lineno = 0; lineno < num_buckets; lineno++) {
                    line = results[lineno + 2];
                    params = line.split("::");
                    String endPointNumber = params[2];
                    String endPointValue = params[1];
                    String endPointActualValue = null;
                    if (params.length > 3) {
                        actualPresent = true;
                        endPointActualValue = params[3];
                    }
                    OracleHistObject histObj = new OracleHistObject(endPointNumber, endPointValue, endPointActualValue);
                    hist[lineno] = histObj;
                    Constants.CPrintToConsole(lineno + "|" + endPointNumber + "|" + endPointValue + "|" + endPointActualValue, Constants.DEBUG_SECOND_LEVEL_Information);
                }
                colStat.setOralceHistogram(hist);
                colStat.setIsActualValuePresent(actualPresent);
                // Update Quantile histogram if enough details are available.
                if (histogramType.equals(OracleColumnStatistics.HeightBalanced)) {
                    if (DataType.isDouble(type) || DataType.isInteger(type) || DataType.isNumeric(type) || actualPresent) {
                        Constants.CPrintToConsole("Quantile Histogram is available.", Constants.DEBUG_SECOND_LEVEL_Information);
                        /**
                         * Column Statistics from SYSSTAT.COLDIST 'Quantile Histogram'
                         *
                         * has to store 4 values. SEQNO, COLVALUE, VALCOUNT, DISTCOUNT
                         * (Sequence Number, Column Value, Frequency, Distinct Values Count [less than or equal COLVALUE])
                         */
                        TreeMap<Integer, HistogramObject> map = new TreeMap();
                        Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT|distCount", Constants.DEBUG_SECOND_LEVEL_Information);
                        Double height = new Double(tableCard.doubleValue()) / num_buckets;
                        for (int lineno = 0; lineno < num_buckets; lineno++) {
                            line = results[lineno + 2];
                            params = line.split("::");
                            String endPointValue = params[1];
                            String endPointActualValue = null;
                            if (actualPresent) {
                                endPointActualValue = params[3];
                            }
                            Integer seqno = new Integer(lineno + 1);
                            String col = null;
                            if (DataType.isDouble(type) || DataType.isInteger(type) || DataType.isNumeric(type)) {
                                col = endPointValue;
                            } else {
                                col = endPointActualValue;
                            }
                            Double valCount = new Double(height);
                            Double distCount = null;
                            Constants.CPrintToConsole(seqno + "|" + col + "|" + valCount + "| " + distCount, Constants.DEBUG_SECOND_LEVEL_Information);
                            HistogramObject histogramObject = new HistogramObject(col, valCount, distCount);
                            map.put(seqno, histogramObject);
                        }
                        colStat.setHistogram(map);
                    }
                }
            }
        }
        catch(Exception e)
        {
             Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in reading Column "+column+" ("+relation+") from DBMS_STATS.GET_COLUMN_STATS procedure of "+settings.getDbName());
        }
        return colStat;
    }

    private String getProcedure_GetIndexStats(String relation, String ind_name)
    {
        return "DECLARE "+
               "ind_rows number; "+
               "ind_leaf_blocks number; "+
               "ind_dist_keys number; "+
               "avg_leaf_blocks number; "+
               "avg_data_blocks number; "+
               "clst_fact number; "+
               "ind_level number; "+
               "begin "+
               "DBMS_STATS.GET_INDEX_STATS(ownname => '"+ settings.getSchema() +"',indname => '"+ind_name+"',numrows => ind_rows, numlblks => ind_leaf_blocks"+
               ", numdist => ind_dist_keys, avglblk => avg_leaf_blocks, avgdblk => avg_data_blocks, clstfct => clst_fact, indlevel => ind_level); "+
               "DBMS_OUTPUT.put_line (ind_rows || '::' || ind_leaf_blocks || '::' || ind_dist_keys || '::' || avg_leaf_blocks || '::' || avg_data_blocks || '::' || clst_fact || '::' || ind_level);"+
               "end; ";
    }

    @Override
    public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {
        OracleIndexStatistics indexStat = new OracleIndexStatistics(relation, colNames);
        String indexName = getIndexName(relation, colNames);
        // indexName shouldn't be null
        /*
        String command = "SELECT NUM_ROWS, LEAF_BLOCKS, DISTINCT_KEYS, AVG_LEAF_BLOCKS_PER_KEY, AVG_DATA_BLOCKS_PER_KEY, CLUSTERING_FACTOR, BLEVEL FROM USER_IND_STATISTICS "+
                                " WHERE INDEX_NAME = '"+indexName+"'AND TABLE_NAME = '"+relation+"'";
         */
        if(indexName != null){ //Happens if there are multiple indexes on a column(system and user-created one) as well. 
        try
        {
            // Using DBMS_GET_TABLE_STATS Procedure
            Statement stmt = createStatement();
            DbmsOutput dbmsOutput = new DbmsOutput( con );
            dbmsOutput.enable(1000000);
            String getIndexStatsProcedure = getProcedure_GetIndexStats(relation, indexName);
            Constants.CPrintToConsole(getIndexStatsProcedure, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.execute(getIndexStatsProcedure);
            String result = dbmsOutput.getResults();
            dbmsOutput.close();
            stmt.close();
            String[] results =  result.split("::");
            BigDecimal NUM_ROWS = new BigDecimal(results[0]);
            BigDecimal LEAF_BLOCKS = new BigDecimal(results[1]);
            BigDecimal DISTINCT_KEYS = new BigDecimal(results[2]);
            BigDecimal AVG_LEAF_BLOCKS_PER_KEY = new BigDecimal(results[3]);
            BigDecimal AVG_DATA_BLOCKS_PER_KEY = new BigDecimal(results[4]);
            Double CLUSTERING_FACTOR = new Double(results[5]);
            BigDecimal IND_LEVEL = new BigDecimal(results[6]);

            indexStat.setNumRows(NUM_ROWS);
            indexStat.setLeafBlocks(LEAF_BLOCKS);
            indexStat.setDistinctKeys(DISTINCT_KEYS);
            indexStat.setAvgLeafBlocksPerKey(AVG_LEAF_BLOCKS_PER_KEY);
            indexStat.setAvgDataBlocksPerKey(AVG_DATA_BLOCKS_PER_KEY);
            indexStat.setClusteringFactor(CLUSTERING_FACTOR);
            indexStat.setIndLevel(IND_LEVEL);
            Constants.CPrintToConsole("NUM_ROWS | LEAF_BLOCKS | DISTINCT_KEYS | AVG_LEAF_BLOCKS_PER_KEY | AVG_DATA_BLOCKS_PER_KEY | CLUSTERING_FACTOR | IND_LEVEL", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole(NUM_ROWS + " | " + LEAF_BLOCKS + " | " + DISTINCT_KEYS + " | " + AVG_LEAF_BLOCKS_PER_KEY + " | " + AVG_DATA_BLOCKS_PER_KEY + " | " + CLUSTERING_FACTOR + " | " + IND_LEVEL, Constants.DEBUG_SECOND_LEVEL_Information);
        }
        catch(Exception e)
        {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in reading index Statistics "+indexName+" of "+relation+" from DBMS_STATS.GET_INDEX_STATS procedure of "+settings.getDbName());
        }
        }
        return indexStat;
    }

    private String getProcedure_SetRelationStats(String relation, BigDecimal numRows, BigDecimal numBlocks, BigDecimal avgRowLen)
    {
        return "begin "+
               "DBMS_STATS.SET_TABLE_STATS(ownname => '"+ settings.getSchema() +"',tabname => '"+relation+"',numrows => "+numRows+",numblks => "+numBlocks+",avgrlen => "+avgRowLen+"); "+
               "end; ";
    }
    
    private String getProcedure_SetSystemStats(BigDecimal cpuspeedNW, BigDecimal ioseektim, BigDecimal iotfrspeed, BigDecimal cpuspeed, 
    		BigDecimal sreadtim, BigDecimal mreadtim, BigDecimal mbrc, BigDecimal maxthr, BigDecimal slavethr)
    {
        return "begin "+
               "DBMS_STATS.DELETE_SYSTEM_STATS;"+
        		"IF " +cpuspeedNW+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('CPUSPEEDNW',"+cpuspeedNW+");"+
        	    "END IF;"+
        	    "IF " +ioseektim+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('IOSEEKTIM', "+ioseektim+");"+
        		"END IF;"+
        		"IF " +iotfrspeed+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('IOTFRSPEED', "+iotfrspeed+");"+
        		"END IF;"+
        		"IF " +cpuspeed+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('CPUSPEED', "+cpuspeed+");"+
        		"END IF;"+
        		"IF " +sreadtim+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('SREADTIM', "+sreadtim+");"+
        		"END IF;"+
        		"IF " +mreadtim+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('MREADTIM', "+mreadtim+");"+
        		"END IF;"+
        		"IF " +mbrc+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('MBRC', "+mbrc+");"+
        		"END IF;"+
        		"IF " +maxthr+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('MAXTHR', "+maxthr+");"+
        		"END IF;"+
        		"IF " +slavethr+ " IS NOT NULL THEN "+
        		"DBMS_STATS.SET_SYSTEM_STATS('SLAVETHR', "+slavethr+");"+
        		"END IF;"+
               "end; ";
    }
    
    @Override
    public boolean setHardwareStatistics() throws DatalessException {
    	OracleSystemStatistics sysStat = OracleSystemStatistics.getInstance();
    	if(!sysStat.skipped){
    		try{
	    		Statement stmt = createStatement();
	    		String command = 
	    				getProcedure_SetSystemStats(sysStat.cpuSpeedNW,sysStat.ioSeekTim,sysStat.ioTFRSpeed,sysStat.cpuSpeed,sysStat.sReadTim,
	    						sysStat.mReadTim,sysStat.mbrc,sysStat.maxThr,sysStat.slaveThr);
	            Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
	            stmt.executeUpdate(command);
	            String command2 = "SELECT ISSYS_MODIFIABLE FROM V$PARAMETER WHERE NAME = 'parallel_max_servers'";
	            ResultSet rset = stmt.executeQuery(command2);
	            if(rset.next()){
	            	String value = rset.getString("ISSYS_MODIFIABLE");
	            	if(value.equalsIgnoreCase("IMMEDIATE") && 
	            	         sysStat.parallelMaxServers != null){
	            		String command3 = "ALTER SYSTEM SET parallel_max_servers = "+ sysStat.parallelMaxServers +" SCOPE=BOTH";
	            		stmt.execute(command3);
	            	}
	            }
	            stmt.close();
	            
    		} catch (Exception e)	{
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in updating Hardware Statistics of "+settings.getDbName());
            }
    	}
    	return true;
    }

    @Override
    public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException {
        //boolean sameDBType = false;
        OracleRelationStatistics relStat = new OracleRelationStatistics(relation, relationStatistics.getSchema());
        relStat.mapForPorting(relationStatistics);
        /*
        if(relationStatistics instanceof OracleRelationStatistics)
        {
            relStat = (OracleRelationStatistics) relationStatistics;
            sameDBType = true;
        }
        */
        try{
            Statement stmt = createStatement();
            /**
             * Delete All Statistics first, then update it.
             * DELETE_TABLE_STATS inturn deletes the column and index stats.
             */
            String command1 = "begin "+
                              "DBMS_STATS.DELETE_TABLE_STATS(ownname => '"+ settings.getSchema() +"',tabname => '"+relation+"'); "+
                              "end; ";
            Constants.CPrintToConsole(command1, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(command1);
            String command;

            //if(sameDBType)
                command= getProcedure_SetRelationStats(relation, relStat.getCardinality(), relStat.getBlocks(),relStat.getAvgRowLen());
            //else
               // command= getProcedure_SetRelationStats(relation, relationStatistics.getCardinality(), relationStatistics.getPages(), BigDecimal.ZERO);

            Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(command);
            stmt.close();
        } catch (Exception e)	{
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating Relation "+relation+" Statistics of "+settings.getDbName());
        }
        return true;
    }

    private String getProcedure_SetColumnStats(String relation, String column, String dataType, boolean userInput, String [] endpoint_number, String [] endpoint_value, BigDecimal dist_cnt, Double density, BigDecimal null_cnt, Integer avg_col_len, Integer buckets, String histogramType)
    {
        // If !userInput, then the values are already prepared (transferred from oracle).
        // Prepare Histogram Values.
        String endpoint_value_input = "";
        String endpoint_number_input = "";

        if (buckets > 0 && dataType.equalsIgnoreCase(DataType.NUMBER)) {
            for (int i = 0; i < (buckets - 1); i++) {
                endpoint_value_input += endpoint_value[i] + ",";
                endpoint_number_input += endpoint_number[i] + ",";
            }
            endpoint_value_input += endpoint_value[buckets - 1];
            endpoint_number_input += endpoint_number[buckets - 1];
        } else if (buckets > 0 && dataType.equalsIgnoreCase(DataType.DATE)) {
            for (int i = 0; i < (buckets - 1); i++) {
                endpoint_value_input += "'" + endpoint_value[i] + "',";
                endpoint_number_input += endpoint_number[i] + ",";
            }
            endpoint_value_input += "'" + endpoint_value[buckets - 1] + "'";
            endpoint_number_input += endpoint_number[buckets - 1];
        } else if(buckets > 0 ) {
            for (int i = 0; i < (buckets - 1); i++) {
                endpoint_value_input += "'" + endpoint_value[i] + "',";
                endpoint_number_input += endpoint_number[i] + ",";
            }
            endpoint_value_input += "'" + endpoint_value[buckets - 1] + "'";
            endpoint_number_input += endpoint_number[buckets - 1];
        }
        Constants.CPrintToConsole(endpoint_value_input + "\t" + endpoint_number_input, Constants.DEBUG_SECOND_LEVEL_Information);
        String sql_block;
        sql_block = "DECLARE "+
                    "m_distcnt number; "+
                    "m_density number; "+
                    "m_nullcnt number; "+
                    "srec dbms_stats.statrec; "+
                    "m_avgclen number; ";
        if (!userInput || dataType.equalsIgnoreCase(DataType.NUMBER)) {
            sql_block = sql_block +
                        "n_array dbms_stats.numarray; ";
        } else if (dataType.equalsIgnoreCase(DataType.DATE)) {
            sql_block = sql_block +
                        "n_array dbms_stats.datearray; ";
        } else {
            sql_block = sql_block +
                        "n_array dbms_stats.chararray; ";
        }
        sql_block = sql_block +
                    "begin "+
                    "m_distcnt := "+dist_cnt+"; "+
                    "m_density := "+density +"; "+
                    "m_nullcnt := "+null_cnt +"; "+
                    "m_avgclen := "+avg_col_len+"; ";
        if (!userInput || dataType.equalsIgnoreCase(DataType.NUMBER)) {
            sql_block = sql_block +
                        "n_array := dbms_stats.numarray("+endpoint_number_input+"); ";
        } else if (dataType.equalsIgnoreCase(DataType.DATE)) {
            sql_block = sql_block +
                        "n_array := dbms_stats.datearray("+endpoint_value_input+"); ";
        } else {
            sql_block = sql_block +
                        "n_array := dbms_stats.chararray("+endpoint_value_input+"); ";
        }
        if (histogramType.equals(OracleColumnStatistics.Frequency)) {
            sql_block = sql_block +
                        "srec.bkvals := dbms_stats.numarray("+endpoint_number_input+"); ";
        } else {
            sql_block = sql_block +
                        "srec.bkvals := null; ";
        }
        sql_block = sql_block +
                    "srec.epc := "+buckets+"; "+
                    "dbms_stats.prepare_column_values(srec, n_array); "+
                    "dbms_stats.set_column_stats(ownname => '"+ settings.getSchema()+"',tabname => '"+ relation +"',colname => '"+column+"',distcnt => m_distcnt,density => m_density,nullcnt => m_nullcnt,srec => srec,avgclen => m_avgclen); "+
                    "end; ";

        return sql_block;

    }

    @Override
    public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException {
        boolean sameDBType = false;
        String type = getType(relation, column);
        OracleColumnStatistics colStat = new OracleColumnStatistics(relation, column,  type, getConstraint(relation, column));
        colStat.mapForPorting(columnStatistics, tableCard);

        if(columnStatistics instanceof OracleColumnStatistics)
        {
            //colStat = (OracleColumnStatistics)columnStatistics;
            sameDBType = true;
        }
        try
        {
            String dataType = this.getType(relation, column);
            boolean userInput = false;
            BigDecimal dist_cnt = new BigDecimal(BigDecimal.ZERO+"");
            Double density = 0.0;
            BigDecimal null_cnt = colStat.getNumNulls();
            Integer avg_col_len = 0;
            Integer buckets = 0;
            String [] endpoint_number = null;
            String [] endpoint_value = null;
            dist_cnt = colStat.getColCard();
            density = colStat.getDensity();
            avg_col_len = colStat.getAvgColLen();

            String histogramType = OracleColumnStatistics.None;
            if (sameDBType)
            {
                buckets = colStat.getNumBuckets();
                histogramType = colStat.getHistogramType();
                if(!histogramType.equals(OracleColumnStatistics.None))
                {
                    // Read OracleHistObject and update endpoint_number, endpoint_value
                    OracleHistObject[] hist = colStat.getOralceHistogram();
                    endpoint_number = new String[buckets];
                    endpoint_value = new String[buckets];
                    for (int i = 0; i < buckets; i++) {
                        endpoint_number[i] = hist[i].getEndPointNumber();
                        endpoint_value[i] = hist[i].getEndPointValue();
                    }
                }
            } else {
                histogramType = OracleColumnStatistics.HeightBalanced;
                // Adjust Histogram For Everybody
                // columnStatistics.adjustHistogram(tableCard, OracleColumnStatistics.DefaultHeightBalancedBucketSize);
                // TreeMap<Integer, HistogramObject> map = (TreeMap)columnStatistics.getHistogram();
                TreeMap<Integer, HistogramObject> map = (TreeMap)colStat.getHistogram();
                if(map != null)
                {
                    Set set = map.entrySet();
                    Iterator i = set.iterator();
                    endpoint_number = new String[set.size()];
                    endpoint_value = new String[set.size()];
                    int e = 0;
                    while(i.hasNext()) {
                        Map.Entry me = (Map.Entry)i.next();
                        HistogramObject histogramObject = (HistogramObject) me.getValue();
                        String colValue = histogramObject.getColValue();
                        endpoint_number[e] = ""; // Will not be used as it is HeightBalancedHistogram
                        endpoint_value[e] = colValue;
                        e++;
                    }
                }
            }
            if(!histogramType.equals(OracleColumnStatistics.None))
            {
                String command = getProcedure_SetColumnStats(relation, column, dataType, userInput, endpoint_number, endpoint_value, dist_cnt, density, null_cnt, avg_col_len, buckets, histogramType);
                Statement stmt = createStatement();
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.executeUpdate(command);
                stmt.close();
            }
            return true;
        }
        catch(Exception e)
        {
             Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating Frequency Histogram of Column "+column+" ("+relation+") from DBMS_STATS.SET_COLUMN_STATS of "+settings.getDbName());
        }

    }

    private String getProcedure_SetIndexStats(String ind_name, BigDecimal ind_rows,BigDecimal ind_leaf_blocks,BigDecimal ind_dist_keys,BigDecimal avg_leaf_blocks,BigDecimal avg_data_blocks,double clst_fact, BigDecimal ind_level)
    {
        return "begin "+
               "DBMS_STATS.SET_INDEX_STATS(ownname => '"+ settings.getSchema() +"',indname => '"+ind_name+"',numrows => "+ind_rows+",numlblks => "+ind_leaf_blocks+",numdist => "+ind_dist_keys+",avglblk => "+avg_leaf_blocks+",avgdblk => "+avg_data_blocks+",clstfct => "+clst_fact+",indlevel => "+ind_level+"); "+
               "end; ";
    }

    @Override
    public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException {
        //boolean sameDBType = false;
        OracleIndexStatistics indexStat = new OracleIndexStatistics(relation, colNames);
        indexStat.mapForPorting(indexStatistics);
        String indexName = getIndexName(relation, colNames);
        if(indexName == null) {
            Constants.CPrintToConsole("Index doesn't exist for relation.", Constants.DEBUG_SECOND_LEVEL_Information);
            return true;
        }
        /*
        if(indexStatistics instanceof OracleIndexStatistics)
        {
            indexStat = (OracleIndexStatistics)indexStatistics;
            sameDBType = true;
        }
        */
        try {
            //if(sameDBType)
            {
                Statement stmt = createStatement();
                String command = getProcedure_SetIndexStats(indexName, indexStat.getNumRows(), indexStat.getLeafBlocks(), indexStat.getDistinctKeys(), indexStat.getAvgLeafBlocksPerKey(), indexStat.getAvgDataBlocksPerKey(), indexStat.getClusteringFactor(), indexStat.getIndLevel());
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.executeUpdate(command);
                stmt.close();
            }
            return true;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating index ("+indexName+")Statistics for Relation "+relation+" Statistics of "+settings.getDbName());
        }
    }

    @Override
    public Plan getPlan(String query) throws DatalessException {
        Plan plan;
        ResultSet rset;
        Statement stmt;

        // fire Query
        try {
            qno++;
            stmt = createStatement();
            String explainQuery = "explain plan set statement_id='" + qno + "' for " + query;
            Constants.CPrintToConsole(explainQuery, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(explainQuery);
            //System.err.println("explain plan set statement_id='" + qno + "' for " + query + ";");
            stmt.close();
        }
        catch (SQLException e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error explaining query: " + e);
        }

        // Initialize
        plan = new Plan();
        /*
        String planQuery = "select id,parent_id,operation,object_name, "
                + "cost, cardinality, options from PLAN_TABLE where statement_id='" + qno + "' order by id";
         *
         */
        // Output List
        String planQuery = "select id,parent_id,operation,object_name, "
                + "cost, cardinality, options, ACCESS_PREDICATES, FILTER_PREDICATES, PROJECTION from PLAN_TABLE where statement_id='" + qno + "' order by id";
        try {
            // getting information from plan_table table to get plan
            // tree information
            Node node;
            java.util.Hashtable HT = new java.util.Hashtable();
            stmt = createStatement();
            Constants.CPrintToConsole(planQuery, Constants.DEBUG_SECOND_LEVEL_Information);
            rset = stmt.executeQuery(planQuery);
            //System.err.println(planQuery + ";");
            int curNode = 0;
            String operation, option;
            int indexId = 100;
            while (rset.next()) {
                node = new Node();
                /*
                 * Warning: Update from planQuery
                 * The following is the ordering of information accessed from Oracle explain tables
                 * 1: Id
                 * 2: Parent Id
                 * 3: Operation
                 * 4: Object Name
                 * 5: Cost (CPU Cost)
                 * 6: Cardinality
                 * 7: Options
                 */
                node.setId(rset.getInt(1));
                if (curNode == 0) {
                    node.setParentId(-1);
                } else {
                    node.setParentId(rset.getInt(2));
                }
                operation = rset.getString(3);
                node.setCost(rset.getDouble(5));
                node.setCard(rset.getDouble(6));
                option = rset.getString(7);
                if (option != null) {
                    node.addArgType("options");
                    node.addArgValue(rset.getString(7));
                }
                // OutputList - Start
                if (rset.getString(8) != null) {
                    node.addArgType("Predicates List");
                    node.addArgValue(rset.getString(8));

                }
                if (rset.getString(9) != null) {

                    if (rset.getString(8) != null) {
                        int index = (node.getArgType()).indexOf("Predicates List");
                        String str = (String) node.getArgValue().get(index);
                        node.getArgValue().set(index, str + " " + rset.getString(9));
                    } else {
                        node.addArgType("Predicates List");
                        node.addArgValue(rset.getString(9));
                    }

                }
                if (rset.getString(10) != null) {
                    node.addArgType("Output List");
                    String str = rset.getString(10);
                    str = str.replace("\"", "");
                    node.addArgValue(str);
                }
                // OutputList - End
                node.setName(operation);
                plan.setNode(node, curNode);
                curNode++;
                if (operation.equals("TABLE ACCESS")) { //|| operation.equals("INDEX")){
					/*node = new Node();
                    node.setId(-1);
                    node.setParentId(rset.getInt(1));
                    node.setName(rset.getString(4));
                    node.setCost(0.0);
                    node.setCard(0.0);
                    plan.setNode(node,curNode);
                    curNode++;*/
                    HT.put("" + rset.getInt(1), rset.getString(4));
                }
                if (operation.equals("INDEX")) {
                    String indexStr = "select TABLE_NAME from user_indexes where index_name='" + rset.getString(4) + "'";
                    Statement stmt1 = createStatement();
                    ResultSet rset1 = stmt1.executeQuery(indexStr);
                    int tblacsid = node.getParentId();
                    node = new Node();
                    node.setId(indexId);
                    node.setParentId(rset.getInt(1));
                    node.setName(rset.getString(4));
                    node.setCost(0.0);
                    node.setCard(0.0);
                    plan.setNode(node, curNode);
                    curNode++;


                    if (HT.get("" + tblacsid) != null) {
                        node = new Node();
                        node.setId(-1);
                        node.setParentId(indexId++);
                        node.setName((String) HT.remove("" + tblacsid));
                        node.setCost(0.0);
                        node.setCard(0.0);
                        plan.setNode(node, curNode);
                        curNode++;
                    } else if (rset1.next()) {
                        node = new Node();
                        node.setId(-1);
                        node.setParentId(indexId++);
                        node.setName(rset1.getString(1));
                        node.setCost(0.0);
                        node.setCard(0.0);
                        plan.setNode(node, curNode);
                        curNode++;
                    }
                    rset1.close();
                    stmt1.close();
                }
            }
            rset.close();
            stmt.close();

            for (java.util.Enumeration enum1 = HT.keys(); enum1.hasMoreElements();) {
                int pid = Integer.parseInt((String) enum1.nextElement());
                node = new Node();
                node.setId(-1);
                node.setParentId(pid);
                node.setName((String) HT.get("" + pid));
                node.setCost(0.0);
                node.setCard(0.0);
                plan.setNode(node, curNode);
                curNode++;
            }

        } catch (SQLException e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error accessing plan: " + e);
        }
        return plan;
    }

    @Override
    public String[] getMultiColumnAttributes(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getMultiColumnHistogramId(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getColumnHistogramId(String relation, String column) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}