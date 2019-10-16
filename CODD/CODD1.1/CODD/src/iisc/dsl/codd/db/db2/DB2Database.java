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
package iisc.dsl.codd.db.db2;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DatabaseAbstract;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Implementation of the functionalities specific to DB2, required for CODD.
 * @author dsladmin
 */
public class DB2Database extends DatabaseAbstract {

	private int qno; // qno used by getPlan
	private final int NODE_OFFSET = 100;
	private final int INDEX_NODE_OFFSET = 10;

	/**
	 * Constructs a DB2Database instance with default values
	 * @param settings Database DBSettings object
	 * @throws DatalessException
	 */
	public DB2Database(DBSettings settings) throws DatalessException {
		super(settings);
		System.out.println("db2settings");
		qno = 0;
	}

	@Override
	public String getQuery_stopAutoUpdateStats() {
		return "call ADMIN_CMD ('update db config for "+settings.getDbName()+" using AUTO_MAINT OFF')";
	}

	@Override
	public String getQuery_SelectRelations() {
		return "select TABNAME from SYSCAT.TABLES where TABSCHEMA = '" + settings.getSchema() + "'";
	}

	@Override
	public String getQuery_dependentRelations(String relation) {
		return "select TABNAME from SYSCAT.REFERENCES where REFTABNAME ='"+relation +"' and TABSCHEMA ='"+settings.getSchema()+"'";
	}

	@Override
	public String getQuery_columnDataType(String relation, String column) {
		return "select TYPENAME from SYSCAT.COLUMNS where TABNAME='" + relation + "' AND COLNAME='" + column + "'";
	}

	@Override
	public String getQuery_getIndexedColumns(String relation) throws DatalessException {
		return "select COLNAMES, INDNAME from SYSCAT.INDEXES where TABNAME='" + relation + "'";
	}

	@Override
	public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
		String colnames = new String();
		for(int i=0;i<cols.size();i++)
		{
			colnames = colnames+"+"+cols.get(i);
		}
		return "select INDNAME from SYSCAT.INDEXES where TABNAME='" + relation + "' AND COLNAMES='"+colnames+"'";
	}

	@Override
	public String getQuery_getAttributes(String relation) throws DatalessException {
		return "select COLNAME from SYSCAT.COLUMNS where TABNAME='" + relation + "'";
	}

	@Override
	public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
		return "SELECT NAME FROM SYSIBM.SYSCOLUMNS WHERE TBNAME = '" + relation + "' AND TBCREATOR = '" + getSchema() + "' AND KEYSEQ > 0 ORDER BY KEYSEQ ASC ";
	}

	@Override
	public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
		return "select REFTABNAME, FK_COLNAMES from SYSCAT.REFERENCES where TABNAME ='"+relation+"' and TABSCHEMA ='"+getSchema()+"'";
	}

	@Override
	public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
		return "SELECT k.COLNAME FROM SYSCAT.KEYCOLUSE k, SYSCAT.TABCONST c WHERE c.CONSTNAME=k.CONSTNAME AND k.TABSCHEMA='"+getSchema()+"' AND k.TABNAME='"+relation+"' AND c.TYPE='F'";
	}

	@Override
	public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
		return "SELECT k.COLNAME FROM SYSCAT.KEYCOLUSE k, SYSCAT.TABCONST c WHERE c.CONSTNAME=k.CONSTNAME AND k.TABSCHEMA='"+getSchema()+"' AND k.TABNAME='"+relation+"' AND c.TYPE='U'";
	}

	@Override
	public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
		// N - NOT NULL, Y - NULLABLE
		return "SELECT NULLS from SYSCAT.COLUMNS where TABNAME='"+relation+"' AND COLNAME='"+column+"' ";
	}

	@Override
	public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
		return "select REFTABNAME, PK_COLNAMES, FK_COLNAMES from SYSCAT.REFERENCES where TABNAME ='"+relation+"' and TABSCHEMA ='"+getSchema()+"' and FK_COLNAMES like '%"+column+"%'";
	}

	@Override
	public boolean connect(DBSettings settings) throws DatalessException {
		String connectString;
		System.out.println("connect");
		if (isConnected()) {
			return true;
		}
		this.settings = settings;
		try {
			connectString = "jdbc:db2://" + settings.getServerName() + ":" + settings.getServerPort() + "/" + settings.getDbName();
			//System.out.println("jdbc:db2://localhost:50000/TPCH+dsladmin+dslrahlam786");
			System.out.println(connectString + "+" + settings.getUserName() + "+" + settings.getPassword());
			Class.forName("com.ibm.db2.jcc.DB2Driver").newInstance();
			System.out.println("......");
			con = DriverManager.getConnection(connectString, settings.getUserName(), settings.getPassword());
			System.out.println("GotConnection!");
		} catch (Exception e) {
			throw new DatalessException(e.getMessage());
		}
		if (con != null) {
			return true;
		}
		return false;
	}

	@Override
	public boolean retain(String [] dropRelations, String[] dependentRelations) throws DatalessException {

		int len = dropRelations.length + dependentRelations.length;
		String [] FK_table = new String[len];
		String [] FK_column = new String[len];
		String [] PK_table = new String[len];
		String [] PK_column = new String[len];
		int k = 0;

		try{
			Statement stmt = createStatement();
			String command = "set current schema "+this.getSchema();
			stmt.execute(command);
			stmt.close();

			/*The process goes as follows:
			 * Truncate the parent relation.
			 * Drop the forign keys of all the child table.
			 * Truncate the referenced relation
			 * Add the foreign key to the parent relation.
			 */
			for(int b=0;b<dropRelations.length;b++)
			{
				Statement stmt1 = createStatement();
				String com = "select CONSTNAME, TABNAME, REFTABNAME, FK_COLNAMES, PK_COLNAMES from SYSCAT.REFERENCES";
				ResultSet rset= stmt1.executeQuery(com);
				while(rset.next())
				{
					if(rset.getString(2).equals(dropRelations[b]))
					{
						FK_table[k]=rset.getString(2);
						PK_table[k]=rset.getString(3);
						FK_column[k]=rset.getString(4).trim().replaceAll("( )+", ",");
						PK_column[k]=rset.getString(5).trim().replaceAll("( )+", ",");
						k++;
						Statement stmt2 = createStatement();
						String com1 = "ALTER TABLE " + dropRelations[b] + " DROP FOREIGN KEY " + rset.getString(1);
						Constants.CPrintToConsole(com1, Constants.DEBUG_SECOND_LEVEL_Information);
						stmt2.executeUpdate(com1);
					}
				}
				rset.close();
				stmt1.close();
			}
			for(int b=0;b<dependentRelations.length;b++)
			{
				Statement stmt1 = createStatement();
				String com = "select CONSTNAME, TABNAME, REFTABNAME, FK_COLNAMES, PK_COLNAMES from SYSCAT.REFERENCES";
				ResultSet rset= stmt1.executeQuery(com);
				while(rset.next())
				{
					if(rset.getString(2).equals(dependentRelations[b]))
					{
						FK_table[k]=rset.getString(2);
						PK_table[k]=rset.getString(3);
						FK_column[k]=rset.getString(4);
						PK_column[k]=rset.getString(5);
						k++;
						Statement stmt2 = createStatement();
						String com1 = "ALTER TABLE " + dependentRelations[b] + " DROP FOREIGN KEY " + rset.getString(1);
						Constants.CPrintToConsole(com1, Constants.DEBUG_SECOND_LEVEL_Information);
						stmt2.executeUpdate(com1);
					}
				}
				rset.close();
				stmt1.close();
			}

			for(int c=0;c<dropRelations.length;c++)
			{
				Statement stmt1 = createStatement();
				String com = "TRUNCATE TABLE "+dropRelations[c]+" DROP STORAGE IMMEDIATE";
				Constants.CPrintToConsole(com, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt1.execute(com);
				stmt1.close();

			}
			for(int c=0;c<dependentRelations.length;c++)
			{
				Statement stmt1 = createStatement();
				String com = "TRUNCATE TABLE "+dependentRelations[c]+" DROP STORAGE IMMEDIATE";
				Constants.CPrintToConsole(com, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt1.execute(com);
				stmt1.close();
			}

			for(int d=0; d<k; d++)
			{
				Statement stmt1 = createStatement();
				String com = "ALTER TABLE " +FK_table[d]+" ADD FOREIGN KEY("+FK_column[d]+") REFERENCES "+PK_table[d]+"("+PK_column[d]+")";
				Constants.CPrintToConsole(com, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt1.executeUpdate(com);
				stmt1.close();
			}

		}
		catch(Exception e)
		{
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
	public void collectStatistics(String relation) throws DatalessException
	{
		try {
			Statement stmt = createStatement();
			String query = "call ADMIN_CMD ('RUNSTATS ON TABLE " + this.getSchema() + "." + relation + " ON ALL COLUMNS WITH DISTRIBUTION ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
			Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(query);
			stmt.close();
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in collecting Statistics for " + relation + " of " + settings.getDbName());
		}
	}
	
    /**
	 * Collects statistics for the specified relation and column. This function is called before updating column statistics, so that enough buckets are created in the COLDIST table.
	 * @param relation Relation name
	 * @param colStats Column Statistics
	 * @throws DatalessException
	*/
    private void collectStatistics(String relation, HashMap<String,ColumnStatistics> colStats) throws DatalessException
    {

        String query = "call ADMIN_CMD ('RUNSTATS ON TABLE " + this.getSchema() + "." + relation + "  WITH DISTRIBUTION ON COLUMNS (";
        
        for(String column: colStats.keySet()){
    		int fBucketSize = ((DB2ColumnStatistics)colStats.get(column)).getFrequencyHistogram()==null?0:((DB2ColumnStatistics)colStats.get(column)).getFrequencyHistogram().size();
    		int qBucketSize = ((DB2ColumnStatistics)colStats.get(column)).getQuantileHistogram()==null?0:((DB2ColumnStatistics)colStats.get(column)).getQuantileHistogram().size();
    		query += column + " NUM_FREQVALUES " + fBucketSize + " NUM_QUANTILES " + qBucketSize + ",";
        }
        query = query.substring(0, query.length()-1) + ")  AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";

        try {
            Statement stmt = createStatement();
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(query);
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating Statistics for " + relation + " of " + settings.getDbName());
        }
    }

/*
    @Override
    public boolean scale(String relation, int scaleFactor, boolean runStats) throws DatalessException
    {
        boolean retValue = false;
        String[] PKeys;
        String own_name  = this.getSchema();
        try{
             // Runs Statistics to collect information.
            if(runStats)
            {
                this.collectStatistics(relation);
            }

            // Get Primary Keys

            PKeys = this.getAttributes(relation);
            // Update table Statistics
            String sqlTab = "UPDATE SYSSTAT.TABLES SET CARD="+scaleFactor+"*CARD, NPAGES="+scaleFactor+"*NPAGES, FPAGES="+scaleFactor+"*FPAGES, OVERFLOW="+scaleFactor+"*OVERFLOW ,ACTIVE_BLOCKS="+scaleFactor+"*ACTIVE_BLOCKS WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"'";

            String sql1="UPDATE SYSSTAT.COLUMNS SET COLCARD="+scaleFactor+"*COLCARD, NUMNULLS="+scaleFactor+"*NUMNULLS, HIGH2KEY="+scaleFactor+"*HIGH2KEY WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"' AND (";
            String sql2="UPDATE SYSSTAT.COLUMNS SET COLCARD="+scaleFactor+"*COLCARD, NUMNULLS="+scaleFactor+"*NUMNULLS WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"' AND COLNAME NOT IN(";
            String sql3="UPDATE SYSSTAT.COLDIST SET COLVALUE="+scaleFactor+"*COLVALUE, VALCOUNT="+scaleFactor+"*VALCOUNT, DISTCOUNT="+scaleFactor+"*DISTCOUNT WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"' AND TYPE='Q' AND COLVALUE IS NOT NULL AND (";
            String sql4="UPDATE SYSSTAT.COLDIST SET COLVALUE="+scaleFactor+"*COLVALUE WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"' AND TYPE='F' AND COLVALUE IS NOT NULL AND (";
            String sql5="UPDATE SYSSTAT.COLDIST SET VALCOUNT="+scaleFactor+"*VALCOUNT WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"' AND COLVALUE IS NOT NULL AND COLNAME NOT IN(";
            for(int i=0;i<PKeys.length;i++) {
                sql1 = sql1 + "COLNAME='" + PKeys[i] + "' OR ";
                sql2 = sql2 + "'" + PKeys[i] + "' ,";
                sql3 = sql3 + "COLNAME='" + PKeys[i] + "' OR ";
                sql4 = sql4 + "COLNAME='" + PKeys[i] + "' OR ";
                sql5 = sql5 + "'" + PKeys[i] + "' ,";

            }
            sql1 = sql1.substring(0, sql1.length() - 3);
            sql1 = sql1 + ")";

            sql2 = sql2.substring(0, sql2.length() - 1);
            sql2 = sql2 + ")";

            sql3 = sql3.substring(0, sql3.length() - 3);
            sql3 = sql3 + ")";

            sql4 = sql4.substring(0, sql4.length() - 3);
            sql4 = sql4 + ")";

            sql5 = sql5.substring(0, sql5.length() - 1);
            sql5 = sql5 + ")";

            Statement stmt = createStatement();
            Constants.CPrintToConsole(sqlTab, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sqlTab);
            Constants.CPrintToConsole(sql1, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql1);
            Constants.CPrintToConsole(sql2, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql2);
            Constants.CPrintToConsole(sql2, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql3);
            Constants.CPrintToConsole(sql4, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql4);
            Constants.CPrintToConsole(sql5, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql5);
            String sql = "UPDATE SYSSTAT.INDEXES SET NLEAF=" + scaleFactor + "*NLEAF, NLEVELS=" + scaleFactor + "*NLEVELS, NUMRIDS=" + scaleFactor + "*NUMRIDS, INDCARD=" + scaleFactor + "*INDCARD WHERE TABSCHEMA = '"+own_name+"' AND TABNAME = '"+relation+"'";
            Constants.CPrintToConsole(sql, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.executeUpdate(sql);
            stmt.close();
            retValue = true;
        }
        catch(Exception e)
        {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in scaling Relations of "+settings.getDbName());
        }
        return retValue;
    }
    /*
    @Override
    public boolean scale(String[] relation, double scaleFactor, boolean runStats) throws DatalessException {
        boolean retValue = false;
        String StrScaleFactor = new String(""+scaleFactor+"");
        msg = new String();
        for(int i=0;i<relation.length;i++)
        {
            boolean ret = this.ScaleRelation(relation[i], StrScaleFactor, runStats);
            if(ret)
                msg += "Metadata Scaling on realtion "+relation[i]+" is successful.\n";
            else
                msg += "Metadata Scaling on realtion "+relation[i]+" is successful.\n";
            retValue = retValue & ret;

        }
        return retValue;
    }
	 * */

	@Override
	public RelationStatistics getRelationStatistics(String relation) throws DatalessException {

		DB2RelationStatistics relStat = new DB2RelationStatistics(relation, this.getSchema());

		String command = "SELECT CARD, NPAGES, FPAGES, OVERFLOW, ACTIVE_BLOCKS FROM SYSSTAT.TABLES WHERE TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";
		Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
		try
		{
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				BigDecimal card = new BigDecimal(rset.getString(1));
				BigDecimal npages = new BigDecimal(rset.getString(2));
				BigDecimal fpages = new BigDecimal(rset.getString(3));
				BigDecimal overflow = new BigDecimal(rset.getString(4));
				BigDecimal activeBlocks = new BigDecimal(rset.getString(5));
				relStat.setCardinality(card);
				relStat.setNPages(npages);
				relStat.setFPages(fpages);
				relStat.setOverflow(overflow);
				relStat.setActiveBlocks(activeBlocks);
				Constants.CPrintToConsole("card|npages|fpages|overflow|activBlocks", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(card+"|"+npages+"|"+fpages+"|"+overflow+"|"+activeBlocks, Constants.DEBUG_SECOND_LEVEL_Information);
			}
			rset.close();
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Relation "+relation+" Statistics of "+settings.getDbName());
		}
		return relStat;
	}

	@Override
	public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard) throws DatalessException {
		String type = getType(relation,column);
		/**
		 * Column Statistics from SYSSTAT.COLUMNS
		 */
		DB2ColumnStatistics colStat = null;
		try
		{
			colStat = new DB2ColumnStatistics(relation, column, type, getConstraint(relation, column));
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception intialzeing Column Statisitcs "+column+" ("+relation+")  of "+settings.getDbName());
		}
		try
		{
			String command = "SELECT COLCARD, NUMNULLS, SUB_COUNT, SUB_DELIM_LENGTH, HIGH2KEY, LOW2KEY, AVGCOLLEN FROM SYSSTAT.COLUMNS "
					+ " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				BigDecimal col_card = new BigDecimal(rset.getString(1));
				BigDecimal null_cnt = new BigDecimal(rset.getString(2));
				Integer sub_count = new Integer(rset.getInt(3));
				Integer sub_delim_len = new Integer(rset.getInt(4));
				String high2key = rset.getString(5);
				String low2key = rset.getString(6);
				Integer avg_col_len = new Integer(rset.getInt(7));

				colStat.setColCard(col_card);
				colStat.setNumNulls(null_cnt);
				colStat.setSubCount(sub_count);
				colStat.setSubDelimLength(sub_delim_len);
				colStat.setAvgColLen(avg_col_len);
				colStat.setHigh2key(high2key);
				colStat.setLow2key(low2key);

				Constants.CPrintToConsole("col_card|null_cnt|sub_count|sub_delim_len|high2key|low2key|avg_col_len", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(col_card + "|" + null_cnt + "|" + sub_count + "|" + sub_delim_len + "|" + high2key + "|" + low2key + "|" + avg_col_len, Constants.DEBUG_SECOND_LEVEL_Information);
			}
			rset.close();
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Column "+column+" ("+relation+") from SYSSTAT.COLUMNS of "+settings.getDbName());
		}

		/**
		 * Column Statistics from SYSSTAT.COLDIST 'Quantile Histogram'
		 *
		 * has to store 4 values. SEQNO, COLVALUE, VALCOUNT, DISTCOUNT
		 * (Sequence Number, Column Value, Frequency, Distinct Values Count [less than or equal COLVALUE])
		 */
		try
		{
			String command = "SELECT SEQNO, COLVALUE, VALCOUNT, DISTCOUNT FROM SYSSTAT.COLDIST "
					+ " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema()
					+ "' AND TYPE = 'Q' ORDER BY SEQNO";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);

			Integer prevValue = 0;
			Integer prevDistCount = 0;
			TreeMap<Integer, HistogramObject> map = new TreeMap<Integer,HistogramObject>();
			Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT|distCount", Constants.DEBUG_SECOND_LEVEL_Information);
			int count = 0;
			while (rset.next()) {
				Integer seqno = new Integer(rset.getInt(1));
				Object colvalue = rset.getObject(2);
				String col = null;
				Integer valCount = new Integer(rset.getInt(3));
				Integer valCountThisBucket = -1;
				if(valCount != -1)
				{
					valCountThisBucket= valCount - prevValue;
				}
				prevValue = valCount;
				Object distCountObj = rset.getObject(4);
				Integer distCount;
				Integer distCountThisBucket = null;
				if(distCountObj != null)
				{
					distCount = new Integer(Integer.parseInt(distCountObj.toString()));
					distCountThisBucket = new Integer(distCount - prevDistCount);
					prevDistCount = distCount;
				}
				else
					distCount = null;

				if (colvalue != null) {
					count++;
					col = colvalue.toString();
				}
				else
					col = null;
				Constants.CPrintToConsole(rset.getInt(1) + "|" + col + "|" + valCount + "| " + distCount, Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(rset.getInt(1) + "|" + col + "|" + valCountThisBucket + "| " + distCountThisBucket, Constants.DEBUG_SECOND_LEVEL_Information);
				// Store as Double value. For DB2, the value must be integer, other engines, it could be double too.
				HistogramObject histogramObject;
				if(distCountThisBucket != null) {
					histogramObject = new HistogramObject(col,valCountThisBucket.doubleValue(),distCountThisBucket.doubleValue());
				} else {
					histogramObject = new HistogramObject(col,valCountThisBucket.doubleValue(),null);
				}
				map.put(seqno, histogramObject);
			}
			rset.close();
			stmt.close();
			if(count == 0) //Histogram was not collected.
				map = null;
			colStat.setQuantileHistogram(map);
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Qunatile Histogram of Column "+column+" ("+relation+") from SYSSTAT.COLDIST of "+settings.getDbName());
		}


		/**
		 * Column Statistics from SYSSTAT.COLDIST 'Frequency Histogram'
		 *
		 * has to store 3 values. SEQNO, COLVALUE, VALCOUNT
		 * (Sequence Number, Column Value, Frequency, [First n high frequent colValues])
		 */
		try
		{
			String command = "SELECT SEQNO, COLVALUE, VALCOUNT FROM SYSSTAT.COLDIST "
					+ " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema()
					+ "' AND TYPE = 'F' ORDER BY SEQNO";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);

			TreeMap<Integer, DB2FreqHistObject> map = new TreeMap<Integer,DB2FreqHistObject>();
			Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT", Constants.DEBUG_SECOND_LEVEL_Information);
			int count = 0;
			while (rset.next()) {
				Integer seqno = new Integer(rset.getInt(1));
				Object colvalue = rset.getObject(2);
				String col = null;
				BigDecimal valCount = new BigDecimal(rset.getString(3));

				if (colvalue != null) {
					count++;
					col = colvalue.toString();
				}
				else
					col = null;
				Constants.CPrintToConsole(rset.getInt(1) + "|" + col + "|" + valCount, Constants.DEBUG_SECOND_LEVEL_Information);
				DB2FreqHistObject freqHistObject = new DB2FreqHistObject(col,valCount);
				map.put(seqno, freqHistObject);
			}
			rset.close();
			stmt.close();
			if(count == 0) //Histogram was not collected.
				map = null;
			colStat.setFrequencyHistogram(map);
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Frequncy Histogram of Column "+column+" ("+relation+") from SYSSTAT.COLDIST of "+settings.getDbName());
		}

		return colStat;
	}

	@Override
	public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {

		DB2IndexStatistics indexStat = new DB2IndexStatistics(relation, colNames);
		String indexName = getIndexName(relation, colNames);
		// indexName shouldn't be null
		String command = "SELECT INDCARD, NLEAF, NLEVELS, DENSITY, NUMRIDS, CLUSTERFACTOR, NUM_EMPTY_LEAFS  FROM SYSSTAT.INDEXES "+
				" WHERE INDNAME = '"+indexName+"'AND TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"'";
		Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);

		try
		{
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				BigDecimal INDCARD = new BigDecimal(rset.getString(1));
				BigDecimal NLEAF = new BigDecimal(rset.getInt(2));
				BigDecimal NLEVELS = new BigDecimal(rset.getInt(3));
				Double DENSITY = rset.getDouble(4);
				BigDecimal NUMRIDS = new BigDecimal(rset.getString(5));
				Double CLUSTERFACTOR = rset.getDouble(6);
				BigDecimal NUM_EMPTY_LEAFS = new BigDecimal(rset.getString(7));

				indexStat.setIndCard(INDCARD);
				indexStat.setnLeaf(NLEAF);
				indexStat.setnLevels(NLEVELS);
				indexStat.setDensity(DENSITY);
				indexStat.setNumRIDs(NUMRIDS);
				indexStat.setClusterFactor(CLUSTERFACTOR);
				indexStat.setNumEmptyLeafs(NUM_EMPTY_LEAFS);

				Constants.CPrintToConsole("INDCARD|NLEAF|NLEVELS|DENSITY|NUMRIDS|CLUSTERFACTOR|NUM_EMPTY_LEAFS", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(INDCARD+"|"+ NLEAF+"|"+ NLEVELS+"|"+ DENSITY+"|"+ NUMRIDS+"|"+ CLUSTERFACTOR+"|"+ NUM_EMPTY_LEAFS, Constants.DEBUG_SECOND_LEVEL_Information);

			}
			rset.close();
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading index Statistics "+indexName+" of "+relation+" Statistics of "+settings.getDbName());
		}
		return indexStat;
	}

	@Override
	public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException {
		// Instead of this method, added another method with following signature:
		// setRelationStatistics(String relation, RelationStatistics relationStatistics, HashMap<String,ColumnStatistics> colStats) throws DatalessException {
		return false;
	}

	public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics, HashMap<String,ColumnStatistics> colStats) throws DatalessException {
		DB2RelationStatistics relStat = new DB2RelationStatistics(relation, relationStatistics.getSchema());
		relStat.mapForPorting(relationStatistics);
		try{
			// Initialize Statistics.
			collectStatistics(relation, colStats);
			
			String command1 = "UPDATE SYSSTAT.INDEXES SET NLEAF=-1, NLEVELS=-1, FIRSTKEYCARD=-1, FIRST2KEYCARD=-1, FIRST3KEYCARD=-1,"+
					"FIRST4KEYCARD=-1, FULLKEYCARD=-1, CLUSTERFACTOR=-1, CLUSTERRATIO=-1, SEQUENTIAL_PAGES=-1, PAGE_FETCH_PAIRS='',"+
					"DENSITY=-1, AVERAGE_SEQUENCE_GAP=-1, AVERAGE_SEQUENCE_FETCH_GAP=-1, AVERAGE_SEQUENCE_PAGES=-1,"+
					"AVERAGE_SEQUENCE_FETCH_PAGES=-1, AVERAGE_RANDOM_PAGES=-1, AVERAGE_RANDOM_FETCH_PAGES=-1, NUMRIDS=-1,"+
					"NUMRIDS_DELETED=-1, NUM_EMPTY_LEAFS=-1 WHERE TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"'";
			String command2 = "UPDATE SYSSTAT.COLUMNS SET COLCARD=-1, NUMNULLS=-1 WHERE TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"'";
			String command = "UPDATE SYSSTAT.TABLES SET CARD=" +relStat.getCardinality()+", NPAGES="+relStat.getNPages()+", FPAGES="+relStat.getFPages()+", OVERFLOW="+relStat.getOverflow()+", ACTIVE_BLOCKS="+relStat.getActiveBlocks()+" WHERE TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"'";

			Statement stmt = createStatement();
			Constants.CPrintToConsole(command1, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command1);
			Constants.CPrintToConsole(command2, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command2);
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command);
			stmt.close();
		} catch (Exception e)	{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Relation "+relation+" Statistics of "+settings.getDbName());
		}
		return true;
	}

	/**
	 * Updating statistics has to mode.
	 * 1) Given columnStatistics is of DB2Type (which is identified by the variable sameDBType)
	 *      Transfer of statistics with same DB Engine
	 *      Update details from child class.
	 * 2) Given columnStatistics is not of DB2Type.
	 *      Inter Engine transfer
	 *      Update details from parent class.

	 * @param relation Relation name
	 * @param column Column name
	 * @param columnStatistics ColumnStatistics
	 * @return true on success.
	 * @throws DatalessException
	 */
	@Override
	public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException {

		/**
		 * Column Statistics from SYSSTAT.COLUMNS
		 *
		 */
		// boolean sameDBType = false;
		String type = getType(relation, column);
		DB2ColumnStatistics colStat = new DB2ColumnStatistics(relation, column, type, getConstraint(relation, column));
		colStat.mapForPorting(columnStatistics, tableCard);
		/*
        if(columnStatistics instanceof DB2ColumnStatistics)
        {
            colStat = (DB2ColumnStatistics)columnStatistics;
            sameDBType = true;
        }
		 */
		try
		{
			String command;
			//if(sameDBType)
			{
				command = "UPDATE SYSSTAT.COLUMNS SET"
						+ " COLCARD=" + colStat.getColCard() + ","
						+ " NUMNULLS=" + colStat.getNumNulls() + ","
						+ " SUB_COUNT=" + colStat.getSubCount() + ","
						+ " SUB_DELIM_LENGTH=" + colStat.getSubDelimLength() + ",";

				if (DataType.isString(colStat.getColumnType())) {
					String high2key = colStat.getHigh2key();
					if(high2key.startsWith("'")) {
						high2key = high2key.substring(1,high2key.length()-1);
					}
					if (high2key.length() > 33) {
						high2key = high2key.substring(0, 33);
					}
					String low2key = colStat.getLow2key();
					if(low2key.startsWith("'")) {
						low2key = low2key.substring(1,low2key.length()-1);
					}
					if (low2key.length() > 33) {
						low2key = low2key.substring(0, 33);
					}
					command = command + " HIGH2KEY='" + high2key + "'," + " LOW2KEY='" + low2key + "',";

				} else if (DataType.isDate(colStat.getColumnType())) {
					String high2key = colStat.getHigh2key();
					if(high2key.startsWith("'")) {
						high2key = high2key.substring(1,high2key.length()-1);
					}
					String low2key = colStat.getLow2key();
					if(low2key.startsWith("'")) {
						low2key = low2key.substring(1,low2key.length()-1);
					}
					if(!high2key.isEmpty() && !low2key.isEmpty()) {
						command = command + " HIGH2KEY=DATE('" + high2key + "')," + " LOW2KEY=DATE('" + low2key + "'),";
					} else {
						command = command + " HIGH2KEY=''," + " LOW2KEY='',";
					}

				} else if (DataType.isTime(colStat.getColumnType())) {
					String high2key = colStat.getHigh2key();
					if (high2key.startsWith("'")) {
						high2key = high2key.substring(1, high2key.length() - 1);
					}
					String low2key = colStat.getLow2key();
					if (low2key.startsWith("'")) {
						low2key = low2key.substring(1, low2key.length() - 1);
					}
					if(!high2key.isEmpty() && !low2key.isEmpty()) {
						command = command + " HIGH2KEY=TIME('" + high2key + "')," + " LOW2KEY=TIME('" + low2key + "'),";
					} else {
						command = command + " HIGH2KEY=''," + " LOW2KEY='',";
					}
				} else {
					if(!colStat.getHigh2key().isEmpty()) {
						command = command + " HIGH2KEY=" + colStat.getHigh2key()+ ",";
					} else {
						command = command + " HIGH2KEY=''"+ ",";
					}
					if(!colStat.getLow2key().isEmpty()) {
						command = command + " LOW2KEY=" + colStat.getLow2key() + ",";
					} else {
						command = command + " LOW2KEY=''" + ",";
					}
				}
				command = command + " AVGCOLLEN=" + colStat.getAvgColLen()
						+ " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";
			}
			/*
            else
            {
                command = "UPDATE SYSSTAT.COLUMNS SET"
                        + " COLCARD=" + columnStatistics.getColCard() + ","
                        + " HIGH2KEY='',"
                        + " LOW2KEY='',"
                        + " NUMNULLS=" + columnStatistics.getNumNulls()
                        + " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";
            }
			 *
			 */
			Statement stmt = createStatement();
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command);
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Column "+column+" ("+relation+") to SYSSTAT.COLUMNS of "+settings.getDbName());
		}

		/**
		 * Column Statistics from SYSSTAT.COLDIST 'Quantile Histogram'
		 *
		 * has to update 4 values. SEQNO, COLVALUE, VALCOUNT, DISTCOUNT
		 * (Sequence Number, Column Value, Frequency, Distinct Values Count [less than or equal COLVALUE])
		 * Sorted based on COLVALUE. SEQNO is assigned based on COLVALUE.
		 */
		try
		{
			TreeMap<Integer, HistogramObject> mapQuantile;

			/*
            if (sameDBType)
            {
                // Adjust Histogram For Everybody
                colStat.adjustHistogram(tableCard, DB2ColumnStatistics.DefaultQuantileBucketSize);
                mapQuantile = (TreeMap) colStat.getQuantileHistogram();
            } else {
                // Adjust Histogram For Everybody
                columnStatistics.adjustHistogram(tableCard, DB2ColumnStatistics.DefaultQuantileBucketSize);
                mapQuantile = (TreeMap) columnStatistics.getHistogram();
            }
			 */
			// adjustment is done in the mapping function itself
			mapQuantile = (TreeMap<Integer,HistogramObject>) colStat.getQuantileHistogram();

			if(mapQuantile != null)
			{
				BigDecimal prevValueCount = new BigDecimal(BigDecimal.ZERO+"");
				BigDecimal prevDistCount = new BigDecimal(BigDecimal.ZERO+"");

				Set<Entry<Integer, HistogramObject>> set = mapQuantile.entrySet();
				Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
				while(i.hasNext()) {
					Map.Entry<Integer, HistogramObject> me = i.next();
					Integer seqno = (Integer) me.getKey();
					HistogramObject histogramObject = (HistogramObject) me.getValue();
					String colValue = histogramObject.getColValue();
					if(colValue == null) {
						break; //No more values
					}
					// Read the intValue. For DB2, the value has to be integer
					BigDecimal bigD = new BigDecimal(histogramObject.getValCount());
					bigD = bigD.add(prevValueCount);
					BigDecimal valCount =  new BigDecimal(bigD+"");
					BigDecimal distCount = null;

					if(histogramObject.getDistCount() != null) {
						BigDecimal bigD1 = new BigDecimal(histogramObject.getDistCount());
						distCount = new BigDecimal(bigD1+"");
					}
					if(distCount != null && prevDistCount != null ) //If DISTCOUNT is present
					{
						BigDecimal temp1 = new BigDecimal(distCount+"");
						temp1 = temp1.add(prevDistCount);
						distCount = new BigDecimal(temp1+"");
					}
					String command = "UPDATE SYSSTAT.COLDIST SET ";
					if (DataType.isString(colStat.getColumnType())) {
						if (colValue.startsWith("'")) {
							colValue = colValue.substring(1, colValue.length() - 1);
						}
						if (colValue.length() > 33) {
							colValue = colValue.substring(0, 33);
						}
						command = command + "COLVALUE='" + colValue + "'";
					} else if (DataType.isDate(colStat.getColumnType())) {
						if (colValue.startsWith("'")) {
							colValue = colValue.substring(1, colValue.length() - 1);
						}
						command = command + "COLVALUE=DATE('" + colValue + "')";
					} else if (DataType.isTime(colStat.getColumnType())) {
						if (colValue.startsWith("'")) {
							colValue = colValue.substring(1, colValue.length() - 1);
						}
						command = command + "COLVALUE=TIME('" + colValue + "')";
					} else {
						command = command + "COLVALUE=" + colValue + "";
					}
					command = command + ", VALCOUNT="+valCount;
					if(distCount != null)
						command = command + ", DISTCOUNT="+distCount;
					command = command +" WHERE COLNAME = '"+column+"' AND TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+
							"' AND TYPE = 'Q' AND SEQNO ="+(seqno);

					Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
					Statement stmt = createStatement();
					stmt.executeUpdate(command);
					stmt.close();
					prevValueCount = valCount;
					prevDistCount = distCount;
				}
			}
			else
			{
				String command = "UPDATE SYSSTAT.COLDIST SET COLVALUE=null, VALCOUNT=-1, DISTCOUNT=null WHERE COLNAME = '"+column+
						"' AND TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"' AND TYPE = 'Q'";
				Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				stmt.executeUpdate(command);
				stmt.close();
			}
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Qunatile Histogram of Column "+column+" ("+relation+") to SYSSTAT.COLDIST of "+settings.getDbName());
		}


		/**
		 * Column Statistics from SYSSTAT.COLDIST 'Frequency Histogram'
		 *
		 * has to update 3 values. SEQNO, COLVALUE, VALCOUNT
		 * (Sequence Number, Column Value, Frequency, [First n high frequent colValues])
		 * Sorted based on VALCOUNT. SEQNO is assigned based on sorted VALCOUNT.
		 */
		try
		{
			//if(sameDBType)
			{
				TreeMap<Integer, DB2FreqHistObject> mapFrequency = (TreeMap<Integer, DB2FreqHistObject>) colStat.getFrequencyHistogram();
				if (mapFrequency != null) {
					Set<Entry<Integer, DB2FreqHistObject>> set = mapFrequency.entrySet();
					Iterator<Entry<Integer, DB2FreqHistObject>> i = set.iterator();
					while (i.hasNext()) {
						Map.Entry<Integer, DB2FreqHistObject> me = i.next();

						// <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
						Integer seqno = (Integer) me.getKey();
						DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
						String colValue = freqHistObject.getColValue();
						if(colValue == null) {
							break; //No more values
						}
						BigDecimal valCount = freqHistObject.getValCount();
						String command = "UPDATE SYSSTAT.COLDIST SET ";
						if (DataType.isString(colStat.getColumnType())) {

							if (colValue.startsWith("'")) {
								colValue = colValue.substring(1, colValue.length() - 1);
							}
							if(colValue.length() > 33) {
								colValue = colValue.substring(0,33);
							}
							command = command + "COLVALUE='" + colValue + "'";
						} else if (DataType.isDate(colStat.getColumnType())) {
							if (colValue.startsWith("'")) {
								colValue = colValue.substring(1, colValue.length() - 1);
							}
							command = command + "COLVALUE=DATE('" + colValue + "')";
						} else if (DataType.isTime(colStat.getColumnType())) {
							if (colValue.startsWith("'")) {
								colValue = colValue.substring(1, colValue.length() - 1);
							}
							command = command + "COLVALUE=TIME('" + colValue + "')";
						} else {
							command = command + "COLVALUE=" + colValue + "";
						}
						command = command + ", VALCOUNT=" + valCount
								+ " WHERE COLNAME = '" + column + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema()
								+ "' AND TYPE = 'F' AND SEQNO =" + (seqno);
						Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
						Statement stmt = createStatement();
						stmt.executeUpdate(command);
						stmt.close();
					}
				}
				else {
					String command = "UPDATE SYSSTAT.COLDIST SET COLVALUE=null, VALCOUNT=-1, DISTCOUNT=null WHERE COLNAME = '" + column
							+ "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "' AND TYPE = 'F'";
					Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
					Statement stmt = createStatement();
					stmt.executeUpdate(command);
					stmt.close();
				}
			}
			/*
            else {
                String command = "UPDATE SYSSTAT.COLDIST SET COLVALUE=null, VALCOUNT=-1, DISTCOUNT=null WHERE COLNAME = '" + column
                        + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "' AND TYPE = 'F'";
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                stmt.executeUpdate(command);
                stmt.close();
            }
			 */
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Frequency Histogram of Column "+column+" ("+relation+") to SYSSTAT.COLDIST of "+settings.getDbName());
		}
		return true;
	}

	@Override
	public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException {
		// boolean sameDBType = false;
		DB2IndexStatistics indexStat = new DB2IndexStatistics(relation, colNames);
		indexStat.mapForPorting(indexStatistics);
		String indexName = getIndexName(relation, colNames);
		if(indexName == null){ 
			Constants.CPrintToConsole("Index doesn't exist for relation.", Constants.DEBUG_SECOND_LEVEL_Information);
			return true;
		}
		/*
        if(indexStatistics instanceof DB2IndexStatistics)
        {
            indexStat = (DB2IndexStatistics)indexStatistics;
            sameDBType = true;
        }
		 */
		try {
			//if(sameDBType)
			{

				Statement stmt = createStatement();
				/*
                String command = "UPDATE SYSSTAT.INDEXES SET NLEAF=" + indexStat.getnLeaf() + ",NLEVELS=" + indexStat.getnLevels()
                        + ",CLUSTERFACTOR=" + indexStat.getClusterFactor() + ",CLUSTERRATIO=" + indexStat.getClusterRatio()
                        + ",DENSITY=" + indexStat.getDensity() + ",NUMRIDS=" + indexStat.getNumRIDs() + ",NUMRIDS_DELETED=" + indexStat.getNumRIDsDeleted() + ","
                        + "NUM_EMPTY_LEAFS=" + indexStat.getNumEmptyLeafs() + ",INDCARD=" + indexStat.getIndCard()
                        + " WHERE INDNAME = '" + indexName + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";
				 *
				 */
				String command = "UPDATE SYSSTAT.INDEXES SET NLEAF=" + indexStat.getnLeaf() + ",NLEVELS=" + indexStat.getnLevels()
						+ ",CLUSTERFACTOR=-1,CLUSTERRATIO=-1,PAGE_FETCH_PAIRS=''"
						+ ",DENSITY=" + indexStat.getDensity() + ",NUMRIDS=" + indexStat.getNumRIDs() + ",NUMRIDS_DELETED=" + indexStat.getNumRIDsDeleted() + ","
						+ "NUM_EMPTY_LEAFS=" + indexStat.getNumEmptyLeafs() + ",INDCARD=" + indexStat.getIndCard()
						+ " WHERE INDNAME = '" + indexName + "' AND TABNAME = '" + relation + "' AND TABSCHEMA = '" + this.getSchema() + "'";

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

	private String removeAliases(String string) {
		return string.replaceAll("\\+Q[\\d]+\\.", ",").trim();
	}

	@Override
	public Plan getPlan(String query) throws DatalessException {
		Plan plan;
		ResultSet rset;
		Statement stmt;
		emptyPlanTable();
		explainQuery(query, ++qno);
		plan = new Plan();

		/*
		 * explain_predicate.how_applied in some cases has 'START' and 'STOP' for which DB2 duplicates
		 * entries. But we don't want this to affect our joins to produce spurious tuples. So we explicitly
		 * avoids tuples in explain_predicate with 'STOP' (arbitrarily) value in how_applied attribute
		 */
		// Modfied to get cpu_cost insted total_cost.
		/*
        String planQuery =
                "select source_id, target_id, operator_type, object_name, explain_operator.cpu_cost, stream_count, argument_type, argument_value "
                + "from " + settings.getSchema() + ".explain_statement, " + settings.getSchema() + ".explain_stream LEFT OUTER JOIN " + settings.getSchema() + ".explain_operator "
                + "ON (source_id = explain_operator.operator_id or (source_id=-1 and source_id = explain_operator.operator_id)) and "
                + "explain_operator.explain_time = explain_stream.explain_time "
                + "LEFT OUTER JOIN " + settings.getSchema() + ".explain_argument "
                + "ON  explain_operator.operator_id = explain_argument.operator_id and explain_stream.explain_time = explain_argument.explain_time "
                + "where explain_stream.explain_time = explain_statement.explain_time and explain_statement.explain_level='P' and queryno="
                + qno + " order by target_id, source_id, argument_type asc, argument_value asc";
		 *
		 */
		// With Output list.
		String planQuery =
				"select source_id, target_id, operator_type, object_name, explain_operator.cpu_cost, stream_count, argument_type, argument_value, column_names, predicate_text, queryno "
						+ "from " + settings.getSchema() + ".explain_statement, " + settings.getSchema() + ".explain_stream LEFT OUTER JOIN " + settings.getSchema() + ".explain_operator "
						+ "ON (source_id = explain_operator.operator_id or (source_id=-1 and source_id = explain_operator.operator_id)) and "
						+ "explain_operator.explain_time = explain_stream.explain_time "
						+ "LEFT OUTER JOIN " + settings.getSchema() + ".explain_argument "
						+ "ON  explain_operator.operator_id = explain_argument.operator_id and explain_stream.explain_time = explain_argument.explain_time "
						+ "left outer join "+settings.getSchema()+".explain_predicate  on  explain_predicate.operator_id = explain_argument.operator_id  and "
						+ "explain_predicate.explain_time=explain_argument.explain_time "
						+ "where explain_stream.explain_time = explain_statement.explain_time and explain_statement.explain_level='P' and queryno="
						+ qno + " and (explain_predicate.how_applied is null or explain_predicate.how_applied not like 'STOP') order by target_id, source_id, argument_type asc, argument_value asc";

		// explain_statement info
		try {
			// getting information from explain_statement table to get plan
			// tree information
			Node node = null;
			stmt = createStatement();
			Constants.CPrintToConsole(planQuery, Constants.DEBUG_SECOND_LEVEL_Information);
			rset = stmt.executeQuery(planQuery);
			int curNode = 0;
			int id, indexId;
			String argType, argValue;

			/*
			 * Warning: Update from planQuery
			 * The following is the ordering of information accessed from DB2 explain tables
			 * 1: Id
			 * 2: Parent Id
			 * 3: Node Type
			 * 4: Node Name
			 * 5: Cost (CPU Cost)
			 * 6: Cardinality
			 * 7: Argument type ( this spans across multiple tuples for the same node )
			 * 8: Argument value ( this spans across multiple tuples for the same node )
			 * 9: Column Names
			 * 10 Predicate Text
			 */
			while (rset.next()) {
				/*
				 * By using the above query, we lose the first node which is the 'RETURN' with same cost
				 * and cardinality as that of the second node. So we just insert it in Plan manually
				 * as a special case when curNode == 0
				 */
				if (curNode == 0) {
					node = new Node();
					node.setId(NODE_OFFSET + 1);
					node.setParentId(-1);
					node.setName("RETURN");
					double cost = rset.getDouble(5)/1000000 ;// in millons of operations
					node.setCost(cost);
					node.setCard(rset.getDouble(6));
					//node.setPredicate(rset.getString(8));
					//node.setSelectivity(rset.getDouble(7));
					if (rset.getString(9) != null) {
						node.addArgType("Column Names");
						String removedAliases = removeAliases(rset.getString(9));
						String opList = removedAliases.substring(1); // removing the first comma
						node.addArgValue(opList);

					}
					if (rset.getString(10) != null) {
						node.addArgType("Predicates List");
						node.addArgValue(rset.getString(10));
					}
					plan.setNode(node, curNode);
					curNode++;
				}

				/*We have to Remove the node below Fetch with table name*/
				if (plan.getNodeById(rset.getInt(2) + NODE_OFFSET).getName().equals("FETCH") && rset.getString(3) == null) {
					continue;
				}
				/*Adding node to the Tree */
				id = rset.getInt(1);
				if (id != -1) {
					id += NODE_OFFSET;
					/*if not already present add it to the plan else it is a suboperator*/
					if (!plan.isIdPresent(id)) {
						node = new Node();
						node.setId(id);
						node.setParentId(rset.getInt(2) + NODE_OFFSET);
						node.setName(rset.getString(3));
						node.setCost(rset.getDouble(5)/1000000);// in millons of operations
						node.setCard(rset.getDouble(6));
						plan.setNode(node, curNode);
						if(rset.getString(9)!=null)
						{
							node.addArgType("Column Names");
							String removedAliases = removeAliases(rset.getString(9));
							String opList = removedAliases.substring(1); // removing the first comma
							node.addArgValue(opList);
						}
						if(rset.getString(10)!=null)
						{
							node.addArgType("Predicates List");
							node.addArgValue(rset.getString(10));
						}
						curNode++;
					}
					argType = rset.getString(7);
					argValue = rset.getString(8);
					if(rset.getString(10)!=null )
					{
						int index=(node.getArgType()).indexOf("Predicates List");
						if(index>=0)
						{
							String predicateList=(String)(node.getArgValue()).get(index);
							String temp=rset.getString(10);
							if(!predicateList.contains(temp))
							{
								node.getArgValue().set(index,predicateList+","+ temp);
							}

						}
					}
					if (argType == null || argValue == null) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("NUMROWS")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("BITFLTR")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("FETCHMAX")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("ISCANMAX")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("MAXPAGES")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("MAXRIDS")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("SPILLED")) {
						continue;
					}
					if (argType.trim().toUpperCase().equals("HASHTBSZ")) {
						continue;
					}
					if (!node.isArgTypePresent(argType)) {
						node.addArgType(argType);
						node.addArgValue(argValue);
					}
					continue;
				}
				/*
				 *Adding leaf nodes having id = -1
				 */
				node = new Node();
				node.setId(id);
				node.setParentId(rset.getInt(2) + NODE_OFFSET);
				if (id == -1) {
					node.setName(rset.getString(4));
				} else {
					node.setName(rset.getString(3));
				}
				node.setCost(rset.getDouble(5)/1000000); // in millons of operations
				node.setCard(rset.getDouble(6));
				plan.setNode(node, curNode);
				curNode++;
			}
			rset.close();
			stmt.close();

			/*
			 *Adding relation names below indexnames
			 */
			int plansize = plan.getSize();
			indexId = INDEX_NODE_OFFSET;
			for (int lp = 0; lp < plansize; lp++) {
				Node curnode = plan.getNode(lp);
				if (curnode.getParentId() != -1) {
					String nodename = plan.getNodeById(curnode.getParentId()).getName();
					if (nodename.equals("IXSCAN")
							|| nodename.equals("FIXSCAN")
							|| nodename.equals("SIXSCAN")
							|| nodename.equals("EISCAN")) {
						String indexStr = "select TABNAME from SYSCAT.INDEXES where indname='" + curnode.getName() + "'";
						Statement stmt1 = createStatement();
						ResultSet rset1 = stmt1.executeQuery(indexStr);
						if (rset1.next()) {
							curnode.setId(indexId);
							Node IDXNODE = plan.getNodeById(curnode.getParentId());
							curnode.setCard(plan.getNodeById(IDXNODE.getParentId()).getCard());
							IDXNODE.setCard(plan.getNodeById(IDXNODE.getParentId()).getCard());
							node = new Node();
							node.setParentId(indexId++);
							node.setId(-1);

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
				//Added so that column names of the indexes are made visible.
				String curNodeName = curnode.getName();
				if (curNodeName.matches("SQL\\d*")) {
					String indexStr = "select COLNAMES from SYSCAT.INDEXES where indname='" + curnode.getName() + "'";
					Statement stmt1 = createStatement();
					ResultSet rset1 = stmt1.executeQuery(indexStr);
					if (rset1.next()) {

						curnode.addArgType("Index Columns: ");
						curnode.addArgValue((rset1.getString(1)));

					}
					rset1.close();
					stmt1.close();
				}
			}
		} catch (SQLException e) {
			Constants.CPrintToConsole(planQuery, Constants.DEBUG_SECOND_LEVEL_Information);
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Database: Error explaining query: " + e);
		}
		return plan;
	}

	private void explainQuery(String query, int qno) throws DatalessException {
		try	{
			Statement stmt = createStatement();
			// String x = "explain plan set queryno=30000000 for select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) from ( select YEAR(o_orderdate) as o_year, l_extendedprice * (1 - l_discount) as volume, n2.n_name as nation from DSLADMIN.part, DSLADMIN.supplier, DSLADMIN.lineitem, DSLADMIN.orders, DSLADMIN.customer, DSLADMIN.nation n1, DSLADMIN.nation n2, DSLADMIN.region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AMERICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY ANODIZED STEEL' and s_acctbal  <= 4654.645 and l_extendedprice  <= 36615.4793485745 ) as all_nations group by o_year order by o_year";
			String explainQuery = "explain plan set queryno=" + qno + " for " + query;
			Constants.CPrintToConsole(explainQuery, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(explainQuery);
			//stmt.executeUpdate(x);
			// stmt.executeUpdate("EXPLAIN PLAN SET QUERYNO = 13 FOR SELECT * FROM NATION");
			stmt.close();
		}catch(SQLException e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Database: Error explaining query: "+e);
		}
	}

	public void emptyPlanTable() throws DatalessException {
		qno = 0;
		try{
			Statement stmt = createStatement();
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_predicate");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_argument");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_instance");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_operator");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_object");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_statement");
			stmt.executeUpdate("delete from "+settings.getSchema()+".explain_stream");
			stmt.execute("commit");
			stmt.close();
		}catch(SQLException e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Database: Error emptying plan table: "+e);
		}
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

	public int getDefaultDegree(){
		int defaultDegree = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBCFG where NAME='dft_degree'";
			ResultSet rs = stmt.executeQuery(command);

			if(rs.next()){
				defaultDegree = rs.getInt(1);
			}
			stmt.close();
			
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return defaultDegree;
	}

	public int getAverageNumOfApps(){
		int avgNumApps = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBCFG where NAME='avg_appls'";
			ResultSet rs = stmt.executeQuery(command);

			if(rs.next()){
				avgNumApps = rs.getInt(1);
			}
			stmt.close();
			
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return avgNumApps;
	}

	public int getSortHeapSize(){
		int sortHeapSize = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBCFG where NAME='sortheap'";
			ResultSet rs = stmt.executeQuery(command);

			if(rs.next()){
				sortHeapSize = rs.getInt(1);
			}
			stmt.close();
			
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return sortHeapSize;
	}

	public int getStatementHeapSize(){
		int stmtHeapSize = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBCFG where NAME='stmtheap'";
			ResultSet rs = stmt.executeQuery(command);

			if(rs.next()){
				stmtHeapSize = rs.getInt(1);
			}
			stmt.close();
			
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return stmtHeapSize;
	}

	public int getApplicationHeapSize(){
		int appHeapSize = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBCFG where NAME='applheapsz'";
			ResultSet rs = stmt.executeQuery(command);

			if(rs.next()){
				appHeapSize = rs.getInt(1);
			}
			stmt.close();
			
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return appHeapSize;
	}

	public double getCPUSpeed(){
		double cpuSpeed = 1;
		try{
			Statement stmt = createStatement();
			String command = "SELECT VALUE FROM SYSIBMADM.DBMCFG where NAME='cpuspeed'";
			ResultSet rs = stmt.executeQuery(command);
			if(rs.next()){
				cpuSpeed = rs.getDouble(1);
			}
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
		return cpuSpeed;
	}

	public void setDefaultDegree(int defaultDegree){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update dbm cfg using intra_parallel yes')";
			stmt.execute(command);
			command = "call ADMIN_CMD ('update db cfg using dft_degree " + defaultDegree + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	public void setAverageNumOfApps(int avgNumApps){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update db cfg using avg_appls " + avgNumApps + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	public void setSortHeapSize(int sortHeapSize){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update db cfg using sheapthres_shr 1000')";
			stmt.execute(command);
			command = "call ADMIN_CMD ('update db cfg using sortheap " + sortHeapSize + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	public void setStatementHeapSize(int stmtHeapSize){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update db cfg using stmtheap " + stmtHeapSize + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	public void setApplicationHeapSize(int appHeapSize){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update db cfg using applheapsz " + appHeapSize + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	public void setCPUSpeed(double cpuSpeed){
		try{
			Statement stmt = createStatement();
			String command = "call ADMIN_CMD ('update dbm cfg using cpuspeed " + cpuSpeed + "')";
			stmt.execute(command);
			stmt.close();
		} catch(Exception e){
			Constants.CPrintErrToConsole(e);
		}
	}

	@Override
	public boolean setHardwareStatistics() throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}