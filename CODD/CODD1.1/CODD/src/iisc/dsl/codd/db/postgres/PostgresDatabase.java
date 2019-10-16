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
package iisc.dsl.codd.db.postgres;

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
import iisc.dsl.codd.ds.Statistics;
import iisc.dsl.codd.plan.Node;
import iisc.dsl.codd.plan.Plan;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Pattern;

/**
 * Implementation of the functionalities specific to Postgres, required for CODD.
 * @author dsladmin
 */
public class PostgresDatabase extends DatabaseAbstract {
	// Added datastructure for getPlan
	private int leafid=0;

	/**
	 * Constructor for PostgresDatabase
	 * @param settings - Database setting object
	 * @throws DatalessException
	 */
	public PostgresDatabase(DBSettings settings) throws DatalessException {
		super(settings);
	}

	@Override
	public String getQuery_stopAutoUpdateStats() {
		// Right the user has to stop (set false) the AUTO_VACUUM (TRACK_COUNT) and AUTO_ANALYZE on the postgresql.conf file. TODO: Automate it.
		return null;
	}

	@Override
	public String getQuery_SelectRelations() {
		return "select relname from pg_stat_user_tables where schemaname = '" + this.getSchema().toLowerCase() + "' order by relname";
	}

	@Override
	public String getQuery_dependentRelations(String relation) {
		return "select conrelid from pg_class, pg_constraint where confrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='f'";
	}

	@Override
	public String getQuery_columnDataType(String relation, String column) {
		return "select typname from  pg_class, pg_attribute, pg_type where attrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and attname='"+column.toLowerCase()+"' and atttypid=pg_type.oid";
	}

	@Override
	public String getQuery_getIndexedColumns(String relation) throws DatalessException {
		return "select indexrelid, indkey from pg_class, pg_index where indrelid=pg_class.oid and relname='"+relation.toLowerCase()+"'";
	}
	
	public String getQuery_getSystemPrimaryIndex(String relation) throws DatalessException {
		return "select indexrelid, indkey from pg_class, pg_index where indrelid=pg_class.oid and relname='"+relation.toLowerCase()+"'";
	}

	@Override
	public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public String getQuery_getAttributes(String relation) throws DatalessException {
		return "select attname from pg_class, pg_attribute where attrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and attnum > 0";
	}

	@Override
	public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
		return "select conkey from pg_class, pg_constraint where conrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='p'";
	}

	@Override
	public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
		return "select confrelid, conkey from pg_class, pg_constraint where conrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='f'";
	}

	@Override
	public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
		return "select conkey from pg_class, pg_constraint where conrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='f'";
	}

	@Override
	public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
		return "select conkey from pg_class, pg_constraint where conrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='u'";
	}

	@Override
	public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
		return "select attnotnull from pg_class, pg_attribute where relname='"+relation.toLowerCase()+"' and attname='"+column.toLowerCase()+"'";
	}

	@Override
	public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
		return "select conkey, confrelid, confkey from pg_class, pg_constraint where conrelid=pg_class.oid and relname='"+relation.toLowerCase()+"' and contype='f'";
	}

	public boolean connect(DBSettings settings) throws DatalessException{
		String connectString;
		if (isConnected()) {
			return true;
		}
		this.settings = settings;
		try {
			Class.forName("org.postgresql.Driver").newInstance();
			connectString = "jdbc:postgresql://" + settings.getServerName() + ":" + settings.getServerPort()
					+ "/" + settings.getDbName();
			con = DriverManager.getConnection(connectString, settings.getUserName(), settings.getPassword());

		} catch (Exception e) {
			throw new DatalessException("Database Engine " + settings.getDbName() + " is not accepting connections");
		}
		if (con != null) {
			return true;
		}
		return false;
	}

	@Override
	public boolean retain(String [] dropRelations, String[] dependentRelations) throws DatalessException {

		boolean success = true;
		/*
		 * TRUNCATE flushes away Relation and Index statistics, but keeps column statistics.
		 *
		 * 1) Create a Dependency Graph
		 * 2) Topological Sort
		 * 3) Bottom Up (reverse) traversal of each relation
		 *      3.1) Get Relation and Index Statistics
		 *      3.2) Truncate Relation
		 * 4) For each relation (Order of traversal doesn't matter here)
		 *      4.1) Restore the Relation and Index statistics
		 *
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
		/**
		 * Step 1) Dependency Graph
		 * Step 2) Topological Sort
		 */
		ArrayList<String> sortedRelations = getTopologicalSortedRelations(allRelations);
		if(sortedRelations == null) {
			return false;
		}

		/** Step 3) Top Down traversal of each relation
		 *      3.1) Get Relation and Index Statistics
		 *      3.2) Truncate Relation
		 */
		Statistics[] stats = new Statistics[sortedRelations.size()];
		for(int s=0;s<sortedRelations.size();s++) {
			String relation = sortedRelations.get(s);
			Constants.CPrintToConsole("Obtaining Relation and Index level statistics for "+relation, Constants.DEBUG_SECOND_LEVEL_Information);
			stats[s] = new Statistics(relation, this.getSettings().getDbVendor());
			RelationStatistics relStat = this.getRelationStatistics(relation);
			stats[s].setRelationStatistics(relStat);
			/*
			 * As 'TRUNCATE rel CASCADE' and 'ANALYZE rel' does not update the column statistics (observation), there is no need to read and restore them.
			 * But still, it can be read ans restored to be safe as these commands behavior is not confirmed.
			 *
			 */
			String[] attribs = this.getAttributes(relation);
			for (int j = 0; j < attribs.length; j++) {
				String type = this.getType(relation, attribs[j]);
				ColumnStatistics colStat = this.getColumnStatistics(relation, attribs[j], relStat.getCardinality());
				stats[s].addColumnStatistics(attribs[j], colStat);
			}
			Map map = this.getIndexedColumns(relation);
			Iterator iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = (String) iterator.next();
				String colNames = (String) map.get(key);
				ArrayList<String> cols = new ArrayList();
				if (colNames.contains("+")) {
					String[] col = colNames.split(Pattern.quote("+"));
					for (int c = 0; c < col.length; c++) {
						cols.add(col[c]);
					}
				} else {
					cols.add(colNames);
				}
				IndexStatistics indexStat = this.getIndexStatistics(relation, cols);
				stats[s].addIndexStatistics(colNames, indexStat);
			}
		}

		for(int s=0;s<sortedRelations.size();s++) {
			String relation = sortedRelations.get(s);
			try {
				Constants.CPrintToConsole("Truncating relation "+relation, Constants.DEBUG_SECOND_LEVEL_Information);
				String command = "TRUNCATE " + relation.toLowerCase() + " CASCADE;";
				Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				stmt.execute(command);
				stmt.close();
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				throw new DatalessException("Exception in Truncating relation: "+relation+".");
			}
		}

		/** Step 4) For each relation (Order of traversal doesn't matter here)
		 *      4.1) Restore the Relation and Index statistics
		 */

		for(int s=0;s<sortedRelations.size();s++) {
			String relation = sortedRelations.get(s);
			Constants.CPrintToConsole("Restoring Relation and Index statistics for: "+relation+".", Constants.DEBUG_SECOND_LEVEL_Information);
			stats[s].updateStatisticsToDatabase(this);
		}
		return success;
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
			String query = "ANALYZE " + this.getSchema().toLowerCase() + "." + relation.toLowerCase();
			Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(query);
			stmt.close();
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in collecting Statistics for " + relation + " of " + settings.getDbName());
		}
	}

	@Override
	public RelationStatistics getRelationStatistics(String relation) throws DatalessException {
		PostgresRelationStatistics relStat = new PostgresRelationStatistics(relation, this.getSchema());
		try
		{
			// Reading PG_CLASS table
			String command = "select reltuples, relpages from pg_class where relname = '"+relation.toLowerCase()+"'"; //and relowner = '"+this.getSchema()+"'";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				
				/*BigDecimal card = new BigDecimal(rset.getInt(1)+"");
				BigDecimal relpages = new BigDecimal(rset.getInt(2)+"");*/
				BigDecimal card = new BigDecimal(rset.getBigDecimal(1)+"");
				BigDecimal relpages = new BigDecimal(rset.getInt(2)+"");
				
				relStat.setCardinality(card);
				relStat.setRelPages(relpages);
				Constants.CPrintToConsole("reltuples|relpages", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(card+"|"+relpages, Constants.DEBUG_SECOND_LEVEL_Information);
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
		String type = getType(relation, column);
		PostgresColumnStatistics colStat = null;
		try {
			colStat = new PostgresColumnStatistics(relation, column, type, getConstraint(relation, column));
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception intialzeing Column Statisitcs " + column + " (" + relation + ")  of " + settings.getDbName());
		}

		try {
			String command = "select null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation from pg_stats where tablename='" + relation.toLowerCase() + "' and attname='"+column.toLowerCase()+"' and schemaname='"+this.getSchema().toLowerCase()+"'";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				Double null_frac = Double.parseDouble(rset.getString(1));
				Integer avg_width = Integer.parseInt(rset.getString(2));
				Double n_distinct = Double.parseDouble(rset.getString(3));
				String mostCommonVals = rset.getString(4);
				String mostCommonFreqs = rset.getString(5);
				String histogramBounds = rset.getString(6);
				Double correlation = Double.parseDouble(rset.getString(7));
				colStat.setNull_frac(null_frac);
				colStat.setAvgWidth(avg_width);
				colStat.setN_distinct(n_distinct);
				colStat.setMost_common_vals(mostCommonVals);
				colStat.setMost_common_freqs(mostCommonFreqs);
				colStat.setHistogram_bounds(histogramBounds);
				colStat.setCorrelation(correlation);
				Constants.CPrintToConsole("null_frac|avg_width|n_distinct|most_common_vals|most_common_freqs|histogram_bounds|correlation", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(null_frac+"|"+avg_width+"|"+n_distinct+"|"+mostCommonVals+"|"+mostCommonFreqs+"|"+histogramBounds+"|"+correlation, Constants.DEBUG_SECOND_LEVEL_Information);
				colStat.convert_PostgresFormat2CoddFormat(tableCard);
			}
			rset.close();
			stmt.close();

		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Column Statistics of relation " + relation + " of " + settings.getDbName());
		}
		return colStat;
	}

	@Override
	public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {
		PostgresIndexStatistics indexStat = new PostgresIndexStatistics(relation, colNames);
		String indexName = getIndexName(relation, colNames);
		try
		{
			// Reading PG_CLASS table
			String command = "select reltuples, relpages from pg_class where relname = '"+indexName+"'"; //and relowner = '"+this.getSchema()+"'";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			while (rset.next()) {
				//BigDecimal reltuples = new BigDecimal(rset.getInt(1)+"");
				BigDecimal reltuples = new BigDecimal(rset.getBigDecimal(1)+"");
				BigDecimal relpages = new BigDecimal(rset.getInt(2)+"");
				indexStat.setRelpages(relpages);
				indexStat.setReltuples(reltuples);
				Constants.CPrintToConsole("reltuples|relpages", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(reltuples+"|"+relpages, Constants.DEBUG_SECOND_LEVEL_Information);
			}
			rset.close();
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Index "+indexName+" Statistics ("+relation+") of "+settings.getDbName());
		}
		return indexStat;
	}

	@Override
	public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException {
		//boolean sameDBType = false;
		PostgresRelationStatistics relStat = new PostgresRelationStatistics(relation, relationStatistics.getSchema());
		relStat.mapForPorting(relationStatistics);
		try{
			// Initialize Statistics.
			collectStatistics(relation);
			String command;
			try{
				if (relStat.getRelPages().intValueExact() > Integer.MAX_VALUE)
				{
					BigDecimal bd = new BigDecimal(String.valueOf(Integer.MAX_VALUE));
					relStat.setRelPages(bd);
				}
			} catch (Exception e) {
				Constants.CPrintToConsole("Relation Pages exceeded allowed INTEGER value, setting to INTEGER.MAX_VALUE", Constants.DEBUG_SECOND_LEVEL_Information);
				BigDecimal bd = new BigDecimal(String.valueOf(Integer.MAX_VALUE));
				relStat.setRelPages(bd);
			}
			command= "UPDATE pg_class SET relpages="+relStat.getRelPages()+", reltuples="+relStat.getCardinality()+" WHERE relname='"+relation.toLowerCase()+"';";// AND owner='"+this.getSchema()+"';";

			Statement stmt = createStatement();
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command);
			stmt.close();
		} catch (Exception e)	{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Relation "+relation+" Statistics of "+settings.getDbName());
		}
		return true;
	}

	@Override
	public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException {
		// boolean sameDBType = false;
		String type = getType(relation, column);
		PostgresColumnStatistics colStat = new PostgresColumnStatistics(relation, column, type, getConstraint(relation, column));;
		colStat.mapForPorting(columnStatistics, tableCard);
		try
		{	
            if(colStat.histogram_bounds != null && colStat.histogram_bounds.startsWith("{")){
            	colStat.histogram_bounds = colStat.histogram_bounds.substring(1, colStat.histogram_bounds.length()-1);        		
        	}
            if(colStat.most_common_vals != null && colStat.most_common_vals.startsWith("{")){
            	colStat.most_common_vals = colStat.most_common_vals.substring(1, colStat.most_common_vals.length()-1);        		
        	
            	if(colStat.most_common_freqs.startsWith("{")){
            		colStat.most_common_freqs = colStat.most_common_freqs.substring(1, colStat.most_common_freqs.length()-1);        		
            	}
            } 
            
            String delim = "\\";

            if(colStat.histogram_bounds != null)
            {
            	if(type.equalsIgnoreCase("varchar") || type.equalsIgnoreCase("bpchar") || type.equalsIgnoreCase("text"))
	            {
	            	String temp = colStat.histogram_bounds;
	            	//System.out.println ("Temp :" + temp);
	            	colStat.histogram_bounds = "";
	                while(!temp.isEmpty()) {
	                    String col;
	                    if(temp.startsWith("\"")) {
	                        int endInd = temp.indexOf("\"",1);
	                        col = temp.substring(1, endInd);
	                        temp = temp.substring(endInd+1);
	                    } else {
	                        int endInd = temp.indexOf(",");
	                        if(endInd == -1) {
	                            col = temp;
	                            temp = "";
	                        } else {
	                            col = temp.substring(0, endInd) + delim;
	                            temp = temp.substring(endInd+1);
	                        }
	                    }
	                    colStat.histogram_bounds = colStat.histogram_bounds + col;
	                }
	            } else {
		            	String temp = colStat.histogram_bounds;
		            	//System.out.println ("Temp :" + temp);
		            	colStat.histogram_bounds = "";
		            	while(!temp.isEmpty()) {
			                    String col;
			                    int endInd = temp.indexOf(",");
		                        if(endInd == -1) {
		                            col = temp;
		                            temp = "";
		                        } else {
		                            col = temp.substring(0, endInd) + delim;
		                            temp = temp.substring(endInd+1);
		                        }
		                    colStat.histogram_bounds = colStat.histogram_bounds + col;
		            	 }
	            	}
            }
            
            if(colStat.most_common_vals != null)
            {
            	if(type.equalsIgnoreCase("varchar") || type.equalsIgnoreCase("bpchar") || type.equalsIgnoreCase("text"))
	            {
	            	String temp = colStat.most_common_vals;
	            	//System.out.println ("Temp :" + temp);
	            	colStat.most_common_vals = "";
	                while(!temp.isEmpty()) {
	                    String col;
	                    if(temp.startsWith("\"")) {
	                        int endInd = temp.indexOf("\"",1);
	                        col = temp.substring(1, endInd);
	                        temp = temp.substring(endInd+1);
	                    } else {
	                        int endInd = temp.indexOf(",");
	                        if(endInd == -1) {
	                            col = temp;
	                            temp = "";
	                        } else {
	                            col = temp.substring(0, endInd) + delim;
	                            temp = temp.substring(endInd+1);
	                        }
	                    }
	                    colStat.most_common_vals = colStat.most_common_vals + col;
	                }
	            } else {
	            	String temp = colStat.most_common_vals;
	            	//System.out.println ("Temp :" + temp);
	            	colStat.most_common_vals = "";
	            	while(!temp.isEmpty()) {
		                    String col;
		                    int endInd = temp.indexOf(",");
	                        if(endInd == -1) {
	                            col = temp;
	                            temp = "";
	                        } else {
	                            col = temp.substring(0, endInd) + delim;
	                            temp = temp.substring(endInd+1);
	                        }
	                    colStat.most_common_vals = colStat.most_common_vals + col;
	            	 }
            	}
            }
            
            if (colStat.most_common_freqs != null)
            {
            	String temp = colStat.most_common_freqs;
              	//System.out.println ("Temp :" + temp);
            	colStat.most_common_freqs = "";
                while(!temp.isEmpty()) {
                    String col;
                    int endInd = temp.indexOf(",");
                    if(endInd == -1) {
                        col = temp;
                        temp = "";
                    } else {
                        col = temp.substring(0, endInd) + delim;
                        temp = temp.substring(endInd+1);
                    }
                    colStat.most_common_freqs = colStat.most_common_freqs + col;
                }
            }
            
			Statement stmt = createStatement();
			String columnMetadata = colStat.null_frac+"|"+colStat.avgWidth+"|"+colStat.n_distinct+"|"
									+colStat.most_common_vals+"|"+colStat.most_common_freqs+"|"+colStat.histogram_bounds+"|"+colStat.correlation;
			
			String command = "DL_ANALYZE " + relation + "(" + column + ") '" + type + "' '" + columnMetadata + "';";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command);
			stmt.close();
		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in writing Column statistics "+column+" ("+relation+") using DL_ANALYZE of "+settings.getDbName());
		}
		return true;
	}

	@Override
	public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException {
		//boolean sameDBType = false;
		PostgresIndexStatistics indexStat = new PostgresIndexStatistics(relation, colNames);
		indexStat.mapForPorting(indexStatistics);
		String indexName = getIndexName(relation, colNames);
		if(indexName == null) {
			Constants.CPrintToConsole("Index doesn't exist for relation.", Constants.DEBUG_SECOND_LEVEL_Information);
			return true;
		}
		/*
        if(indexStatistics instanceof PostgresIndexStatistics)
        {
            indexStat = (PostgresIndexStatistics)indexStatistics;
            sameDBType = true;
        }
		 */
		try {
			String command;
			//if(sameDBType)
			//{
			try{
				if (indexStat.getRelpages().intValueExact() > Integer.MAX_VALUE)
				{
					BigDecimal bd = new BigDecimal(String.valueOf(Integer.MAX_VALUE));
					indexStat.setRelpages(bd);
				}
			} catch (Exception e) {
				Constants.CPrintToConsole("Relation Pages exceeded allowed INTEGER value, setting to INTEGER.MAX_VALUE", Constants.DEBUG_SECOND_LEVEL_Information);
				BigDecimal bd = new BigDecimal(String.valueOf(Integer.MAX_VALUE));
				indexStat.setRelpages(bd);
			}
			command= "UPDATE pg_class SET relpages="+indexStat.getRelpages()+", reltuples="+indexStat.getReltuples()+" WHERE relname='"+indexName.toLowerCase()+"';";// AND owner='"+this.getSchema()+"';";
			Statement stmt = createStatement();
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(command);
			stmt.close();
			//}

			return true;
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating index ("+indexName+")Statistics for Relation "+relation+" Statistics of "+settings.getDbName());
		}
	}

	@Override
	public Plan getPlan(String query) throws DatalessException {
		Vector textualPlan = new Vector();
		Plan plan = new Plan();

		try{
			Statement stmt = createStatement();
			ResultSet rs = stmt.executeQuery("EXPLAIN "+query);
			while(rs.next()){
				String txt=rs.getString(1);
				int index=-1;
				if((index=txt.indexOf("Cond:"))>0)
				{
					txt=txt.substring(index).trim();
					textualPlan.set(textualPlan.size()-1, textualPlan.get(textualPlan.size()-1)+" "+txt);
				}
				else if((index=txt.indexOf("Filter:"))>0)
				{
					txt=txt.substring(index).trim();
					textualPlan.set(textualPlan.size()-1, textualPlan.get(textualPlan.size()-1)+" "+txt);
				}
				else if((index=txt.indexOf("Sort Key:"))>0)
				{
					textualPlan.set(textualPlan.size()-1, textualPlan.get(textualPlan.size()-1)+" "+txt.trim());
				}
				else textualPlan.add(txt);
			}

			rs.close();
			stmt.close();
			if(textualPlan.size()<=0)
				return null;
			/*ListIterator it = textualPlan.listIterator();
                        System.out.println("Parsing Query Plan");
                        while(it.hasNext())
                                System.out.println((String)it.next());*/
		}catch(Exception e){
			e.printStackTrace();
			throw new DatalessException("Error getting plan: "+e);
		}
		//parseNode(plan,0,-1,textualPlan);
		//plan.show();
		CreateNode(plan, (String)textualPlan.remove(0), 0, -1);
		FindChilds(plan, 0, 1, textualPlan, 2);
		SwapSORTChilds(plan);
		return plan;
	}

	int CreateNode(Plan plan, String str, int id, int parentid) {

		if(id==1)
			leafid=-1;
		Node node = new Node();
		if(str.indexOf("->")>=0)
			str=str.substring(str.indexOf("->")+2).trim();
		String cost = str.substring(str.indexOf("..") + 2, str.indexOf("rows") - 1);
		String card = str.substring(str.indexOf("rows") + 5, str.indexOf("width")-1);
		if(str.indexOf(" on ") != -1 ||str.startsWith("Subquery Scan")) {
			node.setId(id++);
			node.setParentId(parentid);
			node.setCost(Double.parseDouble(cost));
			node.setCard(Double.parseDouble(card));
			if(str.startsWith("Index Scan"))
				node.setName("Index Scan");
			else if(str.startsWith("Subquery Scan"))
				node.setName("Subquery Scan");
			else
				node.setName(str.substring(0,str.indexOf(" on ")).trim());
			plan.setNode(node,plan.getSize());
			node = new Node();
			node.setId(leafid--);
			node.setParentId(id-1);
			node.setCost(0.0);
			node.setCard(0.0);
			if(str.startsWith("Subquery Scan"))
				node.setName(str.trim().substring("Subquery Scan".length(),str.indexOf("(")).trim());
			else
				node.setName(str.substring(str.indexOf(" on ")+3,str.indexOf("(")).trim());
			plan.setNode(node,plan.getSize());
		} else {
			node.setId(id++);
			node.setParentId(parentid);
			node.setCost(Double.parseDouble(cost));
			node.setCard(Double.parseDouble(card));
			node.setName(str.substring(0,str.indexOf("(")).trim());
			plan.setNode(node,plan.getSize());

		}
		int condIndex=str.indexOf("Cond:");
		int filterIndex=str.indexOf("Filter:");
		if(condIndex>0)
		{
			node.addArgType("Cond:");
			if(filterIndex>condIndex)
			{
				node.addArgValue((str.substring(str.indexOf("Cond:")+5,str.indexOf("Filter:")).trim()));
			}
			else
			{
				node.addArgValue((str.substring(str.indexOf("Cond:")+5).trim()));
			}

		}
		if(filterIndex>0)
		{
			node.addArgType("Predicates List");
			if(filterIndex<condIndex)
			{
				//node.addArgValue(processPredicates(str.substring(str.indexOf("Filter:")+7,str.indexOf("Cond:")).trim()));
				node.addArgValue(str.substring(str.indexOf("Filter:")+7));
			}
			else
			{
				//node.addArgValue(processPredicates(str.substring(str.indexOf("Filter:")+7).trim()));
				node.addArgValue(str.substring(str.indexOf("Filter:")+7));
			}
		}
		if(str.contains("Sort Key:"))
		{
			node.addArgType("Sort Key:");
			node.addArgValue((str.substring(str.indexOf("Sort Key:")+9).trim()));
		}


		return id;
	}

	int FindChilds(Plan plan, int parentid, int id, Vector text, int childindex) {
		String str ="";
		int oldchildindex=-5;
		while(text.size()>0) {
			int stindex;
			str = (String)text.remove(0);

			if(str.trim().startsWith("InitPlan"))
				stindex=str.indexOf("InitPlan");
			else if(str.trim().startsWith("SubPlan"))
				stindex=str.indexOf("SubPlan");
			else
				stindex=str.indexOf("->");

			if(stindex==-1)
				continue;
			if(stindex==oldchildindex) {
				childindex=oldchildindex;
				oldchildindex=-5;
			}
			if(stindex < childindex) {
				text.add(0,str);
				break;
			}


			if(stindex>childindex) {
				if(str.trim().startsWith("InitPlan")||str.trim().startsWith("SubPlan")) {
					str = (String)text.remove(0);
					stindex=str.indexOf("->");
					oldchildindex=childindex;
					childindex=str.indexOf("->");
				}
				text.add(0,str);
				id = FindChilds(plan, id-1, id, text, stindex);
				continue;
			}

			if(str.trim().startsWith("InitPlan")||str.trim().startsWith("SubPlan")) {
				str = (String)text.remove(0);
				stindex=str.indexOf("->");
				oldchildindex=childindex;
				childindex=str.indexOf("->");
			}
			if(stindex==childindex)
				id = CreateNode(plan,str, id, parentid);
		}
		return id;
	}

	void SwapSORTChilds(Plan plan) {
		for(int i =0;i<plan.getSize();i++) {
			Node node = plan.getNode(i);
			if(node.getName().equals("Sort")) {
				int k=0;
				Node[] chnodes = new Node[2];
				for(int j=0;j<plan.getSize();j++) {
					if(plan.getNode(j).getParentId() == node.getId()) {
						if(k==0)chnodes[0]=plan.getNode(j);
						else chnodes[1]=plan.getNode(j);
						k++;
					}
				}
				if(k>=2) {
					k=chnodes[0].getId();
					chnodes[0].setId(chnodes[1].getId());
					chnodes[1].setId(k);

					for(int j=0;j<plan.getSize();j++) {
						if(plan.getNode(j).getParentId() == chnodes[0].getId())
							plan.getNode(j).setParentId(chnodes[1].getId());
						else if(plan.getNode(j).getParentId() == chnodes[1].getId())
							plan.getNode(j).setParentId(chnodes[0].getId());
					}
				}
			}
		}
	}

	@Override
	public String[] getMultiColumnAttributes(String relation) throws DatalessException{
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

	@Override
	public boolean setHardwareStatistics() throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}