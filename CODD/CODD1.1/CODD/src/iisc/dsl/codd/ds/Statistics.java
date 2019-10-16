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

import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.db2.DB2Database;
import iisc.dsl.codd.db.nonstopsql.NonStopSQLDatabase;
import iisc.dsl.codd.db.oracle.OracleDatabase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.swing.JOptionPane;

/**
 * This class represents the statistics associated with a relation.
 * It includes the relation statistics, column statistics (of all columns of this relation) and
 * the index statistics (all indexes on this relation).
 * @author dsladmin
 */
public class Statistics implements Serializable
{
	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -5153332159407288059L;

	/**
	 * relationName represents the relation name, for which the statistics are kept in this object.
	 */
	String relationName;

	/**
	 * dbType represents the database vendor name of the database, to which the relations belongs. This information is useful
	 * in transferring the statistics. This information is used to identify the inter / intra transfer.
	 */
	String dbType;

	/**
	 * relStat stores the RelationStatistics.
	 */
	RelationStatistics relStat;

	/**
	 * colStats is a HashMap, which maps the column name to the corresponding ColumnStatistics.
	 */
	public HashMap<String,ColumnStatistics> colStats;

	/**
	 * indexStats is a HashMap, which maps the each index to the corresponding IndexStatistics.
	 * indexes are identified by the indexed column names.
	 * indexed column names are represented in a string as follows: col1+col2, if col1, col2 are the indexed columns.
	 * So the HashMap will have an entry like <'col1+col2',indexStat>, where indexStat is an object of IndexStatistics.
	 */
	HashMap<String,IndexStatistics> indexStats;

	/**
	 * Constructor for Statistics.
	 * @param relationName - Name of the relation for which the statistics are kept in this object
	 * @param dbType - database vendor name
	 */
	public Statistics(String relationName, String dbType)
	{
		this.dbType = dbType;
		this.relationName = relationName;
		colStats = new HashMap<String,ColumnStatistics>();
		indexStats = new HashMap<String,IndexStatistics>();
	}

	/**
	 * Returns the relation name, for which the statistics are kept in this object.
	 * @return relationName
	 */
	public String getRelationName() {
		return relationName;
	}

	/**
	 * Replaces the relation name, for which the statistics are kept in this object.
	 * @param relationName - relation name
	 */
	public void setRelationName(String relationName) {
		this.relationName = relationName;
	}

	/**
	 * Replaces the relation statistics with the specified RelationStatistics.
	 * @param relStat - relation statistics
	 */
	public void  setRelationStatistics(RelationStatistics relStat)
	{
		this.relStat = relStat;
	}

	/**
	 * Returns the relation statistics.
	 * @return relStat
	 */
	public  RelationStatistics getRelationStatistics()
	{
		return relStat;
	}

	/**
	 * Adds the specified ColumnStatistics to the HashMap of ColumnStatistics.
	 * @param colName - Column name of the ColumnStatistics
	 * @param colStat ColumnStatistics
	 */
	public void addColumnStatistics(String colName, ColumnStatistics colStat)
	{
		System.out.println("colstats.put: " + colName);
		colStats.put(colName, colStat);
	}

	/**
	 * Returns the ColumnStatistics for the specified column Name.
	 * @param colName - Column name of the ColumnStatistics to be returned
	 * @return ColumnStatistics
	 */
	public ColumnStatistics getColumnStatistics(String colName)
	{
		System.out.println("getcolstats for " + colName);
		return colStats.get(colName);
	}

	/**
	 * Replaces the ColumnStatistics with the specified ColumnStatistics for the specified column.
	 * @param colName - Column name of the ColumnStatistics to be replaced
	 * @param colStat - ColumnStatistics to replace
	 */
	public void setColumnStatistics(String colName, ColumnStatistics colStat)
	{
		System.out.println("Setting column statistics for  " + colName);
		colStats.remove(colName);
		colStats.put(colName, colStat);
	}

	/**
	 * Adds the IndexStatistics to the HashMap of IndexStatistics.
	 * @param colNames - Columns names of the index in the form of 'col1+col2'
	 * @param indStat - IndexStatistics
	 */
	public void addIndexStatistics(String colNames, IndexStatistics indStat)
	{
		indexStats.put(colNames, indStat);
	}

	/**
	 * Returns the IndexStatistics for the specified indexed Column names.
	 * @param colNames - Columns names of the index in the form of 'col1+col2'.
	 * @return IndexStatistics
	 */
	public IndexStatistics getIndexStatistics(String colNames)
	{
		return indexStats.get(colNames);
	}

	/**
	 * Replaces the IndexStatistics with the specified IndexStatistics for the specified indexed Column names.
	 * @param colNames - Columns names of the index in the form of 'col1+col2'
	 * @param indStat -  IndexStatistics to replace
	 */
	public void setIndexStatistics(String colNames, IndexStatistics indStat)
	{
		indexStats.remove(colNames);
		indexStats.put(colNames, indStat);
	}

	/**
	 * Returns the indexed column names (in the form 'col1+col2') of all the relation indexes.
	 * @return String array of indexed column names. Each element represents a indexed column names of an index present in the relation.
	 */
	public String[] getIndexColumnsColNames()
	{
		String colNames[] = new String[indexStats.size()];
		Iterator<String> iterator = this.indexStats.keySet().iterator();
		int i =0;
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			colNames[i] = key;
			i++;
		}
		return colNames;
	}

	/**
	 * Updates the statistics represented by this object into the catalogs of the argument database.
	 * @param database - Database object
	 * @return true, if update is successful.
	 *         false, otherwise.
	 */
	public boolean updateStatisticsToDatabase(Database database)
	{
		// Order of Update. Index - Column - Relation
		boolean retValue = true;
		try
		{
			if(!database.reSetConnection())
			{
				JOptionPane.showMessageDialog(null, "Exception Caught: Connection Reset failed.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
			Constants.CPrintToConsole("Updating Statistics of Relation " + relationName, Constants.DEBUG_FIRST_LEVEL_Information);
			if(database instanceof NonStopSQLDatabase){
				((NonStopSQLDatabase)database).initializeRelationStatistics(relationName);
			}else if(database instanceof DB2Database){
				((DB2Database)database).setRelationStatistics(relationName, relStat, colStats);
			}else{
				if(database instanceof OracleDatabase){
					database.setHardwareStatistics();
				}
				database.setRelationStatistics(relationName, relStat);				
			}
			Iterator<String> iter = colStats.keySet().iterator();
			while (iter.hasNext()) {
				String colName = iter.next();
				ColumnStatistics colStat = colStats.get(colName);
				Constants.CPrintToConsole("Updating Column Statistics of Column: "+colName, Constants.DEBUG_FIRST_LEVEL_Information);
				database.setColumnStatistics(relationName, colName, colStat, relStat.getCardinality());
			}
			if(!(database instanceof NonStopSQLDatabase)){
	            iter = indexStats.keySet().iterator();
	            while (iter.hasNext()) {
	                String colNames = (String)iter.next();
	                IndexStatistics indStat = indexStats.get(colNames);
	                Constants.CPrintToConsole("Updating Index Statistics of Relation "+colNames, Constants.DEBUG_FIRST_LEVEL_Information);
	                ArrayList<String> cols = new ArrayList<String>();
	                if (colNames.contains("+")) {
	                    String[] col = colNames.split(Pattern.quote("+"));
	                    for (int c = 0; c < col.length; c++) {
	                        cols.add(col[c]);
	                    }
	                } else {
	                    cols.add(colNames);
	                }
	                database.setIndexStatistics(relationName, cols, indStat);				
	            }
            }
		}catch(Exception e)
		{
			retValue = false;
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, e.getMessage(), "CODD - Message", JOptionPane.ERROR_MESSAGE);
		}
		return retValue;
	}

	/**
	 * Writes the statistics represented by this object in the catalogs of the argument database.
	 * ToString() function of Statistics object is used to obtain the statistics in a string format, which is written to the file.
	 * @param filename - Filename, in which the statistics will be written.
	 */
	public void writeStatisticsToFile(String filename)
	{
		try
		{
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filename)));
			writer.write("Statistics of Relation "+this.relationName+" \n");
			writer.write("----------------------------------------------- \n");
			writer.write(relStat.toString());
			writer.write("\n");
			Set<String> s = colStats.keySet();
			Iterator<String> i = s.iterator();
			while (i.hasNext()) {
				String colName = (String)i.next();
				ColumnStatistics colStat = colStats.get(colName);
				writer.write("Statistics of Column "+colName+" ( "+colStat.getColumnType()+" ): "+colStat.getConstraint().toString()+" \n");
				writer.write("----------------------------------------------- \n");
				writer.write(colStat.toString());
				writer.write("\n");
			}
			s = indexStats.keySet();
			i = s.iterator();
			while (i.hasNext()) {
				String colNames = (String)i.next();
				IndexStatistics indStat = indexStats.get(colNames);
				writer.write("Statistics of index "+colNames+" \n");
				writer.write("----------------------------------------------- \n");
				writer.write(indStat.toString());
				writer.write("\n");
			}
			writer.close();
		}catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
		}
	}

	/**
	 * Gets the statistics for the realationName of this object using the database object.
	 * THis function is used in the Executor to get the statistics of a relation.
	 * @param database - Database object
	 */
	public void getStatistics(Database database)
	{
		try
		{
			if(!database.reSetConnection())
			{
				JOptionPane.showMessageDialog(null, "Exception Caught: Connection Reset failed.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
			Constants.CPrintToConsole("Getting Statistics of Relation "+this.relationName, Constants.DEBUG_FIRST_LEVEL_Information);
			relStat = database.getRelationStatistics(relationName);

			String[] attribs; 
			if(this.dbType.equals(DBConstants.NONSTOPSQL)){
				attribs = database.getMultiColumnAttributes(relationName);
			}else {
				attribs = database.getAttributes(relationName);
			}
			Constants.CPrintToConsole("Getting Attribute statistics: ", Constants.DEBUG_FIRST_LEVEL_Information);
			for (int j = 0; j < attribs.length; j++) {
				String type = database.getType(relationName, attribs[j]);
				Constants.CPrintToConsole("Getting statistics " + attribs[j] + " : " + type, Constants.DEBUG_FIRST_LEVEL_Information);
				ColumnStatistics colStat = database.getColumnStatistics(relationName, attribs[j], relStat.getCardinality());
				addColumnStatistics(attribs[j], colStat);
			}
			Constants.CPrintToConsole("Getting statistics for Indexed Attributes: ", Constants.DEBUG_FIRST_LEVEL_Information);
			Map<?, ?> map = database.getIndexedColumns(relationName);
			Iterator<?> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = (String) iterator.next();
				String colNames = (String) map.get(key);
				ArrayList<String> cols = new ArrayList<String>();
				if (colNames.contains("+")) {
					String[] col = colNames.split(Pattern.quote("+"));
					for (int c = 0; c < col.length; c++) {
						cols.add(col[c]);
					}
				} else {
					cols.add(colNames);
				}
				IndexStatistics indexStat = database.getIndexStatistics(relationName, cols);

				Constants.CPrintToConsole("Getting for statistics " + key + " : " + map.get(key), Constants.DEBUG_FIRST_LEVEL_Information);
				addIndexStatistics(colNames, indexStat);
			}

		}
		catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
		}
	}
}