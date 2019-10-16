/*
#
# COPYRIGHT INFORMATION
#
# Copyright (C) 2012 Indian Institute of Science
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
# Telephone: (+91) 80 2293-2793
# Fax : (+91) 80 2360-2648
# Email: haritsa@dsl.serc.iisc.ernet.in
# WWW: http://dsl.serc.iisc.ernet.in/˜haritsa
#
 */

package iisc.dsl.codd.client;

import java.util.ArrayList;
import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.nonstopsql.NonStopSQLDatabase;
import iisc.dsl.codd.ds.Constants;


/**
 * This class implements the TPC-H Style Size Scaling. This is a command line version that doesn't call any GUI classes.
 *  * It scales the selected relation by the scaling factor.
 * @author dsladmin
 */
public class CommandLineSizeScaling {
	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 5174458817727387559L;
	String[] selectedRelation; // List of selected relations
	boolean runStats; // Indicates whether statistics on the selected relations must be collected before scaling or not
	Database database; // Database object

	
	
	/**
	 * Constructor for SizeScaling command line mode
	 * @param table_name - Table name to scale ; if null, assumes all the tables in the schema.
	 * @param sf - scale factor
	 * @param runStats - boolean, which determines the statistics collection on the selected relations before scaling
	 * @param database - Database object
	 */
	public CommandLineSizeScaling(String table_name, int sf, boolean runStats, Database database) throws DatalessException {
		
		Constants.CPrintToConsole("Command line Size scaling.", Constants.DEBUG_SECOND_LEVEL_Information);	
		this.database = database;		
		this.runStats = runStats;
				
		try
		{
			if(table_name == null || table_name.isEmpty())
			{
			  ArrayList<String> relations = database.getRelations();
			  int j = 0;
			  String [] selectedRelationTemp = new String[relations.size()];
			  String vendor = database.getSettings().getDbVendor();
			  for (int i = 0; i < relations.size(); i++) {
				String relation = relations.get(i);
				if (!DBConstants.isSystemCreatedRelationToIgnore(vendor, relation)) {
					selectedRelationTemp[j] = relation;
					j++;
				}
			  }
			  //Compact the array to 'j' values;
			  selectedRelation = new String[j];
			  for (int i = 0; i < j ; i++) {
				  selectedRelation[i] = selectedRelationTemp[i];
				  }			 		  
			}
			else
			{
			  selectedRelation = new String[1];
			  selectedRelation[0] = table_name;
			  if(database instanceof NonStopSQLDatabase)
				  selectedRelation[0] = selectedRelation[0].toUpperCase(); // ALL tables names are assumed to be in Upper case in SQL/MX
			  
			}		    
			sizeScalingForRelations(selectedRelation, sf, runStats, database);		
	     }		
		catch(Exception e)
		{
			throw new DatalessException(e.getMessage());
		}		
	}
	
	// Helper method. 
	private void sizeScalingForRelations(String[] tableNames, int sf, boolean runStats, Database database) {
		Constants.CPrintToConsole("Command line Size scaling.", Constants.DEBUG_SECOND_LEVEL_Information);					
				
		if(tableNames.length == 0)
		{
			Constants.CPrintToConsole("No relations to scale", Constants.DEBUG_SECOND_LEVEL_Information);					
		}
		else
		{
			
			boolean success = true;
			for (int i = 0; i < tableNames.length; i++) {
				String relation = (String) tableNames[i];
				try {
					Constants.CPrintToConsole("Size Scaling Relation " + relation + " by " + sf, Constants.DEBUG_SECOND_LEVEL_Information);	
					String msg;
					if (database.scale(relation, sf, runStats)) {
						msg = "Metadata Size Scaling on relation " + relation + " is successful.";
					} else {
						msg = "Metadata Size Scaling on relation " + relation + " has FAILED.";
						success = false;
					}
					Constants.CPrintToConsole(msg, Constants.DEBUG_SECOND_LEVEL_Information);
				} catch (Exception e) {
					Constants.CPrintToConsole("Exception Caught: Size Scaling of Relation failed for " + relation + ".", Constants.DEBUG_FIRST_LEVEL_Information );
					Constants.CPrintErrToConsole(e);
					success = false;
				}
			}
			if(success)
			{
				Constants.CPrintToConsole("Metadata Size Scaling of Relations is Successful.", Constants.DEBUG_SECOND_LEVEL_Information);				
			} else {
				Constants.CPrintToConsole("Metadata Size Scaling of Relations has Failed.", Constants.DEBUG_SECOND_LEVEL_Information);			
			}
		}
		
	}	

}
