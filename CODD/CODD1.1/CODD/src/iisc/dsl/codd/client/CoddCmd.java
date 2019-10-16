package iisc.dsl.codd.client;

	
	import iisc.dsl.codd.DatalessException;
	import iisc.dsl.codd.db.DBConstants;
    import iisc.dsl.codd.ds.Constants;
	import iisc.dsl.codd.ds.DBSettings;
	//import java.io.File;
	//import java.io.FileNotFoundException;
	//import java.util.ArrayList;
	//import java.util.Scanner;
	import java.io.BufferedReader;  
	//import java.io.BufferedWriter; 
	import java.io.FileReader;  
	//import java.io.FileWriter; 
	import java.io.IOException;
	import iisc.dsl.codd.db.Database;
	import iisc.dsl.codd.db.db2.DB2Database;
	import iisc.dsl.codd.db.mssql.MSSQLDatabase;
	import iisc.dsl.codd.db.nonstopsql.NonStopSQLDatabase;
	import iisc.dsl.codd.db.oracle.OracleDatabase;
	import iisc.dsl.codd.db.postgres.PostgresDatabase;
	import iisc.dsl.codd.db.sybase.SybaseDatabase;
	//import iisc.dsl.codd.ds.Constants;
	//import iisc.dsl.codd.client.ConnectDBFrame;

    import java.sql.*;

	public class CoddCmd {
		 //static Connection con = null;
		 //static String url = null;
		 DBSettings settings;
				 
		//The syntax could be something like  $> coddCmd - connection_config_string <conection name> -scale_factor <SF no> 
		//                                             -run_stats <false/true> (optional) -table <table_name> (optional)
		// The arguments are expected in the above order ( except the optional argument which can be swapped among them )

		//Table_name would be optional so that we can scale a particular table in a database and NOT all the tables in the given 
		//the CAT/SCH.

		//Assumption: This assumes an initial configuration to be available and created using the CODD GUI. This is a reasonable 
		//assumption to make and it simplifies the command line processing a lot.
		
		public static void main(String args[]) throws IOException, DatalessException
		{
			 
		  String line;
		  String dbServerName = null;
		  String dbType = null;
		  String dbServerPort = null;
		  String dbSchema = null;
	      String dbUserName = null;
		  String dbPassword = null;
		  String dbCatalog = null;
		  String serverInstance = null;
		  String dbVersion = null;
	      String dbName = null;
			
		  String stringScaleFactor = null;
		  long   scaleFactor = 1;
		  String tableName = null;
		  Boolean runStats = false;
			
		  String msg = new String();
		 			
		  boolean error = false;
		  BufferedReader in_1 = null;
			 
	      try {
				
			//$> coddCmd - connection_config_string <conection name> -scale_factor <SF no> -table <table_name> (optional) -run_stats <0/1>
	    	if(args.length == 0) 
	    	{
	    		msg = "\nUsage:  coddCmd - connection_config_string <conection name> -scale_factor <SF no>  -run_stats <0/1> (optional) -table <table_name> (optional) \n";
				System.out.println(msg);
				return;
	    	}
	      
			if (args.length > 0 && args[0].equals("-connection_config_string")){			
				if (args.length > 1) 
					in_1 = new BufferedReader(new FileReader(args[1]));
			}	 
		    if(args.length > 2 && args[2].equals("-scale_factor")) { 
		    	if (args.length > 3) stringScaleFactor=args[3];  
           	
           	   try {
           		  scaleFactor = Long.parseLong(stringScaleFactor);
			   } catch (NumberFormatException e) {
				  Constants.CPrintToConsole("Validation Error - Scaling Factor is not of Integer type", Constants.DEBUG_FIRST_LEVEL_Information);
			      Constants.CPrintErrToConsole(e);
		          return;
			   }           	       	 
		    }
		    if (stringScaleFactor == null || stringScaleFactor.isEmpty()) {
				error = true;
				msg = msg + " Scale Factor,";
			}     	    
		    /*		 
		    if(args.length > 4 && args[4].equals("-table")) { 
		      if (args.length > 5) tableName=args[5];
	          // Since tableName is optional; we do the error checking only if we have -table specified !
	          if (tableName == null || tableName.isEmpty()) {
				error = true;
				msg = msg + " Table Name,";
			  }            	            	           	 
			}
		    if(args.length > 6 && args[6].equals("-run_stats")) {               
	            try {
	            	if (args.length > 7) runStats=Boolean.parseBoolean(args[7]);
				} catch (NumberFormatException e) {
					  Constants.CPrintToConsole("Validation Error - runStats is not of Boolean type", Constants.DEBUG_FIRST_LEVEL_Information);
				      Constants.CPrintErrToConsole(e);
			          return;
				}           	       	 
			}	
		    */
		    
		    if(args.length > 4 ) {
		    	  // Process the optional -table argument
		    	  if (args[4].equals("-table")) { 
			          if (args.length > 5) tableName=args[5];
			          // Since tableName is optional; we do the error checking only if we have -table specified !
			          if (tableName == null || tableName.isEmpty()) {
						  error = true;
						  msg = msg + " Table Name,";
					  } 
		    	  }
		          if (args.length > 6 && args[6].equals("-table")) { 
			          if (args.length > 7) tableName=args[7];
			          // Since tableName is optional; we do the error checking only if we have -table specified !
			          if (tableName == null || tableName.isEmpty()) {
						  error = true;
						  msg = msg + " Table Name,";
					  } 
		          }
		                     	            	           	 
			      // Process the optional run_stats
		          try {
				      if (args[4].equals("-run_stats"))
				    	if (args.length > 5) runStats=Boolean.parseBoolean(args[5]);	    		  
				        
				      if (args.length > 6 && args[6].equals("-run_stats"))               
			           	if (args.length > 7) runStats=Boolean.parseBoolean(args[7]);
				    } 
				    catch (NumberFormatException e) {
						Constants.CPrintToConsole("Validation Error - runStats is not of Boolean type", Constants.DEBUG_FIRST_LEVEL_Information);
					    Constants.CPrintErrToConsole(e);
				        return;
					}           	       	 
		     }			   
		
			if (in_1 != null)
			{
			    while((line = in_1.readLine()) != null)
			    {
//			       System.out.println(line);
			       String[] temp = line.split("::");
	               if(temp[0].equals("ServerName")) 
	            	 dbServerName=temp[1];
	               else if(temp[0].equals("DBType")) 
	            	 dbType=temp[1];
	               else if(temp[0].equals("Port")) 
	            	 dbServerPort=temp[1];
	               else if(temp[0].equals("DBSchema")) 
	            	 dbSchema=temp[1];
	               else if(temp[0].equals("UserName")) 
	            	 dbUserName=temp[1];
	               else if(temp[0].equals("Password")) 
	            	 dbPassword=temp[1];
	               else if(temp[0].equals("DBCatalog")) 
	            	 dbCatalog=temp[1];
	               else if(temp[0].equals("ServerInstance")) 
	            	 serverInstance=temp[1];
	               else if(temp[0].equals("VersionNumber")) 
	            	 dbVersion=temp[1];
	             
			     }
			     in_1.close();
			  }
			 
			    if (dbServerName == null || dbServerName.isEmpty()) {
					error = true;
					msg = msg + " Database Server Name,";
				}
				if (dbType == null || dbType.isEmpty() || dbServerPort == null || dbServerPort.isEmpty()) {
					error = true;
					msg = msg + " DB Engine,";
				}
				if ((dbName == null || dbName.isEmpty()) && !(dbType == null || dbType.isEmpty()) && !dbType.equals(DBConstants.NONSTOPSQL)) {
					error = true;
					msg = msg + " Database Name,";
				}
				if (dbSchema == null || dbSchema.isEmpty()) {
					error = true;
					msg = msg + " Database Schema,";
				}
				if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.NONSTOPSQL))) {
					if (dbCatalog == null || dbCatalog.isEmpty()) {
						error = true;
						msg = msg + " Database Catalog,";
					}
				}
				if (dbUserName == null || dbUserName.isEmpty()) {
					error = true;
					msg = msg + " Username,";
				}
				if (dbPassword == null || dbPassword.isEmpty()) {
					error = true;
					msg = msg + " Password,";
				}
				if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.MSSQL) || dbType.equals(DBConstants.NONSTOPSQL))) {
					if (serverInstance == null || serverInstance.isEmpty()) {
						error = true;
						msg = msg + " Server Instance,";
					}
				}
				if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.NONSTOPSQL))) {
					if (dbVersion == null || dbVersion.isEmpty()) {
						error = true;
						msg = msg + " Version Number,";
					}
				}
				if (error) {
					msg = msg.substring(0, msg.lastIndexOf(","));
					msg = "Entries in the fields: " + msg + " are incorrect.\nPlease fix the configuration file (using GUI) AND/OR  give a scale factor value/ optional table name" +
							"\n\nUsage:  coddCmd - connection_config_string <conection name> -scale_factor <SF no>  -run_stats <false/true> (optional) -table <table_name> (optional) \n";
					System.out.println(msg);
					return;
				}
				
				msg ="Valid configuration passed in command line";
			
			    // dbName =  "" + dbSchema + "" + dbCatalog + "";
			 
			   Constants.CPrintToConsole(msg, Constants.DEBUG_FIRST_LEVEL_Information);				
			 
			
			    DBSettings settings = new DBSettings(dbServerName, dbServerPort, dbType, dbName, dbSchema, dbUserName, dbPassword);
			    if (dbType.equals(DBConstants.MSSQL)) {
					settings.setSqlServerInstanceName(serverInstance);
			    }
			    if (dbType.equals(DBConstants.NONSTOPSQL)) {
					settings.setCatalog(dbCatalog);
					settings.setVersion(dbVersion);
					settings.setSqlServerInstanceName(serverInstance);
			    }
			 
			    Database database = null;
				if (dbType.equals(DBConstants.DB2)) {
					database = (Database) new DB2Database(settings);
				} else if (dbType.equals(DBConstants.MSSQL)) {
					database = (Database) new MSSQLDatabase(settings);
				} else if (dbType.equals(DBConstants.ORACLE)) {
					database = (Database) new OracleDatabase(settings);
				} else if (dbType.equals(DBConstants.POSTGRES)) {
					database = (Database) new PostgresDatabase(settings);
				} else if (dbType.equals(DBConstants.SYBASE)) {
					database = (Database) new SybaseDatabase(settings);
				} else if (dbType.equals(DBConstants.NONSTOPSQL)) {
					database = (Database) new NonStopSQLDatabase(settings);
				}		 
				if (database == null) {
					Constants.CPrintToConsole("Error: Could not get database object", Constants.DEBUG_ZERO_LEVEL_MANDATATORY_Information);					
				} 
				else if (database.connect()) {
					Constants.CPrintToConsole("Connected to database. ", Constants.DEBUG_FIRST_LEVEL_Information);
					if (!database.stopAutoUpdateOfStats()) {
						Constants.CPrintToConsole("Warning: Could not able to stop auto maintanance of statistics.", Constants.DEBUG_FIRST_LEVEL_Information);
						return;
					}					
				} 
				else { 
					System.out.println("Error: Not able to connect to database.");
					return;
				}
				
				// Connection to DB is OK if we reach here ; 
				
				// now try scaling all the relations in the schema if table option is not given; 
				// Assumes Size scaling by default for now ( No support for time scaling )
				
				if (scaleFactor < 1) {
					// We always want this print to come, irrespective of debug levels and hence 1 as the 2nd argument
					System.out.println("Validation Error - Scaling Factor must be greather than 0");
						
				} else {
					
				   new CommandLineSizeScaling (tableName, (int)scaleFactor, runStats, database);
				   
						// No command line support for Time scaling for now..
						// else if (scalingMethod.equals("Time Scaling")) {                  	
						//	new TimeScaling(relations, scale_factor + "", database).setVisible(true);
						
							
				}									 
        
	      System.out.println("CODD in command line mode completed");		
	    } // end of try
	    catch (Exception e) {
	       Constants.CPrintToConsole("Unable to run CODD in command line mode", Constants.DEBUG_FIRST_LEVEL_Information);
	       Constants.CPrintErrToConsole(e);
        }
      } // end of Main
	
    }
