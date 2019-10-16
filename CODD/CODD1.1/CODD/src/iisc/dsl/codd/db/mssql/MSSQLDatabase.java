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
package iisc.dsl.codd.db.mssql;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DatabaseAbstract;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.plan.Node;
import iisc.dsl.codd.plan.Plan;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * Implementation of the functionalities specific to Oracle, required for CODD.
 * @author dsladmin
 */
public class MSSQLDatabase extends DatabaseAbstract {

    /**
     * Stores the list of statistics names of the columns of the relation.
     * HashMap<String, <HashMap<String,String>>
     * <Relation,<column,statsName>>
     */
    HashMap<String,HashMap<String,String>> statsName;

    /**
     * Constructs a MSSQLDatabase instance with default values
     * @param settings Database DBSettings object
     * @throws DatalessException
     */
    public MSSQLDatabase(DBSettings settings) throws DatalessException {
        super(settings);
        statsName = new HashMap();
    }


    @Override
    public String getQuery_stopAutoUpdateStats() {
        return "ALTER DATABASE ["+settings.getDbName()+"] SET AUTO_UPDATE_STATISTICS OFF";
    }

    @Override
    public String getQuery_SelectRelations() {
        // SELECT TABLE_NAME FROM ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[TABLES] WHERE TABLE_SHCEMA='"+getSchema()+"'";
        return "select name from sys.tables";
    }

    @Override
    public String getQuery_dependentRelations(String relation) {
        return "select b.name from sys.foreign_keys a, sys.tables b where a.parent_object_id=b.object_id and a.referenced_object_id=(select c.object_id from sys.tables c where c.name ='"+relation+"')";
    }

    @Override
    public String getQuery_columnDataType(String relation, String column) {
        return "SELECT DATA_TYPE FROM ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[COLUMNS] WHERE TABLE_SCHEMA='"+this.getSchema()+"' AND TABLE_NAME='"+relation+"' AND COLUMN_NAME='"+column+"'";
    }

    @Override
    public String getQuery_getIndexedColumns(String relation) throws DatalessException {
        return "sp_helpindex '"+getSchema()+"."+relation+"'";
    }

    @Override
    public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
        return "sp_helpindex '"+getSchema()+"."+relation+"'";
    }

    @Override
    public String getQuery_getAttributes(String relation) throws DatalessException {
        return "select c.name from sys.columns c, sys.tables t where t.name='"+relation+"' and c.object_id = t.object_id";
    }

    @Override
    public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc, ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu "
                + " WHERE tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND "
                + "tc.TABLE_SCHEMA='"+getSchema()+"' AND tc.TABLE_NAME='"+relation+"' AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'";
    }

    @Override
    public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
        return "SELECT OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,"
                + "COL_NAME(fc.parent_object_id,fc.parent_column_id) AS ColumnName "
                + " FROM sys.foreign_keys AS f "
                    + "INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id "
                    + "INNER JOIN sys.objects AS o ON o.OBJECT_ID = fc.referenced_object_id WHERE OBJECT_NAME(f.parent_object_id) = '"+relation+"'";
    }

    @Override
    public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc, ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu "
                + " WHERE tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND "
                + "tc.TABLE_SCHEMA='"+getSchema()+"' AND tc.TABLE_NAME='"+relation+"' AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'";
    }

    @Override
    public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
        return "SELECT COLUMN_NAME FROM ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc, ["+this.getSettings().getDbName()+"].[INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu "
                + " WHERE tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND "
                + "tc.TABLE_SCHEMA='"+getSchema()+"' AND tc.TABLE_NAME='"+relation+"' AND tc.CONSTRAINT_TYPE = 'UNIQUE'";
    }

    @Override
    public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
        // 0 - NOT NULL, 1 - NULLABLE
        return "select is_nullable from sys.columns c, sys.tables t where t.name='"+relation+"' and c.name='"+column+"' and c.object_id = t.object_id";
    }

    @Override
    public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
       return "SELECT OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,"
                + "COL_NAME(fc.referenced_object_id,fc.referenced_column_id) AS ReferenceColumnName"
                + " FROM sys.foreign_keys AS f "
                    + "INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id "
                    + "INNER JOIN sys.objects AS o ON o.OBJECT_ID = fc.referenced_object_id WHERE OBJECT_NAME(f.parent_object_id) = '"+relation+"'  and COL_NAME(fc.parent_object_id,fc.parent_column_id) ='"+column+"'";
    }

    public boolean connect(DBSettings settings) throws DatalessException{
        String connectString;
        if(isConnected())
            return true;
        this.settings = settings;
        try
        {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
            connectString = "jdbc:sqlserver://" + settings.getServerName() + ":" + settings.getServerPort()
                    + ";databasename=" + settings.getDbName() ;
            //DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
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

    /**
     * Returns the RetainScript with / without drop object script generation.
     * @param folderPath Path where the script will be saved.
     * @param constructScript True, script for Constructing object.
     * @param dropScript True, script for dropping object. Any one of dropScript or constructScript must be true.
     * @param server Sever Instance
     * @param dbname database name
     * @param table relation name
     * @return Returns the RetainScript Function.
     */
    private String getRetainScript(String folderPath, boolean constructScript, boolean dropScript, String server, String dbname, String table) {
        String retainScript =
                //"function global:RetainScript([string]$server, [string]$dbname, [string]$table){ \n"
                "$server = "+server+" \n"
                + "$dbname = "+dbname+" \n"
                + "$table = "+table+" \n"
                + "[System.Reflection.Assembly]::LoadWithPartialName(\"Microsoft.SqlServer.SMO\") \n" // | out-null
                + "$SMOserver = New-Object ('Microsoft.SqlServer.Management.Smo.Server') -argumentlist $server \n"
                + "  $db = $SMOserver.databases[$dbname] \n"
                + "$Objects = $db.Tables \n "
                + "$SavePath = \""+folderPath+Constants.PathSeparator+"\" + $($dbname) + \""+Constants.PathSeparator+"\" \n "
                // #Create Folder, if not exists
		+ "if (!(Test-Path -path $SavePath)) { \n"
                + "     New-Item ($SavePath) -type directory \n"
                + "} \n"
                + " foreach ($ScriptThis in $Objects | where {!($_.IsSystemObject) }) { \n"
                + "$ScriptFile = $ScriptThis -replace \"\\[|\\]\" \n"
                + "if($ScriptFile -eq $table) { \n ";
        if(constructScript) {
            retainScript = retainScript
                + "$scriptr = new-object ('Microsoft.SqlServer.Management.Smo.Scripter') ($SMOserver) \n"
                + "$scriptr.Options.AppendToFile = $False \n"
                + "$scriptr.Options.AllowSystemObjects = $False \n"
                + "$scriptr.Options.ClusteredIndexes = $True \n"
                + "$scriptr.Options.DriAll = $True \n"
                + "$scriptr.Options.DriIncludeSystemNames = $True \n"
                + "$scriptr.Options.ScriptDrops = $False \n"
                + "$scriptr.Options.IncludeHeaders = $True \n"
                + "$scriptr.Options.ToFileOnly = $True \n"
                + "$scriptr.Options.Indexes = $True \n"
                + "$scriptr.Options.Permissions = $True \n"
                + "$scriptr.Options.WithDependencies = $False \n"
                + "$scriptr.Options.Statistics = $True \n"
                + "$scriptr.Options.OptimizerData = $True \n";
        }
        if(dropScript) {
            //"<#Script the Drop too#>
            retainScript = retainScript
                    + "$ScriptDrop = new-object ('Microsoft.SqlServer.Management.Smo.Scripter') ($SMOserver) \n"
                    + "$ScriptDrop.Options.AppendToFile = $False \n"
                    + "$ScriptDrop.Options.AllowSystemObjects = $False \n"
                    + "$ScriptDrop.Options.ClusteredIndexes = $True \n"
                    + "$ScriptDrop.Options.DriAll = $True \n"
                    //+ "$scriptr.Options.DriIncludeSystemNames = $True \n"
                    + "$ScriptDrop.Options.ScriptDrops = $True \n"
                    + "$ScriptDrop.Options.IncludeHeaders = $True \n"
                    + "$ScriptDrop.Options.ToFileOnly = $True \n"
                    + "$ScriptDrop.Options.Indexes = $True \n"
                    + "$ScriptDrop.Options.WithDependencies = $False \n";
        }
        retainScript = retainScript
                + "$TypeFolder=$ScriptThis.GetType().Name \n"
                + "\"Scripting Out \"+$TypeFolder + \" \" + $ScriptThis \n";
        if(dropScript) {
            retainScript = retainScript
                    + "$ScriptDrop.Options.FileName = \"\" + $($SavePath) + $($ScriptFile) + \"-drop.SQL\" \n";
        }
        if(constructScript) {
            retainScript = retainScript
                    + "$scriptr.Options.FileName = \"\" + $($SavePath) + $($ScriptFile) + \"-metadata.SQL\" \n";
        }
                //#This is where each object actually gets scripted one at a time.
        if(dropScript) {
            retainScript = retainScript
                    + "$ScriptDrop.Script($ScriptThis) \n";
        }
        if (constructScript) {
            retainScript = retainScript
                    + "$scriptr.Script($ScriptThis) \n";
        }
        retainScript = retainScript
                + "} \n" //#This ends the if
                + "} \n" //#This ends the loop
                //+ "} " //#This completes the function
                + "\n \n";
        return retainScript;
    }

    // Source: http://social.technet.microsoft.com/Forums/en-US/winserverpowershell/thread/d32537bd-0aef-440e-8760-6b3085390c37
    private String exec(String command) throws Exception {
        StringBuffer sbInput = new StringBuffer();
        StringBuffer sbError = new StringBuffer();

        Runtime runtime = Runtime.getRuntime();
        Process proc = runtime.exec(command);
        proc.getOutputStream().close();
        InputStream inputstream = proc.getInputStream();
        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
        BufferedReader bufferedreader = new BufferedReader(inputstreamreader);

        String line;
        while ((line = bufferedreader.readLine()) != null) {
            sbInput.append(line + "\n");
        }

        inputstream = proc.getErrorStream();
        inputstreamreader = new InputStreamReader(inputstream);
        bufferedreader = new BufferedReader(inputstreamreader);
        while ((line = bufferedreader.readLine()) != null) {
            sbError.append(line + "\n");
        }

        if (sbError.length() > 0) {
            throw new DatalessException("The command [" + command + "] failed to execute!\n\nResult returned:\n" + sbError.toString());
        }
        return "The command [" + command + "] executed successfully!\n\nResult returned:\n" + sbInput.toString();
    }

    /**
	 * Executes a Powershell script.
	 *
	 * @param scriptFilename the filename of the script
	 * @param args any arguments to pass to the script
	 * @return the result as String.
	 * @throws Exception if an error occurs
	 */
	public String executePSScript(String scriptFilename, String args) throws Exception {
		if (!new File(scriptFilename).exists())
			throw new Exception("Script file doesn't exist: " + scriptFilename);

		String cmd = "cmd /c powershell -ExecutionPolicy RemoteSigned -noprofile -noninteractive -file \"" + scriptFilename + "\"";
		if (args != null && args.length() > 0)
			cmd += " " + args;
		return exec(cmd);
	}

    	/**
	 * Executes a Powershell command.
	 *
	 * @param command the command
	 * @return the result as String.
	 * @throws Exception if an error occurs
	 */
    private String executePSCommand(String command) throws Exception {
        String cmd = "cmd /c powershell -ExecutionPolicy RemoteSigned -noprofile -noninteractive " + command;
        return exec(cmd);
    }

    private String executeScript(String fileName) throws Exception {
        String cmd = "cmd /c sqlcmd -S "+this.getSettings().getSqlServerInstanceName()+" -d "+this.getSettings().getDbName()+" -U "+this.getSettings().getUserName()+" -P "+this.getSettings().getPassword()+" -r0 -i \""+fileName+"\" 1> NULL"; //2> \"OUTPUT.LOG\"
        return exec(cmd);
    }

    private String executeScript(String fileName, Database dest) throws Exception {
        String cmd = "cmd /c sqlcmd -S "+dest.getSettings().getSqlServerInstanceName()+" -d "+dest.getSettings().getDbName()+" -U "+dest.getSettings().getUserName()+" -P "+dest.getSettings().getPassword()+" -r0 -i \""+fileName+"\" 1> NULL"; //2> \"OUTPUT.LOG\"
        return exec(cmd);
    }

    private boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }

    @Override
    public boolean retain(String [] dropRelations, String[] dependentRelations) throws DatalessException {

        boolean success = true;
        /*
         * 1) Create a Dependency Graph
         * 2) Topological Sort
         * 3) Bottom Up (reverse) traversal of each relation
         *      3.1) Generate script for create and load relation with statistics
         *      3.2) Drop Relation
         * 4) Top Down traversal of each relation
         *      4.1) Run the script to create metadata-only database
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
        // Create Temporary Folder
        String tempFolder = Constants.getWorkingDirectory()+Constants.PathSeparator+"temp";
        File f = new File(tempFolder);
        if (!f.exists()) {
            f.mkdir();
        } else {
            // Delete Temporary Folder recursively
            deleteDirectory(f);
            f = new File(tempFolder);
            f.mkdir();
        }

        /** Step 3) Top Down traversal of each relation
         *      3.1) Generate script for create and load relation with statistics
         *      3.2) Drop Relation
         *      3.3) Shrink database to reclaim space
         */
        for(int s=0;s<sortedRelations.size();s++) {
            String relation = sortedRelations.get(s);
            try {
                String dbInstance = settings.getSqlServerInstanceName();
                String instance = "\"" + dbInstance + "\"";
                String dbname = "\"" + this.getSettings().getDbName() + "\"";
                String table = "\"" + this.getSchema() + "." + relation + "\"";
                String retainScript = getRetainScript(f.getAbsolutePath(), true, true, instance, dbname, table);
                String retString;
                String script = f.getAbsolutePath() + Constants.PathSeparator + "script.ps1";
                // script file will be overwritten everyrtime
                BufferedWriter writer = new BufferedWriter(new FileWriter(new File(script)));
                writer.write(retainScript);
                writer.close();
                Constants.CPrintToConsole(retainScript, Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole("Executing Retain Script for: "+relation, Constants.DEBUG_SECOND_LEVEL_Information);
                //retString = executePSCommand(retainScript);
                retString = executePSScript(script, null);
                Constants.CPrintToConsole("Return Value: " + retString, Constants.DEBUG_SECOND_LEVEL_Information);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in executing Retain Script.");
            }
        }
        for(int s=0;s<sortedRelations.size();s++) {
            String relation = sortedRelations.get(s);
            // Execute drop script.
            String table = this.getSchema() + "." + relation;
            String fileName = f.getAbsolutePath()+Constants.PathSeparator+this.getSettings().getDbName()+Constants.PathSeparator+table+"-drop.SQL";
            Constants.CPrintToConsole("Dropping Relation for: "+relation+" from "+fileName, Constants.DEBUG_SECOND_LEVEL_Information);
            try {
                /*
                Statement stmt = createStatement();
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.execute(command);
                stmt.close();
                 *
                 */
                String retString = executeScript(fileName);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in dropping relation for " + relation + " of " + settings.getDbName());
            }

        }

        // SHRINK DATABASE
        try {
        String command = "DBCC SHRINKDATABASE (" + settings.getDbName() + ") ";
        Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
        Statement stmt = createStatement();
        stmt.execute(command);
        stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in shrinking database for " + settings.getDbName());
        }

        /** Step 4) Bottom Up (reverse) traversal of each relation
         *      4.1) Run the script to create metadata-only database
         */
        for(int s=sortedRelations.size()-1;s>=0;s--) {
            String relation = sortedRelations.get(s);
            String table = this.getSchema() + "." + relation;
            String fileName = f.getAbsolutePath()+Constants.PathSeparator+this.getSettings().getDbName()+Constants.PathSeparator+table+"-metadata.SQL";
            Constants.CPrintToConsole("Recreating Schema and Statistics of Relation : "+relation+" from "+fileName, Constants.DEBUG_SECOND_LEVEL_Information);
            try {
                /*
                String command = allCommands.toString();
                Statement stmt = createStatement();
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                stmt.execute(command);
                stmt.close();
                */
                String retString = executeScript(fileName);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in Recreating schema and Statistics for relation " + relation + " of " + settings.getDbName());
            }
        }

        // Delete Temporary Folder recursively
        if (f.exists()) {
            deleteDirectory(f);
        }
        return success;

    }

    @Override
    public boolean transfer(String[] relations, Database dest) throws DatalessException {
        /**
         * 1) Topological Sort the relations.
         * 2) Generate construct script for source table relations
         * 3) Generate drop script for destination.
         * 4) Construct for destination using source metadata.
         */
        boolean success = true;
        ArrayList<String> sortedRelations = getTopologicalSortedRelations(relations);
        if(sortedRelations == null) {
            return false;
        }
        // Create Temporary Folder
        String tempFolder = Constants.getWorkingDirectory()+Constants.PathSeparator+"temp";
        File f = new File(tempFolder);
        if (!f.exists()) {
            f.mkdir();
        } else {
            // Delete Temporary Folder recursively
            deleteDirectory(f);
            f = new File(tempFolder);
            f.mkdir();
        }

        /**  Top Down traversal of each relation
         *   Generate script for create and load relation with statistics
         */
        for(int s=0;s<sortedRelations.size();s++) {
            String relation = sortedRelations.get(s);
            try {
                String dbInstance = settings.getSqlServerInstanceName();
                String instance = "\"" + dbInstance + "\"";
                String dbname = "\"" + this.getSettings().getDbName() + "\"";
                String table = "\"" + this.getSchema() + "." + relation + "\"";
                String retainScript = getRetainScript(f.getAbsolutePath(), true, false, instance, dbname, table);
                String retString;
                String script = f.getAbsolutePath() + Constants.PathSeparator + "script.ps1";
                // script file will be overwritten everyrtime
                BufferedWriter writer = new BufferedWriter(new FileWriter(new File(script)));
                writer.write(retainScript);
                writer.close();
                Constants.CPrintToConsole(retainScript, Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole("Executing Construct Script for: "+relation, Constants.DEBUG_SECOND_LEVEL_Information);
                //retString = executePSCommand(retainScript);
                retString = executePSScript(script, null);
                Constants.CPrintToConsole("Return Value: " + retString, Constants.DEBUG_SECOND_LEVEL_Information);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in executing Construct Script.");
            }
        }
        /**
         * Generate Drop script for destination and execute it.
         */
        for(int s=0;s<sortedRelations.size();s++) {
            String relation = sortedRelations.get(s);
            // Execute drop script.
            try {
                String dbInstance = dest.getSettings().getSqlServerInstanceName();
                String instance = "\"" + dbInstance + "\"";
                String dbname = "\"" + dest.getSettings().getDbName() + "\"";
                String table = "\"" + dest.getSchema() + "." + relation + "\"";
                String retainScript = getRetainScript(f.getAbsolutePath(), false, true, instance, dbname, table);
                String retString;
                String script = f.getAbsolutePath() + Constants.PathSeparator + "script.ps1";
                // script file will be overwritten everyrtime
                BufferedWriter writer = new BufferedWriter(new FileWriter(new File(script)));
                writer.write(retainScript);
                writer.close();
                Constants.CPrintToConsole(retainScript, Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole("Executing Drop Script for: "+relation, Constants.DEBUG_SECOND_LEVEL_Information);
                //retString = executePSCommand(retainScript);
                retString = executePSScript(script, null);
                Constants.CPrintToConsole("Return Value: " + retString, Constants.DEBUG_SECOND_LEVEL_Information);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in executing Drop Script.");
            }
            String table;
            String fileName = null;
            table = dest.getSchema() + "." + relation;
            fileName = f.getAbsolutePath()+Constants.PathSeparator+dest.getSettings().getDbName()+Constants.PathSeparator+table+"-drop.SQL";
            Constants.CPrintToConsole("Dropping Relation for: "+relation+" from "+fileName, Constants.DEBUG_SECOND_LEVEL_Information);
            try {
                String retString = executeScript(fileName, dest);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in dropping relation for " + relation + " of " + dest.getSettings().getDbName());
            }

        }

        /** Step 4) Bottom Up (reverse) traversal of each relation
         *      4.1) Run the script to create metadata-only database on dest
         * destination and source schema must be same. Index Name must be same, statistics name must be same.
         */
        for(int s=sortedRelations.size()-1;s>=0;s--) {
            String relation = sortedRelations.get(s);
            String table = this.getSchema() + "." + relation;
            String fileName = f.getAbsolutePath()+Constants.PathSeparator+this.getSettings().getDbName()+Constants.PathSeparator+table+"-metadata.SQL";
            Constants.CPrintToConsole("Recreating Schema and Statistics of Relation : "+relation+" from "+fileName, Constants.DEBUG_SECOND_LEVEL_Information);
            try {
                String retString = executeScript(fileName, dest);
            } catch (Exception e) {
                Constants.CPrintErrToConsole(e);
                throw new DatalessException("Exception in Recreating schema and Statistics for relation " + relation + " of " + settings.getDbName());
            }
        }

        // Delete Temporary Folder recursively
        if (f.exists()) {
            deleteDirectory(f);
        }
        return success;
    }

    @Override
    public void construct(String[] relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void collectStatistics(String relation) throws DatalessException {
        try {
            Statement stmt = createStatement();
            String command = "UPDATE STATISTICS " + getSchema() + "." + relation + " WITH FULLSCAN, NORECOMPUTE";
            Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
            stmt.execute(command);
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in updating Statistics for " + relation + " of " + settings.getDbName());
        }
    }

    private void getStatisticsName(String relation) throws DatalessException
    {
        try {
            String query = "sp_helpstats "
                + "@objname='"+getSchema()+"."+relation+"',"
                + "@results='ALL'";
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            HashMap<String, String> relStatsName = new HashMap();
            Statement stmt =createStatement();
            ResultSet rset = stmt.executeQuery(query);
            while(rset.next()) {
                String statsName = rset.getString(1);
                String colName = rset.getString(2);
                relStatsName.put(colName, statsName);
            }
            this.statsName.put(relation, relStatsName);
            rset.close();
            stmt.close();
        }
        catch(Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in reading Column Statistics of relation "+relation+" of "+settings.getDbName());
        }
    }

    @Override
    public RelationStatistics getRelationStatistics(String relation) throws DatalessException {
        MSSQLRelationStatistics relStat = new MSSQLRelationStatistics(relation, this.getSchema());

        String command = "SELECT SUM(pa.rows) RowCnt FROM sys.tables ta INNER JOIN sys.partitions pa ON pa.OBJECT_ID = ta.OBJECT_ID INNER JOIN sys.schemas sc ON ta.schema_id = sc.schema_id"
                + " WHERE ta.is_ms_shipped = 0 AND pa.index_id IN (1,0) AND ta.name = '"+relation+"' AND sc.name = '"+getSchema()+"'";
        Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
        try
        {
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(command);
            while (rset.next()) {
                BigDecimal card = new BigDecimal(rset.getString(1));
                relStat.setCardinality(card);
                Constants.CPrintToConsole("card", Constants.DEBUG_SECOND_LEVEL_Information);
                Constants.CPrintToConsole(card+"", Constants.DEBUG_SECOND_LEVEL_Information);
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
        MSSQLColumnStatistics colStat = null;
        try {
            colStat = new MSSQLColumnStatistics(relation, column, type, getConstraint(relation, column));
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception intialzeing Column Statisitcs " + column + " (" + relation + ")  of " + settings.getDbName());
        }

        try {

            if(!this.statsName.containsKey(relation)) {
                this.getStatisticsName(relation);
            }
            String colStatsName = this.statsName.get(relation).get(column);
            if(colStatsName != null)
            {
                // Read Histogram statistics and fill QunatileHistogram
                /**
                 * Column Statistics from SYSSTAT.COLDIST 'Quantile Histogram'
                 *
                 * has to store 4 values. SEQNO, COLVALUE, VALCOUNT, DISTCOUNT
                 * (Sequence Number, Column Value, Frequency, Distinct Values Count [less than or equal COLVALUE])
                 */
                String command = "DBCC SHOW_STATISTICS("+relation+","+colStatsName+") WITH HISTOGRAM";
                Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
                Statement stmt = createStatement();
                ResultSet rset = stmt.executeQuery(command);
                TreeMap<Integer, HistogramObject> map = new TreeMap();
                Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT|distCount", Constants.DEBUG_SECOND_LEVEL_Information);
                int count = 0;
                Double sumOfDistCount = 0.0;
                while (rset.next()) {
                    String col = rset.getString(1);
                    Double rangeRows = rset.getDouble(2);
                    Double eqRows = rset.getDouble(3);
                    Double distinctRangeRows = rset.getDouble(4);

                    Integer seqno = new Integer(count+1); // Seq no starts from 1
                    Double valCount = new Double(rangeRows+eqRows);
                    Double distCount = new Double(distinctRangeRows+1);
                    sumOfDistCount = sumOfDistCount + distCount;

                    Constants.CPrintToConsole(seqno + "|" + col + "|" + valCount + "| " + distCount, Constants.DEBUG_SECOND_LEVEL_Information);
                    // Store as Double value. For DB2, the value must be integer, other engines, it could be double too.
                    HistogramObject histogramObject;
                    histogramObject = new HistogramObject(col, valCount, distCount);
                    map.put(seqno, histogramObject);
                    count++;
                }
                rset.close();
                stmt.close();
                if(count == 0) //Histogram was not collected.
                    map = null;
                colStat.setQuantileHistogram(map);
                BigDecimal colCardDecimal = new BigDecimal(sumOfDistCount);
                BigDecimal colCard = colCardDecimal;
                Constants.CPrintToConsole("Distinct Count: "+colCard, Constants.DEBUG_SECOND_LEVEL_Information);
                colStat.setColCard(colCard);
            }
        }
        catch(Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in reading Column Statistics of relation "+relation+" of "+settings.getDbName());
        }
        return colStat;
    }

    @Override
    public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {
        MSSQLIndexStatistics indexStat = new MSSQLIndexStatistics(relation, colNames);
        String indexName = getIndexName(relation, colNames);
        // indexName shouldn't be null
        /*
        String command = "SELECT INDCARD, NLEAF, NLEVELS, DENSITY, NUMRIDS, CLUSTERFACTOR, NUM_EMPTY_LEAFS  FROM SYSSTAT.INDEXES "+
                                " WHERE INDNAME = '"+indexName+"'AND TABNAME = '"+relation+"' AND TABSCHEMA = '"+this.getSchema()+"'";
        Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);

        try
        {
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(command);
            while (rset.next()) {
                Integer INDCARD = new Integer(rset.getInt(1));

                indexStat.setIndCard(INDCARD);

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
         *
         */
        return indexStat;
    }

    @Override
    public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String processArgValue(String string) {
        return string.replaceAll("Expr[\\d]+", "Expr");
    }

    @Override
    public Plan getPlan(String query) throws DatalessException {
        Plan plan = new Plan();
        Node node;
        int curNode = 0;
        String objectName, argument;
        try {
            Statement stmt = createStatement();
            stmt.execute("DBCC FREEPROCCACHE");
            stmt.execute("set showplan_all on");
            ResultSet rset = stmt.executeQuery(query);
            while (rset.next()) {
                node = new Node();

//				Format:
//				1) StmtText, 2) StmtId, 3) NodeId, 4) Parent, 5) PhysicalOp, 6) LogicalOp, 7) Argument, 8) DefinedValues, + 10 more columns
//				StmtText is split into columns 5,6,7.
//				StmtId is 1 (or whatever set, I think).
//				PhysicalOp -> NULL for top, else Hash Match, etc.
//				LogicalOp -> Same as PhysicalOp or Inner Join etc.
//				Argument -> Key:Value
//				DefinedValues -> Lots of column names or null.
//				Column 16 is Type which is either SELECT or PLAN_ROW

                node.setId(rset.getInt(3));
                node.setParentId(rset.getInt(4));
                node.setName(rset.getString(5));

                if (node.getName().startsWith("Hash Match")) {
                    if (rset.getString(6).equals("Aggregate") || rset.getString(6).equals("Partial Aggregate")) {
                        node.setName(rset.getString(5) + " - " + rset.getString(6));
                    }
                }

                if ((rset.getString(16)).equals("SELECT")) {
                    node.setName("SELECT STATEMENT");
                }
                node.setCost(rset.getDouble(13));
                node.setCard(rset.getDouble("EstimateRows"));
                argument = rset.getString(7);
                if (rset.getString(6) != null) {
                    node.addArgType("Logical-Op");
                    //node.addArgValue(rset.getString(6));
                    // Output List
                    node.addArgValue(processArgValue(rset.getString(6)));
                }
                if (rset.getString(14) != null) { // OutputList
                    node.addArgType("Column List");

                    node.addArgValue(processArgValue(rset.getString(14)));
                }
                addArgument(node, argument);
                objectName = processObjectName(rset.getString(7));
                plan.setNode(node, curNode);
                curNode++;
                if (!objectName.equals("")) {
                    node = new Node();
                    node.setId(-1);
                    node.setParentId(rset.getInt(3));
                    node.setName(objectName);
                    node.setCost(rset.getDouble(13));
                    node.setCard(rset.getDouble("EstimateRows"));
                    plan.setNode(node, curNode);
                    curNode++;
                }
            }
            rset.close();
            stmt.execute("set showplan_all off");
            stmt.close();
        } catch (SQLException e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error explaining query: " + e);
        }
        return plan;
    }

    private String removeBrackets(String str1) {
        int oitmp = 0, itmp = 0, jtmp;
        String str = str1;
        while (true) {
            oitmp = itmp;
            itmp = str.indexOf("<=", itmp + 2);
            if (itmp == -1) {
                break;
            }
            jtmp = itmp;
            while (jtmp > 0 && str.charAt(jtmp) != ' ' && str.charAt(jtmp) != '[') {
                jtmp--;
            }
            //while(str.charAt(jtmp--) != '[');
            //jtmp++;
            String tmp = str.substring(jtmp, itmp);
            //if (isPicassoPredicate(tmp)) {
            // TODO
            if (false) {
                boolean str_date_check = false;
                int k = str.indexOf(" ", itmp + 2);
                int k1 = str.indexOf(")", itmp + 2);
                if ((k1 != -1 && k1 < k) || k == -1) {
                    k = k1;
                }
                tmp = str.substring(k, str.length());
                if (str.substring(itmp + 1, k).indexOf("'") != -1) {
                    tmp = tmp.substring(tmp.indexOf("'") + 1, tmp.length());
                };
                str = str.substring(0, itmp) + ":VARIES" + tmp;
            }
        }
        return str;
    }

    private void addArgument(Node node, String argument) {
        String type, value;
        if (argument != null) {
            String sp[] = argument.split(",[ ]+");
            int i = 0;
            while (i < sp.length) {
                String tmp = sp[i];
                i++;
                if (tmp.indexOf(':') < 0) {
                    continue;
                }
                type = tmp.substring(0, tmp.indexOf(':'));
                value = tmp.substring(tmp.indexOf(':') + 1, tmp.length());
                while (i < sp.length && sp[i].indexOf(':') < 0) {
                    value = value + sp[i];
                    i++;
                }
                if (type.equals("WHERE")) {
                    value = removeBrackets(value);
                } else if (type.equals("DEFINE")) {
                    value = ""; //(for now)
                }
                node.addArgType(type);
                node.addArgValue(value);
            }
        }
    }

    private String processObjectName(String objectName)
    {
        StringTokenizer st = null;
        if (objectName != null) {
            st = new StringTokenizer(objectName, ", ");
            if (st.hasMoreTokens()) {
                objectName = st.nextToken();
            } else {
                objectName = "";
            }
        } else {
            objectName = "";
        }
        //Removing unwanted parameters in object_name ie) [master].[dbo]
        if (objectName.startsWith("OBJECT")) {
            //  Format of a typical object
            //	OBJECT:([master].[dbo].[NATION].[PK__NATION__4BB72C21])
            //	OBJECT:([tpch].[dbo].[NATION].[PK__NATION__4BB72C21])
            int index = objectName.indexOf('[');
            index = objectName.indexOf('[', index + 1);
            index = objectName.indexOf('[', index + 1);
            objectName = objectName.substring(index + 1, objectName.length());
            index = objectName.indexOf(']');
            objectName = objectName.substring(0, index);
        } else {
            objectName = "";
        }
        return objectName;
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


	@Override
	public boolean setHardwareStatistics() throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}