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
package iisc.dsl.codd.db;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.db2.DB2Database;
import iisc.dsl.codd.db.mssql.MSSQLDatabase;
import iisc.dsl.codd.db.nonstopsql.NonStopSQLDatabase;
import iisc.dsl.codd.db.oracle.OracleDatabase;
import iisc.dsl.codd.db.postgres.PostgresDatabase;
import iisc.dsl.codd.db.sybase.SybaseDatabase;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.ds.Statistics;
import iisc.dsl.codd.plan.Plan;

import java.math.BigDecimal;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.swing.JOptionPane;

/**
 * Database is a abstract class, which specifies the methods to be implemented by the subclass.
 * Apart from that, it also has few functions implemented here.
 * In order to port CODD to another Database, one has to create another class extending the Database class.
 * @author dsladmin
 */
public abstract class DatabaseAbstract implements Database {

    protected Connection con; // Database Connection
    protected DBSettings settings; // Database Connection Information

    /**
     * Constructor for Database Class.
     * @param settings Connection information through DBSettings object
     * @throws DatalessException
     */
    public DatabaseAbstract(DBSettings settings) throws DatalessException {
        this.settings = settings;
        connect(settings);
    }

    /**
     * Connect to the database with available database connection (attribute settings) parameters.
     * @return
     * @throws DatalessException
     */
    @Override
    public boolean connect() throws DatalessException {
        return connect(settings);
    }

    /**
     * Returns the query to Stop Automatic Update of Statistics.
     * @return the query to Stop Automatic Update of Statistics
     */
    @Override
    abstract public String getQuery_stopAutoUpdateStats();

    /**
     * Returns the query to get the relations of the user.
     * @return the query to get the relations of the user
     */
    abstract public String getQuery_SelectRelations();

    /**
     * Returns the query to get the dependent relations (recursively) of the specified relation.
     * @return the query to get the dependent relations (recursively) of the specified relation
     */
    abstract public String getQuery_dependentRelations(String relation);

    /**
     * Returns the query to get the datatype of the column of relation.
     * @return the query to get the datatype of the column of relation
     */
    abstract public String getQuery_columnDataType(String relation, String column);

    /**
     * Returns the query to get the indexed columns of the specified relation.
     * @return the query to get the indexed columns of the specified relation
     */
    @Override
    abstract public String getQuery_getIndexedColumns(String relation) throws DatalessException;

    /**
     * Returns the query to get the index name of the specified relation built on the columns specified by cols.
     * @return the query to get the index name of the specified relation built on the columns
     */
    @Override
    abstract public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException;

    /**
     * Returns the query to get the attributes of the specified relation.
     * @return the query to get the attributes of the specified relation
     */
    @Override
    abstract public String getQuery_getAttributes(String relation) throws DatalessException;

    /**
     * Returns the query to get the primary key attributes of the specified relation.
     * @return the query to get the primary key attributes of the specified relation
     */
    @Override
    abstract public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException;
    // Map <FKColumnName, PKRelaionName>

    /**
     * Returns the query to get the Foreign key columns of the specified relation and the corresponding Primary Key relations.
     * @return Foreign Key Columns and the corresponding Primary Key relations
     */
    @Override
    abstract public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException;

    /**
     * Returns the query to get the foreign key attributes of the specified relation.
     * @return the query to get the foreign key attributes of the specified relation
     */
    @Override
    abstract public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException;

    /**
     * Returns the query to get the unique attributes of the specified relation.
     * @return the query to get the unique attributes of the specified relation
     */
    @Override
    abstract public String getQuery_getUniqueAttributes(String relation) throws DatalessException;

    /**
     * Returns the query to get the nullable attributes of the specified relation.
     * @return the query to get the nullable attributes of the specified relation
     */
    @Override
    abstract public String getQuery_getNullableValue(String relation, String column) throws DatalessException;

    /**
     * Returns the query to get the Primary Key Relation and the Primary Key Column of the specified Foreign Key column of relation.
     * @return the query to get the Primary Key Relation and the Primary Key Column
     */
    @Override
    abstract public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException;

    @Override
    abstract public String getMultiColumnHistogramId(String relation) throws DatalessException;

    @Override
    abstract public String getColumnHistogramId(String relation, String column) throws DatalessException;

    /**
     * Connects to the database with the specified Database connection parameters.
     * @param settings Connection Parameters as Settings object
     * @return true, if connection is successful,
     *         false, otherwise.
     * @throws DatalessException
     */
    @Override
    abstract public boolean connect(DBSettings settings) throws DatalessException;

    /**
     * Implements the RetainMode.
     * dropRelations, dependentRelations data is deleted and only the Metadata is kept.
     * @param dropRelations Relations to retain
     * @param dependentRelations Dependent relations of dropRelations
     * @return true on success
     * @throws DatalessException
     */
    @Override
    abstract public boolean retain(String[] dropRelations, String[] dependentRelations) throws DatalessException;

    /**
     * Implemented only for MSSQL Server. Inter Engine Transfer. It transfers the relation statistics to destination.
     * @param relation set of relations.
     * @param destDatabase destination database.
     * @return true, successful. false, otherwise.
     * @throws DatalessException
     */
    @Override
    abstract public boolean transfer(String[] relation, Database destDatabase) throws DatalessException;

    @Override
    abstract public void construct(String[] relation) throws DatalessException;

    /**
     * Collect statistics on the specified relation.
     * @param relation Relation name
     * @throws DatalessException
     */
    @Override
    abstract public void collectStatistics(String relation) throws DatalessException;

    /**
     * Returns the RelationStatistics for the specified relation.
     * @param relation Relation name
     * @return RelationStatistics
     * @throws DatalessException
     */
    @Override
    abstract public RelationStatistics getRelationStatistics(String relation) throws DatalessException;

    /**
     * Returns the ColumnStatistics for the specified column.
     * Integer tableCard used in Oracle to determine the height of the bucket for Height balanced histogram.
     * @param relation Relation name
     * @param column Column name
     * @param tableCard Table Cardinality
     * @return ColumnStatisitcs
     * @throws DatalessException
     */
    @Override
    abstract public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard) throws DatalessException;

    /**
     * Returns the IndexStatistics for the specified indexed column names.
     * @param relation Relation name
     * @param colNames Indexed column names in an ArrayList
     * @return IndexStatistics
     * @throws DatalessException
     */
    @Override
    abstract public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException;

    @Override
    abstract public String[] getMultiColumnAttributes(String relation) throws DatalessException;

    /**
     * Updates the database catalog with the specified Relation Statistics.
     * @param relation Relation name
     * @param relationStatistics RelationStatistics
     * @return true on success
     * @throws DatalessException
     */
    @Override
    abstract public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException;

    /**
     * Updates the database catalog with the specified Column Statistics.
     * Integer tableCard used in DB2, in case inter-engine transfer. Used to align the histogram from double / to 20 buckets
     * @param relation Relation name
     * @param column Column name ColumnStatistics
     * @param columnStatistics
     * @param tableCard Table Cardinality
     * @return true on success
     * @throws DatalessException
     */
    @Override
    abstract public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException;

    /**
     * Updates the database catalog with the specified Index Statistics.
     * @param relation Relation name
     * @param colNames Indexed column names in an ArrayList
     * @param indexStatistics IndexStatistics
     * @return true on success
     * @throws DatalessException
     */
    @Override
    abstract public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException;

    /**
     * Returns the Query Execution Plan Tree for the specified query.
     * For all the subclasses, this function is copied from Picasso code, with few modifications.
     * The modifications include removing isPicassoPredicate function and cost to CPU cost.
     * Except for PostgreSQL, we use CPU cost. Since PostgreSQL does not provide we use Total cost of the operator.
     * @param query Query
     * @return Plan
     * @throws DatalessException
     */
    @Override
    abstract public Plan getPlan(String query) throws DatalessException;

    @Override
    public String getSchema() {
        return settings.getSchema();
    }

    public String getCatalog() {
        return settings.getCatalog();
    }

    public String getVersion() {
        return settings.getVersion();
    }

    public String getServerInstanceName() {
        return settings.getSqlServerInstanceName();
    }

    @Override
    public boolean isConnected() {
        return con != null;
    }

    @Override
    public boolean reSetConnection() throws DatalessException {
        if (isConnected()) {
            close();
        }
        return connect();
    }

    @Override
    public DBSettings getSettings() {
        return settings;
    }

    @Override
    public boolean commit() {
        try {
            con.commit();
        } catch (SQLException e) {
            new DatalessException(" Exception in commit. Database.java");
            return false;
        }
        return true;
    }

    @Override
    public boolean rollback() {
        try {
            con.rollback();
        } catch (SQLException e) {
            new DatalessException(" Exception in Rollback. Database.java");
            return false;
        }
        return true;
    }
    
    @Override
    public boolean close() {
        if (isConnected() == false) {
            return true;
        }
        try {
            con.close();
        } catch (SQLException e) {
            new DatalessException(" Exception in closing the connection. Database.java");
            con = null;
            return false;
        }
        con = null;
        return true;
    }

    @Override
    public Statement createStatement() {
        try {
            return con.createStatement();
        } catch (SQLException e) {
            new DatalessException(" Exception in creating a statement. Database.java");
            return null;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String temp) {
        try {
            return con.prepareStatement(temp);
        } catch (SQLException e) {
            new DatalessException(" Exception in creating a statement. Database.java");
            return null;
        }

    }

    /**
     * Constructs a Database instance based on the connection information.
     * For supporting a new Database system add the new database name in the if.else ladder and
     * instantiate the corresponding Database class.
     * @param settings DBSettings object
     * @return Database object
     * @throws DatalessException
     */
    public Database getDatabase(DBSettings settings) throws DatalessException {
        DatabaseAbstract db = null;
        String vendor = settings.getDbVendor();
        if (vendor.equals(DBConstants.DB2)) {
            db = new DB2Database(settings);
        } else if (vendor.equals(DBConstants.ORACLE)) {
            db = new OracleDatabase(settings);
        } else if (vendor.equals(DBConstants.MSSQL)) {
            db = new MSSQLDatabase(settings);
        } else if (vendor.equals(DBConstants.SYBASE)) {
            db = new SybaseDatabase(settings);
        } else if (vendor.equals(DBConstants.POSTGRES)) {
            db = new PostgresDatabase(settings);
        }
        /*
        else if(vendor.equals(DBConstants.INFORMIX))
        db = new InformixDatabase(settings);
         *
         */
        return db;
    }

    /**
     * Stop Automatic Update of Statistics.
     * @return true on success
     * @throws DatalessException
     */
    @Override
    public boolean stopAutoUpdateOfStats() throws DatalessException {
        try {
            String query = this.getQuery_stopAutoUpdateStats();
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            if (query != null && !query.trim().isEmpty()) {
                Statement stmt = createStatement();
                stmt.executeUpdate(query);
                stmt.close();
            }
            return true;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Unable to stop automatic collection of statistics for " + settings.getDbName());
        }
    }

    /**
     * Returns the relations belong to the connected database user.
     * @return relations
     * @throws DatalessException
     */
    @Override
    public ArrayList<String> getRelations() throws DatalessException {
        if (!isConnected()) {
            connect();
        }
        ArrayList<String> relations = new ArrayList<String>();
        try {
            String query = this.getQuery_SelectRelations();
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(query);
            while (rset.next()) {
                relations.add(rset.getString(1).trim().toUpperCase());
                Constants.CPrintToConsole(rset.getString(1).trim(), Constants.DEBUG_SECOND_LEVEL_Information);
            }
            rset.close();
            stmt.close();
        } catch (Exception e) {
            // Constants.CPrintErrToConsole(e);
            throw new DatalessException("Could not able to retrive list of relations for " + settings.getDbName());
        }
        if(relations.size()==0){
        	throw new DatalessException("Could not able to retrive list of relations for " + settings.getDbName()+". \nMake sure that the input schema is correct and there exist relations in input schema.");
        }
        return relations;
    }

    /**
     * Given PK relations, returns the FK relations.
     * It is used only by the drop mode, to figure out the dependent relations recursively.
     * @param relations Set of PK relations
     * @return ArrayList of dependent relations excluding the given one.
     * @throws DatalessException
     */
    @Override
    public ArrayList<String> getDependentRelations(ArrayList<String> relations) throws DatalessException {
        if (!isConnected()) {
            connect();
        }
        ArrayList<String> newRelations = new ArrayList<String>();
        HashSet<String> dependentRelations = new HashSet<String>();
        int k;
        try {

            for (k = 0; k < relations.size(); k++) {
                ArrayList<String> depenRels = getDependentRelations(relations.get(k));
                for (int r = 0; r < depenRels.size(); r++) {
                    String dependentRelation = depenRels.get(r);
                    // depRel is not in inputList and not in newRel then, add it to newRel to find dep recursively.
                    // if depRel is in inputList, dep will be find subsequently
                    // 2nd to have unique elements.
                    if (!relations.contains(dependentRelation) && !newRelations.contains(dependentRelation)) {
                        newRelations.add(dependentRelation); // have to find dependency recursively.
                        dependentRelations.add(dependentRelation); // add it to depRelList
                    }
                }
            }
            if (newRelations.size() > 0) {
                // add recursive depRel only if not in relations list
                ArrayList<String> recursiveDepRels = getDependentRelations(newRelations);
                Iterator<String> iter = recursiveDepRels.iterator();
                while (iter.hasNext()) {
                    String depRel = (String) iter.next();
                    if (!relations.contains(depRel) && !dependentRelations.contains(depRel)) {
                        dependentRelations.add(depRel);
                    }
                }
            }
            ArrayList<String> depRels = new ArrayList<String>();
            Iterator<String> iter = dependentRelations.iterator();
            while (iter.hasNext()) {
                depRels.add((String) iter.next());
            }
            return depRels;
        } catch (Exception e) {
            // Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in finding Dependent Relations of " + settings.getDbName());
        }
    }

    /**
     * Given a relation (PK), identifies all its dependent relations (FK's).
     * For eg, TPC-H, Region -> Nation
     *                Part -> Partsupp, Lineitem (recursive if FK is itself a part of PK)
     * This will used by getDependentRelations(ArrayList) in retain mode and
     * at cost scaling, MSSQL Retain mode to get dependency graph.
     * @param relation Relation name
     * @return ArrayList of dependent relations
     * @throws DatalessException
     */
    @Override
    public ArrayList<String> getDependentRelations(String relation) throws DatalessException {
        if (!isConnected()) {
            connect();
        }
        try {
            String command = this.getQuery_dependentRelations(relation);
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(command);
            ArrayList<String> rels = new ArrayList<String>();
            while (rset.next()) {
                String dependentRelation = null;
                if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                    String fkRelationId = rset.getString(1).trim();
                    // Get PK Relation Name
                    Statement stmt1 = createStatement();
                    String query1 = "select relname from pg_class where pg_class.oid=" + fkRelationId;
                    ResultSet rset1 = stmt1.executeQuery(query1);
                    while (rset1.next()) {
                        dependentRelation = rset1.getString(1).trim().toUpperCase();
                    }
                    rset1.close();
                    stmt1.close();
                } else {
                    dependentRelation = rset.getString(1).trim();
                }
                rels.add(dependentRelation);
                // Check for recursively referenced E.g Part - Partsupp - Lineitem
                // <FKColumnName, RefPKRelationName>
                TreeMap<String, String> FKColRefRel = (TreeMap<String, String>) this.getFKColumnRefRelation(dependentRelation);
                String[] pkKeys = this.getPrimaryKeyAttributes(dependentRelation);
                for (int p = 0; pkKeys != null && p < pkKeys.length; p++) {
                    String column = pkKeys[p];
                    // Is column a foreign key to other relation and the other relation is dependentRelation, then do find recursive depRels.
                    if (FKColRefRel.containsKey(column) && relation.equalsIgnoreCase(FKColRefRel.get(column))) {
                        // relation FK is part of PK in dependentRelation. Find recursively depRels
                        ArrayList<String> depDepRels = this.getDependentRelations(dependentRelation);
                        for (int dd = 0; dd < depDepRels.size(); dd++) {
                            String rel = depDepRels.get(dd);
                            if (!rels.contains(rel)) {
                                rels.add(rel);
                            }
                        }
                    }
                }
            }
            rset.close();
            stmt.close();
            return rels;
        } catch (Exception e) {
            // Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in finding Dependent Relations of " + settings.getDbName());
        }


    }

    /**
     * Returns the Datatype of the column.
     * @param relation Relation name
     * @param column Column name
     * @return Data type in String format
     */
    @Override
    public String getType(String relation, String column) throws DatalessException {
        String type = "";

        /*
         * If the column is '|' separated strings.. then find inividual types for them and concatinate them .... This is handled for multicolumn histograms
         */
        try {
            ResultSet rset = null;
            Statement stmt = null;

            StringTokenizer st = new StringTokenizer(column, "|");

            while (st.hasMoreTokens()) {
                stmt = createStatement();
                String query = getQuery_columnDataType(relation, st.nextToken());
                rset = stmt.executeQuery(query);
                while (rset.next()) {
                    type = type + rset.getString(1).trim() + "|";
                }
            }

            if (type.contains("|")) {
                type = type.substring(0, type.length() - 1);
            }

            rset.close();
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in getting data type for the Column " + column + " (" + relation + ") from Statistics of " + settings.getDbName());
        }
        return type;
    }

    /**
     * Returns the list of indexes available on this relation.
     * Return <Index Name, Indexed Columns (col1+col2+col3..)>
     * @param relation Relation name
     * @return Mapping of Index name to the Indexed Columns
     * @throws DatalessException
     */
    @Override
    public Map<String, String> getIndexedColumns(String relation) throws DatalessException {
        Map<String, String> map = new HashMap<String, String>();
        try {
            Statement stmt = createStatement();
            String query = getQuery_getIndexedColumns(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            ResultSet rset;
            if (this instanceof MSSQLDatabase) {
                boolean results = stmt.execute(query);
                if (results) {
                    rset = stmt.getResultSet();
                } else {
                    return map;
                }
            } else {
                rset = stmt.executeQuery(query);
            }
            while (rset.next()) {
                String ind_name;
                String ind_attrib;
                if (this.getSettings().getDbVendor().equals(DBConstants.MSSQL)) {
                    ind_name = rset.getString(1).trim();
                    ind_attrib = rset.getString(3).trim();
                    if (ind_attrib.contains(",")) { // multiplekeys
                        String[] col = ind_attrib.split(Pattern.quote(", "));
                        ind_attrib = new String();
                        for (int c = 0; c < col.length - 1; c++) {
                            ind_attrib = ind_attrib + col[c] + "+";
                        }
                        ind_attrib = ind_attrib + col[col.length - 1];
                    }
                    map.put(ind_name, ind_attrib);
                } else if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                    String indrelid = rset.getString(1).trim();
                    String indkey = rset.getString(2).trim();
                    ind_name = null;
                    // Get Index Name
                    Statement stmt1 = createStatement();
                    String query1 = "select relname from pg_class where pg_class.oid=" + indrelid;
                    ResultSet rset1 = stmt1.executeQuery(query1);
                    while (rset1.next()) {
                        ind_name = rset1.getString(1).trim();
                    }
                    rset1.close();
                    stmt1.close();

                    // Get Indexed attributes
                    String[] keys = indkey.split(" ");
                    ind_attrib = new String();
                    for (int k = 0; k < keys.length; k++) {
                        Statement stmt2 = createStatement();
                        String query2 = "select attname from  pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attnum = " + keys[k];
                        ResultSet rset2 = stmt2.executeQuery(query2);
                        String attrib = null;
                        while (rset2.next()) {
                            attrib = rset2.getString(1).trim();
                        }
                        if (k > 0) {
                            ind_attrib = ind_attrib + "+";
                        }
                        ind_attrib = ind_attrib + attrib.toUpperCase();
                        rset2.close();
                        stmt2.close();
                    }
                    Constants.CPrintToConsole(ind_name + " : " + ind_attrib, Constants.DEBUG_THIRD_LEVEL_Information);
                    map.put(ind_name, ind_attrib);
                } else {
                    ind_name = rset.getString(2).trim();
                    ind_attrib = rset.getString(1).trim();
                    if (ind_attrib.startsWith("+")) {
                        ind_attrib = ind_attrib.substring(1);
                    }
                    if (map.containsKey(ind_name)) // For Oracle
                    {
                        String ind_attrib1 = map.get(ind_name);
                        ind_attrib = ind_attrib1 + "+" + ind_attrib;
                    }
                    map.put(ind_name, ind_attrib);
                }
            }
            rset.close();
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in getting indexes for " + relation + " from Statistics of " + settings.getDbName());
        }
        return map;
    }

    /**
     * Returns the name of the index build on the columns.
     * @param relation Relation name
     * @param cols Indexed columns
     * @return Index name
     * @throws DatalessException
     */
    @Override
    public String getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
        try {
            if (this.getSettings().getDbVendor().equals(DBConstants.MSSQL)) {
                Statement stmt = createStatement();
                String query = getQuery_getIndexName(relation, cols);
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                ResultSet rset;
                if (this instanceof MSSQLDatabase) {
                    boolean results = stmt.execute(query);
                    if (results) {
                        rset = stmt.getResultSet();
                    } else {
                        return null;
                    }
                } else {
                    rset = stmt.executeQuery(query);
                }
                String ind_name = null;
                while (rset.next()) {
                    ind_name = rset.getString(1).trim();
                    String ind_attrib = rset.getString(3).trim();
                    int count = 0;
                    for (int i = 0; i < cols.size(); i++) {
                        String col = cols.get(i);
                        if (ind_attrib.contains(col)) {
                            count++;
                        }
                    }
                    if (count == cols.size()) {
                        return ind_name;
                    }
                }
                return ind_name;
            } else if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                // Get the information from IndexedColumns
                Map<String, String> map = getIndexedColumns(relation);
                Iterator<String> iterator = map.keySet().iterator();
                while (iterator.hasNext()) {
                    String ind_name = (String) iterator.next();
                    String colNames = (String) map.get(ind_name);
                    ArrayList<String> ind_attrib = new ArrayList<String>();
                    if (colNames.contains("+")) {
                        String[] col = colNames.split(Pattern.quote("+"));
                        for (int c = 0; c < col.length; c++) {
                            ind_attrib.add(col[c]);
                        }
                    } else {
                        ind_attrib.add(colNames);
                    }
                    // Check for argument column names
                    int count = 0;
                    for (int i = 0; i < cols.size(); i++) {
                        String col = cols.get(i);
                        if (ind_attrib.contains(col)) {
                            count++;
                        }
                    }
                    if (count == cols.size() && count == ind_attrib.size()) {
                        return ind_name;
                    }
                }
                return null;
            } else {
                Statement stmt = createStatement();
                String query = getQuery_getIndexName(relation, cols);
                Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
                ResultSet rset = stmt.executeQuery(query); 
                String ind_name = null;
                int count = 0;
                while (rset.next()) {
                    count++;
                    if (ind_name != null) // For Oracle
                    {
                        // Multiple indexes on these columns
                        String ind_name1 = new String(rset.getString(1).trim());
                        if (!ind_name1.equals(ind_name)) {
                            return null;
                        }
                    }
                    ind_name = new String(rset.getString(1).trim());
                }
                if (getSettings().getDbVendor().equals(DBConstants.ORACLE) && count != cols.size()) {
                    // There is no index for the columns in cols.
                    return null;
                }
                rset.close();
                stmt.close();
                return ind_name;
            }
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in getting indexes name for from Statistics of " + settings.getDbName());
        }

    }

    /**
     * Returns the attributes of the specified relation
     * @param relation Relation name
     * @return Attributes
     * @throws DatalessException
     */
    @Override
    public String[] getAttributes(String relation) throws DatalessException {

        Set<String> set = new HashSet<String>();
        try {
            Statement stmt = createStatement();
            String query = getQuery_getAttributes(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            ResultSet rset = stmt.executeQuery(query);
            while (rset.next()) {
                String attrib = rset.getString(1).trim();
                set.add(attrib.toUpperCase());
            }
            rset.close();
            stmt.close();
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in getting attributes for " + relation + " from Statistics of " + settings.getDbName());
        }
        String[] attribs = new String[set.size()];
        Iterator<String> iter = set.iterator();
        int k = 0;
        while (iter.hasNext()) {
            attribs[k++] = (String) iter.next();
        }
        return attribs;
    }

    /**
     * Returns the primary key attributes of the specified relation.
     * @param relation Relation name
     * @return Primary Key attributes
     * @throws DatalessException
     */
    @Override
    public String[] getPrimaryKeyAttributes(String relation) throws DatalessException {
        String[] PKeys;
        try {
            // Get Primary Keys
            String query = getQuery_getPrimaryKeyAttributes(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                String PKIds = null;
                // Get Index Name
                Statement stmt1 = createStatement();
                ResultSet rset1 = stmt1.executeQuery(query);
                while (rset1.next()) {
                    PKIds = rset1.getString(1).trim();
                }
                rset1.close();
                stmt1.close();
                if (PKIds == null) {
                    return null;
                }

                // Remove paranthesis
                if (PKIds.startsWith("{")) {
                    PKIds = PKIds.substring(1);
                }
                if (PKIds.endsWith("}")) {
                    PKIds = PKIds.substring(0, PKIds.length() - 1);
                }
                // Get PK attributes
                String[] keys = PKIds.split(",");
                PKeys = new String[keys.length];
                for (int k = 0; k < keys.length; k++) {
                    Statement stmt2 = createStatement();
                    String query2 = "select attname from  pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attnum = " + keys[k];
                    ResultSet rset2 = stmt2.executeQuery(query2);
                    String attrib = null;
                    while (rset2.next()) {
                        attrib = rset2.getString(1).trim();
                    }
                    PKeys[k] = attrib.toUpperCase();
                    rset2.close();
                    stmt2.close();
                }
            } else {
                Statement stmt1 = createStatement();
                ResultSet rset = stmt1.executeQuery(query);
                String rel = new String();
                while (rset.next()) {
                    String temp = rset.getString(1).trim();
                    Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
                    rel = rel + temp + "::";
                }
                rset.close();
                stmt1.close();
                PKeys = rel.split("::");
            }
            return PKeys;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting Primary Key Attributes of relation:" + relation + "  :" + e);
        }
    }

    /**
     * Returns the Foreign key columns of the specified relation and the corresponding Primary Key relations.
     * @param relation Relation name
     * @return Foreign Key Columns and the corresponding Primary Key relations (Map <FKColumnName, PKRelaionName>)
     * @throws DatalessException
     */
    @Override
    public Map<String, String> getFKColumnRefRelation(String relation) throws DatalessException {
        try {
            String query = getQuery_getFKColumnRefRelation(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            String vendor = settings.getDbVendor();
            Statement stmt = createStatement();
            ResultSet rset = stmt.executeQuery(query);
            TreeMap<String, String> fkColRefRel = new TreeMap<String, String>();
            while (rset.next()) {
                String pkRelation = rset.getString(1).trim();
                String fkCols = rset.getString(2).trim();

                if (vendor.equals(DBConstants.DB2)) {
                    fkCols = fkCols.trim();
                    if (fkCols.contains(" ")) {
                        String[] fkCol = fkCols.split(" ");
                        for (int f = 0; f < fkCol.length; f++) {
                            String fk = fkCol[f].trim();
                            if (!fk.isEmpty()) {
                                Constants.CPrintToConsole(fk + " - " + pkRelation, Constants.DEBUG_SECOND_LEVEL_Information);
                                fkColRefRel.put(fk, pkRelation);
                            }
                        }
                    } else {
                        Constants.CPrintToConsole(fkCols + " - " + pkRelation, Constants.DEBUG_SECOND_LEVEL_Information);
                        fkColRefRel.put(fkCols, pkRelation);
                    }
                } else if (vendor.equals(DBConstants.POSTGRES)) {
                    String pkRelationId = rset.getString(1).trim();
                    String FKIds = rset.getString(2).trim();
                    // Get PK Relation Name
                    Statement stmt1 = createStatement();
                    String query1 = "select relname from pg_class where pg_class.oid=" + pkRelationId;
                    ResultSet rset1 = stmt1.executeQuery(query1);
                    while (rset1.next()) {
                        pkRelation = rset1.getString(1).trim();
                    }
                    rset1.close();
                    stmt1.close();
                    if (FKIds == null) {
                        return fkColRefRel;
                    }
                    // Get FK Cols
                    // Remove paranthesis
                    if (FKIds.startsWith("{")) {
                        FKIds = FKIds.substring(1);
                    }
                    if (FKIds.endsWith("}")) {
                        FKIds = FKIds.substring(0, FKIds.length() - 1);
                    }
                    // Get PK attributes
                    String[] keys = FKIds.split(",");
                    for (int k = 0; k < keys.length; k++) {
                        Statement stmt2 = createStatement();
                        String query2 = "select attname from  pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attnum = " + keys[k];
                        ResultSet rset2 = stmt2.executeQuery(query2);
                        String attrib = null;
                        while (rset2.next()) {
                            attrib = rset2.getString(1).trim();
                        }
                        fkColRefRel.put(attrib.toUpperCase(), pkRelation);
                        rset2.close();
                        stmt2.close();
                    }
                } else {  // Other Databases
                    Constants.CPrintToConsole(fkCols + " - " + pkRelation, Constants.DEBUG_SECOND_LEVEL_Information);
                    fkColRefRel.put(fkCols, pkRelation);
                }
            }
            rset.close();
            stmt.close();
            return fkColRefRel;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Exception in finding FK Column - PK Relation mapping of " + settings.getDbName());
        }
    }

    /**
     * Returns whether the specified column is a part primary key or not.
     * @param relation Relation name
     * @param column Column name
     * @return true, if column is a PK,
     *         false, otherwise
     * @throws DatalessException
     */
    @Override
    public boolean isPrimaryKey(String relation, String column) throws DatalessException {
        try {
            // Get Primary Keys
            String[] PKeys = getPrimaryKeyAttributes(relation);
            for (int i = 0; PKeys != null && i < PKeys.length; i++) {
                if (PKeys[i].equalsIgnoreCase(column)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting Primary Key Attributes of relation:" + relation + "  :" + e);
        }

    }

    /**
     * Returns the foreign key attributes of the specified relation.
     * @param relation Relation name
     * @return Foreign key attributes of the specified relation
     * @throws DatalessException
     */
    @Override
    public String[] getForeignKeyAttributes(String relation) throws DatalessException {
        String[] FKeys = null;
        try {
            // Get foreign Keys
            String query = getQuery_getForeignKeyAttributes(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                String FKIds = null;
                // Get Index Name
                Statement stmt1 = createStatement();
                ResultSet rset1 = stmt1.executeQuery(query);
                ArrayList<String> ForeignKeys = new ArrayList<String>();
                while (rset1.next()) {
                    FKIds = rset1.getString(1).trim();
                    if (FKIds == null) {
                        continue;
                    }
                    // Remove paranthesis
                    if (FKIds.startsWith("{")) {
                        FKIds = FKIds.substring(1);
                    }
                    if (FKIds.endsWith("}")) {
                        FKIds = FKIds.substring(0, FKIds.length() - 1);
                    }
                    // Get PK attributes
                    String[] keys = FKIds.split(",");
                    for (int k = 0; k < keys.length; k++) {
                        Statement stmt2 = createStatement();
                        String query2 = "select attname from  pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attnum = " + keys[k];
                        ResultSet rset2 = stmt2.executeQuery(query2);
                        String attrib = null;
                        while (rset2.next()) {
                            attrib = rset2.getString(1).trim();
                        }
                        ForeignKeys.add(attrib.toUpperCase());
                        rset2.close();
                        stmt2.close();
                    }
                }
                if (ForeignKeys.size() > 0) {
                    FKeys = new String[ForeignKeys.size()];
                    for (int fk = 0; fk < ForeignKeys.size(); fk++) {
                        FKeys[fk] = ForeignKeys.get(fk);
                    }
                } else {
                    FKeys = null;
                }
                rset1.close();
                stmt1.close();
            } else {
                Statement stmt1 = createStatement();
                ResultSet rset = stmt1.executeQuery(query);
                String rel = "";
                while (rset.next()) {
                    String temp = rset.getString(1).trim();
                    Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
                    rel = rel + temp + "::";
                }
                rset.close();
                stmt1.close();
                if(rel!=null){
                    FKeys = rel.split("::");                	
                }
            }
            return FKeys;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting Foreign Key Attributes of relation:" + relation + "  :" + e);
        }
    }

    /**
     * Returns the Unique key attributes of the specified relation.
     * @param relation Relation name
     * @return Unique key attributes of the specified relation
     * @throws DatalessException
     */
    @Override
    public String[] getUniqueAttributes(String relation) throws DatalessException {
        String[] UniqueCols;
        try {
            // Get Primary Keys
            String query = getQuery_getUniqueAttributes(relation);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                String UIds = null;
                // Get Index Name
                Statement stmt1 = createStatement();
                ResultSet rset1 = stmt1.executeQuery(query);
                ArrayList<String> UniqueKeys = new ArrayList<String>();
                while (rset1.next()) {
                    UIds = rset1.getString(1).trim();
                    if (UIds == null) {
                        continue;
                    }
                    // Remove paranthesis
                    if (UIds.startsWith("{")) {
                        UIds = UIds.substring(1);
                    }
                    if (UIds.endsWith("}")) {
                        UIds = UIds.substring(0, UIds.length() - 1);
                    }
                    // Get PK attributes
                    String[] keys = UIds.split(",");
                    for (int k = 0; k < keys.length; k++) {
                        Statement stmt2 = createStatement();
                        String query2 = "select attname from  pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attnum = " + keys[k];
                        ResultSet rset2 = stmt2.executeQuery(query2);
                        String attrib = null;
                        while (rset2.next()) {
                            attrib = rset2.getString(1).trim();
                        }
                        UniqueKeys.add(attrib.toUpperCase());
                        rset2.close();
                        stmt2.close();
                    }
                }
                if (UniqueKeys.size() > 0) {
                    UniqueCols = new String[UniqueKeys.size()];
                    for (int fk = 0; fk < UniqueKeys.size(); fk++) {
                        UniqueCols[fk] = UniqueKeys.get(fk);
                    }
                } else {
                    UniqueCols = null;
                }
                rset1.close();
                stmt1.close();
            } else {
                Statement stmt1 = createStatement();
                ResultSet rset = stmt1.executeQuery(query);
                String rel = new String();
                while (rset.next()) {
                    String temp = rset.getString(1).trim();
                    Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
                    rel = rel + temp + "::";
                }
                rset.close();
                stmt1.close();
                UniqueCols = rel.split("::");
            }
            return UniqueCols;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting Unique Attributes of relation:" + relation + "  :" + e);
        }
    }

    /**
     * Checks whether the column is nullable or not.
     * @param relation Relation name
     * @param column Column name
     * @return true if NOTNULL.
     * @throws DatalessException
     */
    @Override
    public boolean isNotNullable(String relation, String column) throws DatalessException {
        try {
            // Get Primary Keys
            String query = getQuery_getNullableValue(relation, column);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            Statement stmt1 = createStatement();
            ResultSet rset = stmt1.executeQuery(query);
            String val = new String();
            while (rset.next()) {
                val = rset.getString(1).trim();
                Constants.CPrintToConsole(val, Constants.DEBUG_SECOND_LEVEL_Information);
            }
            String vendor = this.getSettings().getDbVendor();
            if (vendor.equals(DBConstants.MSSQL)) {
                if (val.equals("0")) {
                    return true;
                }
                return false;
            } else if (vendor.equals(DBConstants.POSTGRES)) {
                if (val.equals("t")) {
                    return true;
                }
                return false;
            } else {
                if (val.equals("N")) {
                    return true;
                }
                return false;
            }

        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting NotNUll for " + column + " of relation:" + relation + "  :" + e);
        }

    }

    /**
     * Returns the Integrity constraint type of the column
     * @param relation Relation name
     * @param column Column name
     * @return Integrity Constraint present in the column
     * @throws DatalessException
     */
    @Override
    public Constraint getConstraint(String relation, String column) throws DatalessException {
        try {
            boolean isPK, isComposite, isFK, isUnique, isNotNull;
            String type = "";
            isPK = false;
            isComposite = false;
            isFK = false;
            isUnique = false;
            isNotNull = false;

            String[] PKeys = getPrimaryKeyAttributes(relation);
            String[] FKeys = getForeignKeyAttributes(relation);
            String[] UniqueCols = getUniqueAttributes(relation);

            StringTokenizer st = new StringTokenizer(column, "|");

            String col;
            while (st.hasMoreTokens()) {
                col = st.nextToken();

                // Check for Primary Key
                for (int i = 0; PKeys != null && i < PKeys.length; i++) {
                    if (PKeys[i].equalsIgnoreCase(col)) {
                        isPK = true;
                    }
                }
                // Check for Composite Primary Key
                if (isPK && PKeys.length > 1) {
                    isComposite = true;
                }
                // Check for Foreign Key
                for (int i = 0; FKeys != null && i < FKeys.length; i++) {
                    if (FKeys[i].equalsIgnoreCase(col)) {
                        isFK = true;
                    }
                }
                // Check for Unique Constraint
                for (int i = 0; UniqueCols != null && i < UniqueCols.length; i++) {
                    if (UniqueCols[i].equalsIgnoreCase(col)) {
                        isUnique = true;
                    }
                }
                // Check for Not Null
                isNotNull = isNotNullable(relation, col);

                if (isPK && isComposite && isFK) {
                    type = type + Constraint.COMPOSITE_PK_FK + "|";
                } else if (isPK && isComposite) {
                    type = type + Constraint.COMPOSITE_PK + "|";
                } else if (isPK) {
                    type = type + Constraint.PRIMARYKEY + "|";
                } else if (isFK) {
                    type = type + Constraint.FOREIGNKEY + "|";
                } else if (isUnique) {
                    type = type + Constraint.UNIQUE + "|";
                } else if (isNotNull) {
                    type = type + Constraint.NOTNULL + "|";
                } else {
                    type = type + Constraint.NONE + "|";
                }

                isPK = false;
                isComposite = false;
                isFK = false;
                isUnique = false;
                isNotNull = false;
            }
            if (type.contains("|")) {
                type = type.substring(0, type.length() - 1);
            }

            return new Constraint(type);
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting the constraint on column " + column + " of relation:" + relation + "  :" + e);
        }
    }

    /**
     * Given a Foreign key column and relation, return the primary key relation [0] and column [1]
     * @param relation Relation
     * @return Primary Key Relation and the Primary Key Column
     * @throws DatalessException
     */
    @Override
    public String[] getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
        String[] PK = new String[2];
        try {
            // Get Primary Keys
            String query = getQuery_getPrimaryKeyRelationAndColumn(relation, column);
            Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
            if (this.getSettings().getDbVendor().equals(DBConstants.POSTGRES)) {
                Statement stmt1 = createStatement();
                ResultSet rset = stmt1.executeQuery(query);
                while (rset.next()) {
                    String FKIds = rset.getString(1).trim();
                    String PKRelId = rset.getString(2).trim();
                    String PKIds = rset.getString(3).trim();
                    // Get PK Relation
                    String PKRelation = null;
                    Statement stmt2 = createStatement();
                    String query2 = "select relname from pg_class where pg_class.oid=" + PKRelId;
                    ResultSet rset2 = stmt2.executeQuery(query2);
                    while (rset2.next()) {
                        PKRelation = rset2.getString(1).trim().toUpperCase();
                    }
                    rset2.close();
                    stmt2.close();
                    // Get Column Id
                    String colId = null;
                    stmt2 = createStatement();
                    query2 = "select attnum from pg_class, pg_attribute where attrelid=pg_class.oid and relname='" + relation.toLowerCase() + "' and attname='" + column.toLowerCase() + "'";
                    rset2 = stmt2.executeQuery(query2);
                    while (rset2.next()) {
                        colId = rset2.getString(1).trim();
                    }
                    rset2.close();
                    stmt2.close();

                    // Remove paranthesis
                    if (FKIds.startsWith("{")) {
                        FKIds = FKIds.substring(1);
                    }
                    if (FKIds.endsWith("}")) {
                        FKIds = FKIds.substring(0, FKIds.length() - 1);
                    }
                    if (PKIds.startsWith("{")) {
                        PKIds = PKIds.substring(1);
                    }
                    if (PKIds.endsWith("}")) {
                        PKIds = PKIds.substring(0, PKIds.length() - 1);
                    }
                    // Get PK attribId
                    String PKColId = null;
                    String[] keys = FKIds.split(",");
                    String[] Pkeys = PKIds.split(",");
                    for (int k = 0; k < keys.length; k++) {
                        if (keys[k].equals(colId)) {
                            PKColId = Pkeys[k];
                        }
                    }
                    // GET PK attributeName
                    String PKColName = null;
                    if (PKColId != null) {
                        stmt2 = createStatement();
                        query2 = "select attname from pg_attribute where attrelid='" + PKRelId + "' and attnum='" + PKColId + "'";
                        rset2 = stmt2.executeQuery(query2);
                        while (rset2.next()) {
                            PKColName = rset2.getString(1).trim().toUpperCase();
                        }
                        rset2.close();
                        stmt2.close();
                        PK[0] = PKRelation;
                        PK[1] = PKColName;
                    }

                }
                rset.close();
                stmt1.close();

            } else {
                Statement stmt1 = createStatement();
                ResultSet rset = stmt1.executeQuery(query);
                String vendor = getSettings().getDbVendor();
                while (rset.next()) {
                    PK[0] = rset.getString(1).trim();
                    String pkCols = rset.getString(2).trim();
                    if (vendor.equals(DBConstants.DB2)) {
                        String fkCols = rset.getString(3).trim();
                        TreeMap<String, String> map = new TreeMap<String, String>();
                        fkCols = fkCols.trim();
                        pkCols = pkCols.trim();
                        if (fkCols.contains(" ")) {
                            String[] fkCol = fkCols.split(" ");
                            ArrayList<String> fkC = new ArrayList<String>();
                            for (int f = 0; f < fkCol.length; f++) {
                                String fk = fkCol[f].trim();
                                if (!fk.isEmpty()) {
                                    fkC.add(fk);
                                }
                            }
                            String[] pkCol = pkCols.split(" ");
                            ArrayList<String> pkC = new ArrayList<String>();
                            for (int f = 0; f < pkCol.length; f++) {
                                String pk = pkCol[f].trim();
                                if (!pk.isEmpty()) {
                                    pkC.add(pk);
                                }
                            }
                            for (int f = 0; f < fkC.size(); f++) {
                                map.put(fkC.get(f), pkC.get(f));
                            }
                        } else {
                            map.put(fkCols, pkCols);
                        }
                        PK[1] = map.get(column);

                    } else {  // Other Databases
                        PK[1] = pkCols;
                    }

                    Constants.CPrintToConsole(PK[0] + "-" + PK[1], Constants.DEBUG_SECOND_LEVEL_Information);
                }
                rset.close();
                stmt1.close();
            }
            return PK;
        } catch (Exception e) {
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in getting Primary Key column, relation of specified foreign key relation:" + relation + "  :" + e);
        }
    }

    /**
     * Given the set of input relations, a dependency graph is formed and its topologically sorted and the ordered nodes are returned.
     * This is used in the RetainMode operation of MSSQL and Postgres.
     * @param allRelations Input relations
     * @return Ordered relations
     * @throws DatalessException
     */
    @Override
    public ArrayList<String> getTopologicalSortedRelations(String[] allRelations) throws DatalessException {
        /** Step 1) Dependency Graph
         *  Stores the graph of dependency. Each relation has a list of all FK relations
         *  Stores incoming relations.  (Edges are from FK to PK relations)
         */
        HashMap<String, ArrayList<String>> dependencyGraph; // Given PK, gives all the FK relations
        HashMap<String, ArrayList<String>> inverseMap; // Given FK, gives all the PK relations
        dependencyGraph = new HashMap<String, ArrayList<String>>();
        inverseMap = new HashMap<String, ArrayList<String>>();
        ArrayList<String> NodesWithNoIncomingEdges = new ArrayList<String>(); // Used in toplogical sorting
        for (int i = 0; i < allRelations.length; i++) {
            String relation = allRelations[i];
            ArrayList<String> depRels = null;
            try {
                ArrayList<String> dep = getDependentRelations(relation);
                depRels = new ArrayList<String>();
                for (int k = 0; k < dep.size(); k++) {
                    String rel = dep.get(k);
                    depRels.add(rel);
                    // Update inverse Map
                    if (inverseMap.containsKey(rel)) {
                        ArrayList<String> PKRels = inverseMap.get(rel);
                        PKRels.add(relation);
                        inverseMap.put(rel, PKRels);
                    } else {
                        ArrayList<String> PKRels = new ArrayList<String>();
                        PKRels.add(relation);
                        inverseMap.put(rel, PKRels);
                    }
                }
                if (depRels.isEmpty()) {
                    NodesWithNoIncomingEdges.add(relation);
                }
            } catch (DatalessException ex) {
                Constants.CPrintErrToConsole(ex);
                JOptionPane.showMessageDialog(null, "Exception in initializing dependency graph.", "CODD - Exception", JOptionPane.WARNING_MESSAGE);
                return null;
            }
            dependencyGraph.put(relation, depRels);
        }
        /** Step 2) Topological Sort
         * L - Empty list that will contain the sorted elements
         * S - Set of all nodes with no incoming edges
         * while S is non-empty do
         *  remove a node n from S
         *  insert n into L
         *  for each node m with an edge e from n to m do
         *      remove edge e from the graph
         *      if m has no other incoming edges then
         *          insert m into S
         * if graph has edges then
         *  return error (graph has at least one cycle)
         * else
         *  return L (a topologically sorted order)
         */
        ArrayList<String> sortedRelations = new ArrayList<String>();
        while (!NodesWithNoIncomingEdges.isEmpty()) {
            String relation = NodesWithNoIncomingEdges.remove(0); // remove the first element.
            sortedRelations.add(relation);
            // Get the parent nodes of relation.
            ArrayList<String> PKRels = inverseMap.get(relation);
            if (PKRels != null) {
                for (int pk = 0; pk < PKRels.size(); pk++) {
                    String PKRelation = PKRels.get(pk);
                    // remove the edge
                    ArrayList<String> depRels = dependencyGraph.get(PKRelation);
                    depRels.remove(relation);
                    if (depRels.isEmpty()) {
                        NodesWithNoIncomingEdges.add(PKRelation);
                    }
                }
            }
        }
        // if graph has an edge
        Set<String> keySet = dependencyGraph.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            String rel = (String) iter.next();
            ArrayList<String> depRels = dependencyGraph.get(rel);
            if (!depRels.isEmpty()) {
                Constants.CPrintToConsole("Dependency graph has a cycle. Relation" + rel + " edges are present after topological Sort.", Constants.DEBUG_SECOND_LEVEL_Information);
                throw new DatalessException("Dependency graph has a cycle.");
            }
        }
        Constants.CPrintToConsole("Topological Sorted order of relations: ", Constants.DEBUG_SECOND_LEVEL_Information);
        for (int s = 0; s < sortedRelations.size(); s++) {
            String rel = sortedRelations.get(s);
            Constants.CPrintToConsole(rel, Constants.DEBUG_SECOND_LEVEL_Information);
        }
        return sortedRelations;
    }

    /**
     * Scales (size based) the Statistics of the specified relation by scaleFactor times.
     * @param relation Relation name
     * @param scaleFactor scale factor
     * @param runStats true, collect statistics before scaling
     * @return true on success
     * @throws DatalessException
     */
    @Override
    public boolean scale(String relation, int scaleFactor, boolean runStats) throws DatalessException {
        try {
            if (runStats) {
                this.collectStatistics(relation);
            }
            Statistics stats = new Statistics(relation, getSettings().getDbVendor());
            RelationStatistics relStat = getRelationStatistics(relation);
            stats.setRelationStatistics(relStat);
            String[] attribs;
            if (this instanceof NonStopSQLDatabase) {
                attribs = getMultiColumnAttributes(relation);
            } else {
                attribs = getAttributes(relation);
            }
            for (int j = 0; j < attribs.length; j++) {
                ColumnStatistics colStat = getColumnStatistics(relation, attribs[j], relStat.getCardinality());
                stats.addColumnStatistics(attribs[j], colStat);
            }
            if (attribs.length == 0) {
            	Constants.CPrintToConsole("Error: Histograms are not initialized for the table: "+ relation, 1);
            	return false;            
            }
            
            if(!(this instanceof NonStopSQLDatabase)){
                Map<String,String> map = getIndexedColumns(relation);
                Iterator<String> iterator = map.keySet().iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    String colNames = map.get(key);
                    ArrayList<String> cols = new ArrayList<String>();
                    if (colNames.contains("+")) {
                    String[] col = colNames.split(Pattern.quote("+"));
                        for (int c = 0; c < col.length; c++) {
                            cols.add(col[c]);
                        }
                    } else {
                        cols.add(colNames);
                    }
                    IndexStatistics indexStat = getIndexStatistics(relation, cols);
                    stats.addIndexStatistics(colNames, indexStat);
                }            	
            }

            if (!reSetConnection()) {
                JOptionPane.showMessageDialog(null, "Exception Caught: Connection Reset failed.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
            }
            // Start a transaction boundary by setting auto commit off, before we start writing the scaled values back 
            // into the database. We do this after starting a fresh connection in the above step.
            con.setAutoCommit(false);
            
            // Scale and Set    
            relStat.scale(scaleFactor);
            if(this instanceof DB2Database){
            	((DB2Database)this).setRelationStatistics(relation, relStat, stats.colStats);            	
            }else{
                setRelationStatistics(relation, relStat);
            }

            for (int j = 0; j < attribs.length; j++) {
                ColumnStatistics colStat = stats.getColumnStatistics(attribs[j]);
                colStat.scale(scaleFactor);
                if(this instanceof NonStopSQLDatabase){
                    ((NonStopSQLDatabase)this).setColumnStatisticsForScaling(relation, attribs[j], colStat);                	
                }else{
                	this.setColumnStatistics(relation, attribs[j], colStat,relStat.getCardinality());
                }
            }

            if(!(this instanceof NonStopSQLDatabase)){
                Map<String,String> map = getIndexedColumns(relation);
                Iterator<String> iterator = map.keySet().iterator();
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
                IndexStatistics indexStat = stats.getIndexStatistics(colNames);
                indexStat.scale(scaleFactor);
                setIndexStatistics(relation, cols, indexStat);
            }            	
            }
            // Commit the transaction boundary by setting auto commit off and commit the work, so that the scaled values are 
            // committed into the database.
            con.commit();
            con.setAutoCommit(true);
            
            //
            return true;
        } catch (Exception e) {
        	// Abort the transaction boundary by setting auto commit off and rollback the work, so that the scaled values are 
            // NOT committed into the database.
            try {
				con.rollback();
				con.setAutoCommit(true);
			} catch (SQLException e1) {
				// Unable to rollback() work.
				e1.printStackTrace();
			}
            
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database: Error in scaling relation:" + relation + "  :" + e);
        }
    }
}