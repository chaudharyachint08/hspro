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
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.plan.Plan;
import java.math.BigDecimal;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Map;

/**
 * Database is a  class, which specifies the methods to be implemented by the subclass.
 * Apart from that, it also has few functions implemented here.
 * In order to port CODD to another Database, one has to create another class extending the Database class.
 * @author dsladmin
 */
public interface Database {
//    public Connection con = null; // Database Connection
//    public DBSettings settings = null; // Database Conenction Information

    /**
     * Constructor for Database Class.
     * @param settings Connection information through DBSettings object
     * @throws DatalessException
     */
    
//    public Database(DBSettings settings) throws DatalessException {
//        this.settings = settings;
//        connect(settings);
//    }

    
    /**
     * Connect to the database with available database connection (attribute settings) parameters.
     * @return
     * @throws DatalessException
     */
    public boolean connect() throws DatalessException;
    /**
     * Returns the query to Stop Automatic Update of Statistics.
     * @return the query to Stop Automatic Update of Statistics
     */
     public String getQuery_stopAutoUpdateStats();
    /**
     * Returns the query to get the relations of the user.
     * @return the query to get the relations of the user
     */
     public String getQuery_SelectRelations();
    /**
     * Returns the query to get the dependent relations (recursively) of the specified relation.
     * @return the query to get the dependent relations (recursively) of the specified relation
     */
     public String getQuery_dependentRelations(String relation);
    /**
     * Returns the query to get the datatype of the column of relation.
     * @return the query to get the datatype of the column of relation
     */
     public String getQuery_columnDataType(String relation, String column);
    /**
     * Returns the query to get the indexed columns of the specified relation.
     * @return the query to get the indexed columns of the specified relation
     */
     public String getQuery_getIndexedColumns(String relation) throws DatalessException;
    /**
     * Returns the query to get the index name of the specified relation built on the columns specified by cols.
     * @return the query to get the index name of the specified relation built on the columns
     */
     public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException;
    /**
     * Returns the query to get the attributes of the specified relation.
     * @return the query to get the attributes of the specified relation
     */
     public String getQuery_getAttributes(String relation) throws DatalessException;
    
    /**
     * Returns the query to get the primary key attributes of the specified relation.
     * @return the query to get the primary key attributes of the specified relation
     */
     public String getQuery_getPrimaryKeyAttributes(String relation)  throws DatalessException;
    // Map <FKColumnName, PKRelaionName>
    /**
     * Returns the query to get the Foreign key columns of the specified relation and the corresponding Primary Key relations.
     * @return Foreign Key Columns and the corresponding Primary Key relations
     */
     public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException;
    /**
     * Returns the query to get the foreign key attributes of the specified relation.
     * @return the query to get the foreign key attributes of the specified relation
     */
     public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException;
    /**
     * Returns the query to get the unique attributes of the specified relation.
     * @return the query to get the unique attributes of the specified relation
     */
     public String getQuery_getUniqueAttributes(String relation) throws DatalessException;
    /**
     * Returns the query to get the nullable attributes of the specified relation.
     * @return the query to get the nullable attributes of the specified relation
     */
     public String getQuery_getNullableValue(String relation, String column) throws DatalessException;
    /**
     * Returns the query to get the Primary Key Relation and the Primary Key Column of the specified Foreign Key column of relation.
     * @return the query to get the Primary Key Relation and the Primary Key Column
     */
     public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException;

     public String getMultiColumnHistogramId(String relation) throws DatalessException;
     
     public String getColumnHistogramId(String relation, String column) throws DatalessException;
             
    /**
     * Connects to the database with the specified Database connection parameters.
     * @param settings Connection Parameters as Settings object
     * @return true, if connection is successful,
     *         false, otherwise.
     * @throws DatalessException
     */
     public boolean connect(DBSettings settings) throws DatalessException;
    /**
     * Implements the RetainMode.
     * dropRelations, dependentRelations data is deleted and only the Metadata is kept.
     * @param dropRelations Relations to retain
     * @param dependentRelations Dependent relations of dropRelations
     * @return true on success
     * @throws DatalessException
     */
     public boolean retain(String[] dropRelations, String[] dependentRelations) throws DatalessException;
    /**
     * Implemented only for MSSQL Server. Inter Engine Transfer. It transfers the relation statistics to destination.
     * @param relation set of relations.
     * @param destDatabase destination database.
     * @return true, successful. false, otherwise.
     * @throws DatalessException
     */
     public boolean transfer(String[] relation, Database destDatabase) throws DatalessException;

     public void construct(String[] relation)  throws DatalessException;
    /**
     * Collect statistics on the specified relation.
     * @param relation Relation name
     * @throws DatalessException
     */
     public void collectStatistics(String relation)  throws DatalessException;

    /**
     * Returns the RelationStatistics for the specified relation.
     * @param relation Relation name
     * @return RelationStatistics
     * @throws DatalessException
     */
     public RelationStatistics getRelationStatistics(String relation)  throws DatalessException;
    /**
     * Returns the ColumnStatistics for the specified column.
     * Integer tableCard used in Oracle to determine the height of the bucket for Height balanced histogram.
     * @param relation Relation name
     * @param column Column name
     * @param tableCard Table Cardinality
     * @return ColumnStatisitcs
     * @throws DatalessException
     */
     public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard)  throws DatalessException;
    /**
     * Returns the IndexStatistics for the specified indexed column names.
     * @param relation Relation name
     * @param colNames Indexed column names in an ArrayList
     * @return IndexStatistics
     * @throws DatalessException
     */
     public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException;
      /**
     * Returns the query to get the multi - column attributes of the specified relation.
     * @return the query to get the multi - column attributes of the specified relation
     */
     public String[] getMultiColumnAttributes(String relation) throws DatalessException;
     /**
      * Updates the database catalog with the specified Hardware Statistics.
      * @throws DatalessException
      */
      public boolean setHardwareStatistics() throws DatalessException;
    /**
     * Updates the database catalog with the specified Relation Statistics.
     * @param relation Relation name
     * @param relationStatistics RelationStatistics
     * @return true on success
     * @throws DatalessException
     */
     public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException;
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
     public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException;
    /**
     * Updates the database catalog with the specified Index Statistics.
     * @param relation Relation name
     * @param colNames Indexed column names in an ArrayList
     * @param indexStatistics IndexStatistics
     * @return true on success
     * @throws DatalessException
     */
     public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException;
     
    /**
     * Returns the Query Execution Plan Tree for the specified query.
     * For all the subclasses, this function is copied from Picasso code, with few modifications.
     * The modifications include removing isPicassoPredicate function and cost to CPU cost.
     * Except for PostgreSQL, we use CPU cost. Since PostgreSQL does not provide we use Total cost of the operator.
     * @param query Query
     * @return Plan
     * @throws DatalessException
     */
     public Plan getPlan(String query) throws DatalessException;

    public String getSchema();
    
    public String getCatalog();

    public boolean isConnected();

    public boolean reSetConnection() throws DatalessException;

    public DBSettings getSettings();

    public boolean commit();
	
	public boolean rollback();

    public boolean close();

    public Statement createStatement();

    public PreparedStatement prepareStatement(String temp);

    /**
     * Constructs a Database instance based on the connection information.
     * For supporting a new Database system add the new database name in the if.else ladder and
     * instantiate the corresponding Database class.
     * @param settings DBSettings object
     * @return Database object
     * @throws DatalessException
     */
    //public static Database getDatabase(DBSettings settings);
    public Database getDatabase(DBSettings settings) throws DatalessException;
    /**
     * Stop Automatic Update of Statistics.
     * @return true on success
     * @throws DatalessException
     */
    public boolean stopAutoUpdateOfStats() throws DatalessException;

    /**
     * Returns the relations belong to the connected database user.
     * @return relations
     * @throws DatalessException
     */
    public ArrayList<String> getRelations() throws DatalessException;

    /**
     * Given PK relations, returns the FK relations.
     * It is used only by the drop mode, to figure out the dependent relations recursively.
     * @param relations Set of PK relations
     * @return ArrayList of dependent relations excluding the given one.
     * @throws DatalessException
     */
    public ArrayList<String> getDependentRelations(ArrayList<String> relations) throws DatalessException;
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
    public ArrayList<String> getDependentRelations(String relation) throws DatalessException;
    /**
     * Returns the Datatype of the column.
     * @param relation Relation name
     * @param column Column name
     * @return Data type in String format
     */
    public String getType(String relation, String column) throws DatalessException;

    /**
     * Returns the list of indexes available on this relation.
     * Return <Index Name, Indexed Columns (col1+col2+col3..)>
     * @param relation Relation name
     * @return Mapping of Index name to the Indexed Columns
     * @throws DatalessException
     */
    public Map<String,String> getIndexedColumns(String relation) throws DatalessException;

    /**
     * Returns the name of the index build on the columns.
     * @param relation Relation name
     * @param cols Indexed columns
     * @return Index name
     * @throws DatalessException
     */
    public String getIndexName(String relation, ArrayList<String> cols) throws DatalessException;

    /**
     * Returns the attributes of the specified relation
     * @param relation Relation name
     * @return Attributes
     * @throws DatalessException
     */
    public String[] getAttributes(String relation) throws DatalessException;

    /**
     * Returns the primary key attributes of the specified relation.
     * @param relation Relation name
     * @return Primary Key attributes
     * @throws DatalessException
     */
    public String[] getPrimaryKeyAttributes(String relation) throws DatalessException;

    /**
     * Returns the Foreign key columns of the specified relation and the corresponding Primary Key relations.
     * @param relation Relation name
     * @return Foreign Key Columns and the corresponding Primary Key relations (Map <FKColumnName, PKRelaionName>)
     * @throws DatalessException
     */
    public Map<String, String> getFKColumnRefRelation(String relation) throws DatalessException;

    /**
     * Returns whether the specified column is a part primary key or not.
     * @param relation Relation name
     * @param column Column name
     * @return true, if column is a PK,
     *         false, otherwise
     * @throws DatalessException
     */
    public boolean isPrimaryKey(String relation, String column) throws DatalessException;

    /**
     * Returns the foreign key attributes of the specified relation.
     * @param relation Relation name
     * @return Foreign key attributes of the specified relation
     * @throws DatalessException
     */
    public String[] getForeignKeyAttributes(String relation) throws DatalessException;
    /**
     * Returns the Unique key attributes of the specified relation.
     * @param relation Relation name
     * @return Unique key attributes of the specified relation
     * @throws DatalessException
     */
    public String[] getUniqueAttributes(String relation) throws DatalessException;

    /**
     * Checks whether the column is nullable or not.
     * @param relation Relation name
     * @param column Column name
     * @return true if NOTNULL.
     * @throws DatalessException
     */
    public boolean isNotNullable(String relation, String column) throws DatalessException;

    /**
     * Returns the Integrity constraint type of the column
     * @param relation Relation name
     * @param column Column name
     * @return Integrity Constraint present in the column
     * @throws DatalessException
     */
    public Constraint getConstraint(String relation, String column) throws DatalessException;
    /**
     * Given a Foreign key column and relation, return the primary key relation [0] and column [1]
     * @param relation Relation
     * @return Primary Key Relation and the Primary Key Column
     * @throws DatalessException
     */
    public String[] getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException;

    /**
     * Given the set of input relations, a dependency graph is formed and its topologically sorted and the ordered nodes are returned.
     * This is used in the RetainMode operation of MSSQL and Postgres.
     * @param allRelations Input relations
     * @return Ordered relations
     * @throws DatalessException
     */
    public ArrayList<String> getTopologicalSortedRelations(String[] allRelations) throws DatalessException;
  
    /**
     * Scales (size based) the Statistics of the specified relation by scaleFactor times.
     * @param relation Relation name
     * @param scaleFactor scale factor
     * @param runStats true, collect statistics before scaling
     * @return true on success
     * @throws DatalessException
     */
    public boolean scale(String relation, int scaleFactor, boolean runStats) throws DatalessException;

    }