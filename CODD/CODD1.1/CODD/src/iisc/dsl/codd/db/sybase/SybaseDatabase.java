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
package iisc.dsl.codd.db.sybase;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DatabaseAbstract;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.plan.Plan;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Implementation of the functionalities specific to Sybase, required for CODD.
 * @author dsladmin
 */
public class SybaseDatabase extends DatabaseAbstract {

    /**
     * Constructs a SybaseDatabase instance with default values
     * @param settings Database DBSettings object
     * @throws DatalessException
     */
    public SybaseDatabase(DBSettings settings) throws DatalessException {
        super(settings);
    }

    @Override
    public String getQuery_stopAutoUpdateStats() {
        return null;
    }

    @Override
    public String getQuery_SelectRelations() {
        return "select name from dbo.sysobjects where type = 'U'";
    }

    @Override
    public String getQuery_dependentRelations(String relation) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_columnDataType(String relation, String column) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getIndexedColumns(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getAttributes(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
            throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Connects to the database instance using the specified database connection settings.
     * @param settings - database settings.
     * @return true, if the connection is successful.
     * @throws DatalessException
     */
    public boolean connect(DBSettings settings) throws DatalessException{
        String connectString;
        if(isConnected())
            return true;
        this.settings = settings;
        try
        {
            Class.forName("com.sybase.jdbc3.jdbc.SybDriver").newInstance();
            //connectString = "jdbc:sybase:Tds:" + settings.getServerName() + ":" +settings.getServerPort()+"/"+settings.getDbName()+"?user="+settings.getUserName()+"&password="+settings.getPassword();
            connectString = "jdbc:sybase:Tds:" + settings.getServerName() + ":" +settings.getServerPort();
            Properties properties = new Properties();
            properties.put ( "user", settings.getUserName() );
            properties.put ( "password", settings.getPassword());
            Constants.CPrintToConsole(connectString, Constants.DEBUG_FIRST_LEVEL_Information);
            con = DriverManager.getConnection(connectString, properties);
        } catch (Exception e)	{
            Constants.CPrintErrToConsole(e);
            throw new DatalessException("Database Engine "+settings.getDbName()+" is not accepting connections");
        }
        if(con != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean retain(String [] dropRelations, String[] dependentRelations) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RelationStatistics getRelationStatistics(String relation) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
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

    @Override
    public Plan getPlan(String query) throws DatalessException {
        throw new UnsupportedOperationException("Not supported yet.");
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