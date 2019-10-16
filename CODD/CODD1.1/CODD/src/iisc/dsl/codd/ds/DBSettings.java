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

import iisc.dsl.codd.db.DBConstants;
import java.io.Serializable;

/**
 * The connection settings to a database is represented with this class.*
 * @author dsladmin
 */
public class DBSettings implements Serializable{
    /**
     * String attribute representing the Database Vendor Name. This is must
     * be one of the Constants from DBConstants.
     */
    private String dbVendor;
    /**
     * String attribute representing the database server name / machine name.
     */
    private String serverName;
    /**
     * String attribute representing the database server port number.
     */
    private String serverPort;
    /**
     * String attribute representing the database name.
     */
    private String databaseName;
    /**
     * String attribute representing the database schema name.
     */
    private String schemaName;
    /**
     * String attribute representing the database user name.
     */
    private String userName;
    /**
     * String attribute representing the database user password for 'userName'.
     */
    private String userPassword;

    /**
     * String attribute representing the SQL Server or Non Stop SQL Instance name, which is required for retain and intra-transfer.
     */
    private String sqlServerInstanceName;

    /**
     * String attribute representing the database catalog name.
     */
    private String catalogName;

    /**
     * String attribute representing the database version number.
     */
    private String version;

    /**
     * Constructor for DBSettings class.
     * @param dbServerName - database Server Name / Machine Name
     * @param dbServerPort - database Server Port
     * @param dbType - database Vendor Name
     * @param dbName - database Name
     * @param dbSchema - database Schema Name
     * @param dbUserName - database User Name
     * @param dbPassword  - database User Password
     */
    public DBSettings(String dbServerName, String dbServerPort, String dbType, String dbName, String dbSchema, String dbUserName, String dbPassword) {
        this.serverName = dbServerName;
        this.serverPort = dbServerPort;
        this.dbVendor = dbType;
        this.databaseName = dbName;
        if(dbVendor.equals(DBConstants.POSTGRES)) {
            this.schemaName = dbSchema;
        } else {
            this.schemaName = dbSchema.toUpperCase();
        }
        this.userName = dbUserName;
        this.userPassword = dbPassword;
        this.sqlServerInstanceName = new String();
        this.version = new String();
        this.catalogName = new String();
    }

    public DBSettings(String dbServerName, String dbServerPort, String dbType, String dbName, String dbSchema, String dbCatalog, String dbUserName, String dbPassword, String serverInstance, String dbVersion) {
        this.serverName = dbServerName;
        this.serverPort = dbServerPort;
        this.dbVendor = dbType;
        this.databaseName = dbName;
        this.schemaName = dbSchema.toUpperCase();
        this.catalogName = dbCatalog.toUpperCase();
        this.userName = dbUserName;
        this.userPassword = dbPassword;
        //this.sqlServerInstanceName = new String();
        this.sqlServerInstanceName = serverInstance;
        this.version = dbVersion;
    }

    /**
     * Replaces the database vendor name with the specified database vendor name.
     * @param dbVendor - database Vendor name
     */
    public void setDbType(String dbVendor) {
        this.dbVendor = dbVendor;
    }

    /**
     * Returns the database vendor name.
     * @return dbVendor
     */
    public String getDbVendor() {
        return dbVendor;
    }

    /**
     * Replaces the database server name with the specified database server name.
     * @param serverName - database server name
     */
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    /**
     * Returns the database server name.
     * @return serverName
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Replaces the database server port number with the specified database server port number.
     * @param serverPort - database server port number
     */
    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    /**
     * Returns the database server port number.
     * @return serverPort
     */
    public String getServerPort() {
        return serverPort;
    }

    /**
     * Replaces the database server user name with the specified database server user name.
     * @param userName - database user name
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the database server user name.
     * @return userName
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Replaces the database server user password with the specified database server user password.
     * @param password - database user password.
     */
    public void setPassword(String userPassword) {
        this.userPassword = userPassword;
    }

    /**
     * Returns the database server user password.
     * @return userPassword
     */
    public String getPassword() {
        return userPassword;
    }

    /**
     * Replaces the database schema name with the specified database schema name.
     * @param schema - database schema name.
     */
    public void setSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * Returns the database schema name.
     * @return schemaName
     */
    public String getSchema() {
        return schemaName;
    }


    /**
     * Replaces the database catalog name with the specified database catalog name.
     * @param catalog - database catalog name.
     */
    public void setCatalog(String catalogName) {
        this.catalogName = catalogName;
    }

    /**
     * Returns the database catalog name.
     * @return schemaName
     */
    public String getCatalog() {
        return catalogName;
    }

    /**
     * Replaces the database name with the specified database name.
     * @param dbName - database name
     */
    public void setDbName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the database name.
     * @return - databaseName
     */
    public String getDbName() {
        return databaseName;
    }

    /**
     * Replaces the SQL Server Instance Name with the specified SQL Server Instance Name.
     * @param sqlServerInstanceName - SQL Server Instance Name
     */
    public void setSqlServerInstanceName(String sqlServerInstanceName) {
        this.sqlServerInstanceName = sqlServerInstanceName;
    }

    /**
     * Returns the SQL Server Instance name.
     * @return - sqlServerInstanceName
     */
    public String getSqlServerInstanceName() {
        return sqlServerInstanceName;
    }

    /**
     * Returns the version number.
     * @return - version number
     */
    public String getVersion() {
        return version;
    }

    /**
     * Replaces the version number with the specified version number.
     * @param Non STop SQL version number - version
     */
    public void setVersion(String version) {
        this.version = version;
    }


}
