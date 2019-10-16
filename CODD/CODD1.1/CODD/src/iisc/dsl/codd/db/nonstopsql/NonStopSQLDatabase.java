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
# WWW: http://dsl.serc.iisc.ernet.in/˜haritsa
#
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package iisc.dsl.codd.db.nonstopsql;

import iisc.dsl.codd.db.DatabaseAbstract;
import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.Main;
import iisc.dsl.codd.client.TransferMode;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.DBSettings;
import iisc.dsl.codd.ds.DataType;
import iisc.dsl.codd.plan.Node;
import iisc.dsl.codd.plan.Plan;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import javax.swing.JOptionPane;

import com.tandem.t4jdbc.SQLMXStatement;

/**
 *
 * @author DeepaliNemade
 */
public class NonStopSQLDatabase extends DatabaseAbstract {

	/*
	 *    TODO: Check what the below comment means ?
	 *    Full code is written assuming that all tables resides in single catalog.
	 *    But the dependent relations may be from different catalog.
	 *    Then properties of dependent relations can be taken from their catalog.
	 */
	//qno used by getPlan
	private int qno;

	public NonStopSQLDatabase(DBSettings settings) throws DatalessException {
		super(settings);
		qno = 0;
	}

	@Override
	public String getQuery_stopAutoUpdateStats() {
		return "";              // Comment Added by Deepali - In SQL/MX statistics are not automatically updated.
	}

	@Override
	public String getQuery_SelectRelations() {
		return "SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName()+ ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND "
				+ "OBJECT_SECURITY_CLASS = 'UT' AND OBJECT_TYPE = 'BT' AND OBJECT_NAME_SPACE = 'TA'";
	}

	@Override
	public String getQuery_dependentRelations(String relation) {
		//Dependent Relations can be from different catalog... That is not handled here
		return "SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_UID IN "
		+ " (SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_UID IN "
		+ "     (SELECT FOREIGN_KEY_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE WHERE UNIQUE_CONSTRAINT_UID IN "
		+ "         (SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_UID IN "
		+ "             (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))) )";
	}

	@Override
	public String getQuery_columnDataType(String relation, String column) {
		// Assumed that the relation is in same catalog everytime.... see if different catalog needs to be considered here or not
		return "SELECT SQL_DATA_TYPE FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS "
		+ " WHERE COLUMN_NAME = '" + column.toUpperCase() + "' AND OBJECT_UID IN (SELECT OBJECT_UID FROM"
		+ " " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')";
	}

	@Override
	public String getQuery_getIndexedColumns(String relation) throws DatalessException {
		return "SELECT C.COLUMN_NAME, O.OBJECT_NAME "
				+ "FROM "
				+ "     " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS O, " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS C "
				+ " WHERE C.OBJECT_UID IN "
				+ "         (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
				+ "       AND C.COLUMN_NUMBER IN ("
				+ "          SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".ACCESS_PATH_COLS"
				+ "          WHERE ACCESS_PATH_UID IN ("
				+ "             SELECT ACCESS_PATH_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".ACCESS_PATHS WHERE ACCESS_PATH_TYPE = 'IX' AND "
				+ "                 TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))) "
				+ "                 AND O.OBJECT_NAME IN ("
				+ "                     SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_UID IN ("
				+ "                         SELECT ACCESS_PATH_UID  FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".ACCESS_PATHS WHERE ACCESS_PATH_TYPE = 'IX' AND "
				+ "                         TABLE_UID IN ("
				+ "                             SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))) AND "
				+ "     O.OBJECT_UID IN (SELECT ACCESS_PATH_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".ACCESS_PATH_COLS WHERE COLUMN_NUMBER = C.COLUMN_NUMBER) "
				+ "     AND C.COLUMN_CLASS <> 'S'";
	}

	@Override
	public String getQuery_getIndexName(String relation, ArrayList<String> cols) throws DatalessException {
		String str = "(";
		for (int i = 0; i < cols.size(); i++) {
			str = "'" + cols.get(i) + "', ";
		}
		str = str + ")";
		/* This query gives all the index names over the relation as output. If only index names corresponding to the input
        column names are required then ADD logic for that in query using str */
		return "SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_UID IN ("
		+ " SELECT ACCESS_PATH_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".ACCESS_PATHS WHERE ACCESS_PATH_TYPE = 'IX' AND "
		+ " TABLE_UID IN ( SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))";

	}

	@Override
	public String getQuery_getAttributes(String relation) throws DatalessException {
		return "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN ( SELECT OBJECT_UID"
				+ " FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "') AND COLUMN_CLASS <> 'S'";
	}

	/**
	 * Returns the query which returns the attributes related to input histogramId.
	 * The attributes can be single dimensional as well as multi-dimensional.
	 * @param relation
	 * @param histogramId
	 * @return
	 * @throws DatalessException
	 */
	public String getQuery_multiColumnAttributes(String relation, long histogramId) throws DatalessException {
		return "SELECT C.COLUMN_NAME FROM  " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS C, " + getCatalog() + "." + getSchema() + ".HISTOGRAMS H WHERE C.OBJECT_UID = H.TABLE_UID AND H.TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND C.COLUMN_NUMBER = H.COLUMN_NUMBER AND H.HISTOGRAM_ID = " + histogramId +  " order by H.COL_POSITION;";
	}

	@Override
	public String getQuery_getPrimaryKeyAttributes(String relation) throws DatalessException {
		return "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
				+ "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
				+ " AND COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN "
				+ "(SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND TABLE_UID IN "
				+ "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')))";
	}

	@Override
	public String getQuery_getFKColumnRefRelation(String relation) throws DatalessException {
		/*Comment Added by Deepali - This query returns foreign key names and base table names....
        Add constraint in it to get base table names corresponding to their repective foreign key attributes
		 */
		/*
        return "SELECT O.OBJECT_NAME, C.COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS O, " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".COLS C WHERE "
        + " C.OBJECT_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "')"
        + " AND C.COLUMN_NUMBER IN ("
        + "SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN ( "
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND TABLE_UID IN ( "
        + " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "')))"
        + " AND O.OBJECT_UID IN (SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID IN "
        + "(SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN ("
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND TABLE_UID IN (SELECT OBJECT_UID FROM "
        + " " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "'))))";
		 */

		/*Correct and complete query*/
		return "SELECT O.OBJECT_NAME, C.COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS O, " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS C "
		+ " WHERE "
		+ " C.OBJECT_UID IN  "
		+ "     (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME ='" + relation.toUpperCase() + "') "
		+ " AND C.COLUMN_NUMBER IN "
		+ "	(SELECT K.COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE K WHERE K.CONSTRAINT_UID IN "
		+ "		(SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' "
		+ "              AND TABLE_UID IN "
		+ "			(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')) "
		+ "      AND O.OBJECT_UID IN "
		+ "     	(SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' "
		+ "             AND CONSTRAINT_UID IN "
		+ "     		(SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID = K.CONSTRAINT_UID)));";
	}

	@Override
	public String getQuery_getForeignKeyAttributes(String relation) throws DatalessException {
		return "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
				+ "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
				+ " AND COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN "
				+ " (SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE TABLE_UID IN ( "
				+ " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "') AND CONSTRAINT_TYPE = 'F'))";
	}

	@Override
	public String getQuery_getUniqueAttributes(String relation) throws DatalessException {
		return "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
				+ "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
				+ " AND COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN "
				+ " (SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE TABLE_UID IN ( "
				+ " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "') AND CONSTRAINT_TYPE = 'U'))";
	}

	@Override
	public String getQuery_getNullableValue(String relation, String column) throws DatalessException {
		/*Comment Added by Deepali - Query written with an assumption that the name of objects with NOT NULL constraints starts with relation name */
		return "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
		+ " (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
		+ " AND COLUMN_NUMBER NOT IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".CK_COL_USAGE WHERE "
		+ " TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "')"
		+ " AND CONSTRAINT_UID IN (SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'C'"
		+ " AND TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))) AND COLUMN_CLASS <> 'S' ";       // Comment Added by Deepali - SYSKEY is one extra SYSTEM DEFINED column. So eliminate.
	}

	@Override
	public String getQuery_getPrimaryKeyRelationAndColumn(String relation, String column) throws DatalessException {
		/*This query is giving all primary key attributes. Refine it to get only those attributes which are reffered bu that particular column*/
		/*  return "SELECT O.OBJECT_NAME, C.COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS O, " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".COLS C WHERE "
        + " C.OBJECT_UID = O.OBJECT_UID AND C.COLUMN_CLASS <> 'S' AND "
        + " C.COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".COLS WHERE OBJECT_UID IN "
        + " (SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID IN "
        + " (SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN ( "
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND TABLE_UID IN ("
        + " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "')))) AND COLUMN_NUMBER IN ( "
        + " SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN ( "
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID IN "
        + " (SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN ( "
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND TABLE_UID IN ( "
        + " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "')))))) "
        + " AND "
        + " O.OBJECT_UID IN (SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID IN "
        + " (SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN ( "
        + " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND TABLE_UID IN ( "
        + " SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation + "'))));";   */

		//New Query to get the primary key column and relation corresponding to a given table and a foreign key
		return "SELECT O.OBJECT_NAME, C.COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS O, " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS C "
		+ " WHERE "
		+ "     C.OBJECT_UID = O.OBJECT_UID "
		+ " AND  "
		+ "     C.COLUMN_NUMBER IN( "
		+ "         SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN  "
		+ "             (SELECT T.TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS T WHERE T.CONSTRAINT_TYPE = 'P' AND T.CONSTRAINT_UID IN  "
		+ "                 (SELECT R.UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE R WHERE R.FOREIGN_KEY_UID IN "
		+ "			(SELECT TC.CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS TC WHERE TC.CONSTRAINT_TYPE = 'F' "
		+ "			 AND TC.TABLE_UID IN "
		+ "					(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "') "
		+ "			AND TC.CONSTRAINT_UID IN  "
		+ "				(SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE COLUMN_NUMBER IN "
		+ "     				(SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID = TC.TABLE_UID AND COLUMN_NAME = '" + column.toUpperCase() + "'))) "
		+ "				) "
		+ "                  AND "
		+ "                  C.COLUMN_NUMBER IN "
		+ "                     (SELECT K.COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE K WHERE K.CONSTRAINT_UID IN  "
		+ "                         (SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID = T.CONSTRAINT_UID)) "
		+ "                 AND  "
		+ "                     O.OBJECT_UID IN  "
		+ "                         (SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' AND CONSTRAINT_UID = T.CONSTRAINT_UID))); ";
	}

	/**
	 * Returns the query which gives the id's of all histograms (one dimensional as well as multi dimensional) available for the input relation.
	 */
	@Override
	public String getMultiColumnHistogramId(String relation) throws DatalessException {
		return "SELECT HISTOGRAM_ID FROM "+ getCatalog() +"."+ getSchema() +".HISTOGRAMS WHERE TABLE_UID IN "
				+ " (SELECT OBJECT_UID FROM "+ getCatalog() +".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "') GROUP BY HISTOGRAM_ID;";
	}

	/**
	 * Returns the query which gives the id of histogram of given input "columns". 
	 * The input variable "columns" will have either one single column,
	 * or a set of columns concatenated by pipe(|).
	 */
	@Override
	public String getColumnHistogramId(String relation, String columns) throws DatalessException {
		int colCount = 0;
		String columnList = "";
		StringTokenizer stok = new StringTokenizer(columns, "|");
		while (stok.hasMoreTokens()) {
			columnList = columnList + "'" + stok.nextToken() + "',";
			colCount++;
		}
		columnList = columnList.substring(0, columnList.length() - 1);

		if (colCount == 1) {
			return "SELECT HISTOGRAM_ID FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS WHERE COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
					+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND COLUMN_NAME IN (" + columnList + ")) and table_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
					+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND COLCOUNT = " + colCount + " GROUP BY HISTOGRAM_ID;";
		} else {
			Statement stmt = createStatement();
			int count = 0;
			try {
				ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT(HISTOGRAM_ID)) FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS WHERE TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_NAME = '" + relation + "') AND COLCOUNT = " + colCount + " GROUP BY HISTOGRAM_ID;");
				if (rs.next()) {
					count = rs.getInt(1);
				}
				rs.close();
				stmt.close();
			} catch (SQLException ex) {
				throw new DatalessException("Error getting multicolumn histogram count.");
			}
			if (count == 1) {
				// Suresh - Bug in query; it always selects the same (smaller histogram id) histogram for multi column updates !
//				return "SELECT HISTOGRAM_ID FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS WHERE COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
//					+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND COLUMN_NAME IN (" + columnList + ")) and TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_NAME = '" + relation + "') AND COLCOUNT = " + colCount + " GROUP BY HISTOGRAM_ID;";
				return "SELECT HISTOGRAM_ID FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS WHERE COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND COLUMN_NAME IN (" + columnList + ")) and TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_NAME = '" + relation + "') AND COLCOUNT = " + colCount + " GROUP BY HISTOGRAM_ID HAVING count(HISTOGRAM_ID) = " +colCount + ";";

			} else {
				// Suresh - Not sure when is the below query used !
				System.out.println("relation" +relation + "columns" + columns);
				throw new DatalessException("Not sure when is the below query used");
				//return "SELECT H2.HISTOGRAM_ID FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS H1, " + getCatalog() + "." + getSchema() + ".HISTOGRAMS H2 WHERE H1.COLUMN_NUMBER NOT IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
				//		+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation + "') AND COLUMN_NAME IN (" + columnList + ")) and H1.table_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_NAME = '" + relation + "')  AND H2.TABLE_UID = H1.TABLE_UID AND H1.COLCOUNT = " + colCount + " AND H1.COLCOUNT = H2.COLCOUNT AND H2.HISTOGRAM_ID != H1.HISTOGRAM_ID GROUP BY H2.HISTOGRAM_ID;";
			}
		}
	}

	/**
	 * Returns the multi-column attributes of the specified relation. Attributes can be single dimensional as well as multi dimensional.
	 * @param relation Relation name
	 * @return Attributes
	 * @throws DatalessException
	 */
	public String[] getMultiColumnAttributes(String relation) throws DatalessException {

		/*
		 * Steps:
		 *  1. Extract the histogram Id's of the multicolumn histograms
		 *  2. Get the column names corresponding to each histogram_id and concatenate them to form one attribute
		 */

		Set<String> set = new HashSet<String>();
		long[] histogramId;
		try {
			Statement stmt = createStatement();
			Statement stm = createStatement();
			String query = null;

			String attribQuery = null;


			query = getMultiColumnHistogramId(relation);
			Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);

			ResultSet rset1 = stmt.executeQuery(query);
			int j = 0;
			while (rset1.next()) {
				j++;
			}
			rset1.close();
			rset1 = stmt.executeQuery(query);
			histogramId = new long[j]; 
			int k = 0;
			while (rset1.next()) {

				histogramId[k] = rset1.getLong(1);
				attribQuery = getQuery_multiColumnAttributes(relation, histogramId[k]);
				k++;
				Constants.CPrintToConsole(attribQuery, Constants.DEBUG_SECOND_LEVEL_Information);
				ResultSet rs = stm.executeQuery(attribQuery);
				String attrib = "";
				while (rs.next()) {
					attrib = attrib + rs.getString(1).trim() + "|";   // Column Names Separator
				}
				if( attrib != "" || !attrib.isEmpty()) 
					attrib = attrib.toUpperCase().substring(0, attrib.length() - 1);   // To remove the last separator
								
				if(!attrib.contains("SYSKEY")){
					set.add(attrib);	// Don't add SYSKEY attribute in the attribute list for a relation.
				}
				rs.close();
			}
			stm.close();
			rset1.close();
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

	@Override
	public boolean connect(DBSettings settings) throws DatalessException {
		try {
			if(Main.isJdbcDriver){
				Class.forName("com.tandem.t4jdbc.SQLMXDriver");
				con = DriverManager.getConnection("jdbc:t4sqlmx://" +settings.getServerName().trim() +":" + settings.getServerPort().trim()+ "/:user=" + settings.getUserName().trim()+";password="+settings.getPassword().trim()+";serverDataSource=TDM_Default_DataSource");
			}else{
				con = new NSConnection();        		
			}
			this.settings = settings;
			return true;
		} catch(ClassNotFoundException e){
			throw new DatalessException("Could not load the JDBC Driver.");
		} catch(SQLException e){
			throw new DatalessException("Could not connect to the Database.");
		} catch (Exception e) {
			throw new DatalessException("Cannot start the SQLCI process.");
		}
	}

	@Override
	public boolean retain(String[] dropRelations, String[] dependentRelations) throws DatalessException {
		/*The process goes as follows:
		 * Lock the Statistics
		 * Disable all FK Constraints
		 * Truncate the relations.
		 * Enable all FK Constraints
		 * Unlock the Statistics??
		 */
		int k = 0;
		int len = dropRelations.length + dependentRelations.length;
		String[] allRelations = new String[len];
		for (int b = 0; b < dropRelations.length; b++) {
			allRelations[k] = dropRelations[b];
			k++;
		}
		for (int b = 0; b < dependentRelations.length; b++) {
			allRelations[k] = dependentRelations[b];
			k++;
		}
		// Table Name, Constraint Name
		ArrayList<String[]> FKConstraints = new ArrayList<String[]>();
		try {

			// 1) Lock the statistics
			/* Statistics are by default not updated automatically. */

			// 2) Disable all FK Constraints
			for (k = 0; k < len; k++) {
				String relation = allRelations[k];
				String query = "SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_UID IN ("
						+ " SELECT CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'F' AND "
						+ " TABLE_UID IN (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'))";
				Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				ResultSet rset = stmt.executeQuery(query);
				while (rset.next()) {
					String constraint = rset.getString(1).trim();
					String[] cons = new String[2];
					cons[0] = relation;
					cons[1] = constraint;
					FKConstraints.add(cons);
					Constants.CPrintToConsole(constraint + ", ", Constants.DEBUG_SECOND_LEVEL_Information);
				}
				Constants.CPrintToConsole("", Constants.DEBUG_SECOND_LEVEL_Information);
				rset.close();
				stmt.close();
			}

			/*
			 * BEFORE DISABLING STORE FK CONSTRAINT NAME, PK RELATION, PK COLUMN
			 *
			 */
			// Write query to get foreign key columns and their corresponding primary key columns and table and complete the query below
			String FKColumns[] = new String[FKConstraints.size()];
			String PKRelation[] = new String[FKConstraints.size()];
			String PKColumns[] = new String[FKConstraints.size()];

			for (int l = 0; l < FKConstraints.size(); l++) {
				FKColumns[l] = "";
				String[] cons = FKConstraints.get(l);
				//Get Foreign key columns
				String query_FKColumns = "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
						+ " (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '<TABLE>') "
						+ " AND COLUMN_NUMBER IN "
						+ " (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN "
						+ " (SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '<FK_CONSTRAINT>' AND OBJECT_TYPE = 'RC'));";
				ResultSet rset = null;
				Statement stm = createStatement();
				query_FKColumns = query_FKColumns.replace("<TABLE>", cons[0]);
				rset = stm.executeQuery(query_FKColumns.replace("<FK_CONSTRAINT>", cons[1]));
				//String FKColumns = "";
				while (rset.next()) {
					FKColumns[l] = FKColumns[l] + rset.getString(1).trim() + ",";
				}
				int length = FKColumns[l].length();
				FKColumns[l] = FKColumns[l].substring(0, length - 1);     // Removing last comma
				stm.close();
				rset.close();

				// Get Primary
				PKRelation[l] = "";
				String query_PKRelation = "SELECT OBJECT_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE OBJECT_UID IN "
						+ "(SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P'  AND CONSTRAINT_UID IN "
						+ "(SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN "
						+ "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '<FK_CONSTRAINT>' AND OBJECT_TYPE = 'RC')));";
				query_PKRelation = query_PKRelation.replace("<FK_CONSTRAINT>", cons[1]);
				ResultSet rs = null;
				Statement st = createStatement();
				//String PKRelation = "";
				rs = st.executeQuery(query_PKRelation);
				while (rs.next()) {
					PKRelation[l] = PKRelation[l] + rs.getString(1).trim();
				}
				st.close();
				rs.close();

				// Get corresponding Primary key columns
				PKColumns[l] = "";
				String query_PKColumns = "SELECT COLUMN_NAME FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".COLS WHERE OBJECT_UID IN "
						+ "	(SELECT TABLE_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".TBL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P'  AND CONSTRAINT_UID IN"
						+ "		(SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN"
						+ "			(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '<FK_CONSTRAINT>' AND OBJECT_TYPE = 'RC')))"
						+ " AND "
						+ "	COLUMN_NUMBER IN "
						+ "		(SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".KEY_COL_USAGE WHERE CONSTRAINT_UID IN"
						+ "			(SELECT UNIQUE_CONSTRAINT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".RI_UNIQUE_USAGE WHERE FOREIGN_KEY_UID IN"
						+ "			(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + ".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '<FK_CONSTRAINT>' AND OBJECT_TYPE = 'RC')))";
				ResultSet rset1 = null;
				Statement stm1 = createStatement();
				//query_PKColumns = query_PKColumns.replace("<TABLE>", cons[0]);
				rset1 = stm1.executeQuery(query_PKColumns.replace("<FK_CONSTRAINT>", cons[1]));
				//String PKColumns = "";
				while (rset1.next()) {
					PKColumns[l] = PKColumns[l] + rset1.getString(1).trim() + ",";
				}
				int lengthPK = PKColumns[l].length();
				PKColumns[l] = PKColumns[l].substring(0, lengthPK - 1);     // Removing last comma
				stm1.close();
				rset1.close();
			}


			/*
			 * Comment Added By Deepali -
			 *   Disabling Foreign Key is preferable. But TBL_CONSTRAINTS is a system metadata table therefore user can not alter it and thus we
			 *   will here drop the foreign key temporarily.
			 *   If there is a way to disable FK then do it.
			 */
			for (int l = 0; l < FKConstraints.size(); l++) {
				String[] cons = FKConstraints.get(l);
				String query = "ALTER TABLE " + getCatalog() + "." + getSchema() + "." + cons[0] + " DROP CONSTRAINT " + cons[1];
				Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				stmt.executeQuery(query);
				stmt.close();
			}

			// 3) Truncate the relations
			for (k = 0; k < len; k++) {
				String relation = allRelations[k];
				String query = "DELETE FROM  " + getCatalog() + "." + getSchema() + "." + relation;
				Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				stmt.executeQuery(query);
				stmt.close();
			}

			// 4) Enable all FK Constraints
			for (int l = 0; l < FKConstraints.size(); l++) {
				String[] cons = FKConstraints.get(l);
				String query = "ALTER TABLE " + getCatalog() + "." + getSchema() + "." + cons[0] + " ADD CONSTRAINT " + cons[1] + " FOREIGN KEY (" + FKColumns[l] + ") REFERENCES " + PKRelation[l] + " (" + PKColumns[l] + ")";
				Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
				Statement stmt = createStatement();
				stmt.executeQuery(query);
				stmt.close();
			}

			// 6) UnLock the statistics
			/*Tables were never Locked*/
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in dropping Relations of " + settings.getDbName());
		}
		return true;
	}

	@Override
	public boolean transfer(String[] relation, Database destDatabase) throws DatalessException {
		return false;
	}

	@Override
	public void construct(String[] relation) throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	// This function can be used to initialize statistics for any table. Scaling mode use this function.
	// TODO: There is another function initializeRelationStatistics() that seems to do the same ! Do we need both ?
	// 1. it generates statistics for all individual columns & 
	//    the minimum multi column stats, if there is a composite PK.
	// 2. If there is data in the table ( it uses the data ) to generate full histograms OR 
	//    its creates the initial empty histogram(2 default rows in HISTOGRAM_INTERVALS)if there 
	//    is no data in the table.
	// 3. TODO: Also we clear all the existing histograms for this given table before we start! Is this required
	//    all the time ?
	@Override
	public void collectStatistics(String relation) throws DatalessException {
		try {
			Statement stmt = createStatement();
			String query = "UPDATE STATISTICS FOR TABLE " + this.getCatalog() + "." + this.getSchema() + "." + relation + " ON EVERY COLUMN CLEAR;";
			Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(query);
			query = "UPDATE STATISTICS FOR TABLE " + this.getCatalog() + "." + this.getSchema() + "." + relation + " ON EVERY COLUMN;";
			Constants.CPrintToConsole(query, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(query);
			stmt.close();
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in updating Statistics for " + relation + " of " + settings.getDbName());
		}

	}

	@Override
	public RelationStatistics getRelationStatistics(String relation) throws DatalessException {
		// Now that transfer and modifying an existing histogram using CODD is supported, we need to get the relation cardinality;
		// We get the stats by preparing a select * on the table, and extracting the cardinality from the explain plan.
		
		NonStopSQLRelationStatistics relStat = new NonStopSQLRelationStatistics(relation, this.getSchema(), this.getCatalog());
		relStat.setCardinality(BigDecimal.ZERO);
		
		
		
		try {
			Statement stmt = createStatement();
			String statementLabel = null;
			
			PreparedStatement preparestmt= (PreparedStatement) prepareStatement(CARDINALITY_QUERY_A.replace("<TABLE>", this.getCatalog()+"."+this.getSchema()+"."+ relation.toUpperCase()));
            statementLabel =((SQLMXStatement)preparestmt).getStatementLabel();
            ResultSet rs = stmt.executeQuery(CARDINALITY_QUERY_B.replace("<LABEL>", "\'" + statementLabel + "\'"));
            
		    if (rs.next()) {
            	
				BigDecimal CARDINALITY = new BigDecimal(rs.getString(1).trim()).setScale(0);
				relStat.setCardinality(CARDINALITY);
				Constants.CPrintToConsole("CARDINALITY", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(CARDINALITY + "", Constants.DEBUG_SECOND_LEVEL_Information);
            }
			rs.close();
			stmt.close();
		} catch (Exception e) {
			
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Relation " + relation.toUpperCase() + " statistics of " + settings.getDbName());

		}
		
	
		
		return relStat;
	}

	@Override
	public ColumnStatistics getColumnStatistics(String relation, String column, BigDecimal tableCard) throws DatalessException {

		/*Check if this function is performing well.*/
		Integer numBuckets = 0;

		String type = getType(relation, column);

		NonStopSQLColumnStatistics colStat = null;
		try {
			colStat = new NonStopSQLColumnStatistics(relation, column, type, getConstraint(relation, column));
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception intializing Column Statistics " + column + " (" + relation + ") of " + settings.getDbName());
		} 
		long histogramId = 0;
		try {

			//gET HISTOGRAM_ID OF THE COLUMN
			Statement stm = createStatement();
			ResultSet rs = stm.executeQuery(getColumnHistogramId(relation, column));

			if (rs.next()) {
				histogramId = rs.getLong(1);
			} else {
				throw new DatalessException("Exception in getting histogram id's");
			}
			rs.close();
			stm.close();

			String command = "SELECT H.LOW_VALUE, H.HIGH_VALUE, H.ROWCOUNT, H.TOTAL_UEC, H.INTERVAL_COUNT FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAMS H WHERE"
					+ " HISTOGRAM_ID = " + histogramId;
			//                    + " TABLE_UID IN "
			//                    + "(SELECT O.OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS O WHERE O.OBJECT_NAME = '" + relation.toUpperCase() + "'"
			//                    + " AND H.COLUMN_NUMBER IN (SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".COLS WHERE COLUMN_NAME = '" + column.toUpperCase() + "'"
			//                    + "                        AND OBJECT_UID = O.OBJECT_UID)) "
			//                    + " AND H.COLCOUNT = 1";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
			if (rset.next()) {
				String LOW_VALUE = rset.getString(1).trim();
				
				// Replace the comma with a pipe, only if for multi column histograms.
				// else, just remove the start and end markers of low_value
				if(column.contains("|"))
					LOW_VALUE = LOW_VALUE.substring(1,LOW_VALUE.length()-1).replace(',', '|');
				else
					LOW_VALUE = LOW_VALUE.substring(1,LOW_VALUE.length()-1);
								
				if (DataType.isString(type))
					LOW_VALUE = LOW_VALUE.replace("'", "");
				String HIGH_VALUE = rset.getString(2).trim();
				
				
				if(column.contains("|"))
					HIGH_VALUE = HIGH_VALUE.substring(1,HIGH_VALUE.length()-1).replace(',', '|');
				else
					HIGH_VALUE = HIGH_VALUE.substring(1,HIGH_VALUE.length()-1);
				
				if (DataType.isString(type))
					HIGH_VALUE = HIGH_VALUE.replace("'", "");
				BigDecimal ROWCOUNT = new BigDecimal(rset.getString(3).trim());               // If int does not work then change datatype to long or bigint
				BigDecimal TOTAL_UEC = new BigDecimal(rset.getString(4).trim());
				Integer INTERVAL_COUNT = new Integer(rset.getString(5).trim());

				colStat.setHIGH_VALUE(HIGH_VALUE);
				colStat.setLOW_VALUE(LOW_VALUE);
				colStat.setColCard(ROWCOUNT);
				colStat.setTOTAL_UEC(TOTAL_UEC);
				colStat.setNumBuckets(INTERVAL_COUNT);
				Constants.CPrintToConsole("LOW_VLAUE|HIGH_VALUE|ROWCOUNT|TOTAL_UEC", Constants.DEBUG_SECOND_LEVEL_Information);
				Constants.CPrintToConsole(LOW_VALUE + "|" + HIGH_VALUE.replace('|', ',') + "|" + ROWCOUNT + "|" + TOTAL_UEC, Constants.DEBUG_SECOND_LEVEL_Information);
			}
			rset.close();
			stmt.close();


		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading Column " + column + " (" + relation + ")");
		}

		/*
		 * Histogram Information
		 */
		try {
			String command = " SELECT HI.INTERVAL_NUMBER, HI.INTERVAL_ROWCOUNT, HI.INTERVAL_UEC, HI.INTERVAL_BOUNDARY " //, HI.STD_DEV_OF_FREQ "
					+ " FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS HI WHERE"
					+ " HI.HISTOGRAM_ID = " + histogramId;
			//            String command = " SELECT HI.INTERVAL_NUMBER, HI.INTERVAL_ROWCOUNT, HI.INTERVAL_UEC, HI.INTERVAL_BOUNDARY " //, HI.STD_DEV_OF_FREQ "
			//                    + " FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS HI, " + getCatalog() + "." + getSchema() + ".HISTOGRAMS H WHERE"
			//                    + " HI.TABLE_UID IN "
			//                    + "	(SELECT O.OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS O WHERE O.OBJECT_NAME = '" + relation.toUpperCase() + "' "
			//                    + "	 AND H.COLUMN_NUMBER IN	(SELECT COLUMN_NUMBER FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".COLS "
			//                    + " WHERE COLUMN_NAME = '" + column.toUpperCase() + "' AND OBJECT_UID = O.OBJECT_UID)"
			//                    + " AND H.TABLE_UID = O.OBJECT_UID AND H.HISTOGRAM_ID = HI.HISTOGRAM_ID AND H.COLCOUNT = 1)";
			Constants.CPrintToConsole(command, Constants.DEBUG_SECOND_LEVEL_Information);

			Statement stmt = createStatement();
			ResultSet rset = stmt.executeQuery(command);
            
			Statement stmt1 = createStatement();
			ResultSet rst = stmt1.executeQuery(command);		
			
			//Calculate number of buckets for the column

			while (rst.next()) {
				numBuckets++;
			}
			
			/* In Transfer Mode, we should not transfer the first row of HISTOGRAM_INTERVALS */
			if (TransferMode.mode == 1)
				numBuckets--;
			
			//Setting NonStop Histogram.
			NonStopSQLHistObject[] hist = new NonStopSQLHistObject[numBuckets];
			int j = 0;
			while (rset.next()) {
				/* In Transfer Mode, we should not transfer the first row of HISTOGRAM_INTERVALS */
				if (TransferMode.mode == 1)
				{
					TransferMode.mode = 0;
					continue;
				}
				Integer interval_number = rset.getInt(1);
				BigDecimal interval_rowcount = new BigDecimal(rset.getString(2).trim());
				BigDecimal interval_uec = new BigDecimal(rset.getString(3).trim());
				String interval_boundary = rset.getString(4).trim();
				interval_boundary = interval_boundary.substring(1,interval_boundary.length()-1);
				if (DataType.isString(type))
					interval_boundary = interval_boundary.replace("'", "");
				NonStopSQLHistObject histObj = new NonStopSQLHistObject(interval_number, interval_rowcount, interval_uec, interval_boundary);

				hist[j] = histObj;
				j++;
				Constants.CPrintToConsole(interval_number + "|" + interval_rowcount + "|" + interval_uec + "|" + interval_boundary, Constants.DEBUG_SECOND_LEVEL_Information);
			}

			colStat.setNonStopSQLHistogram(hist);
			stmt.close();
			stmt1.close();
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Exception in reading column " + column + " (" + relation + ")");
		}
		return colStat;
	}

	@Override
	public IndexStatistics getIndexStatistics(String relation, ArrayList<String> colNames) throws DatalessException {
		NonStopSQLIndexStatistics indexStat = new NonStopSQLIndexStatistics(relation, colNames);
		// String indexName = getIndexName(relation, colNames);
		// String indexStatCommand = "SELECT RECORD_SIZE FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".ACCESS_PATHS WHERE TABLE_UID IN "
		//         + "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + relation.toUpperCase() + "') "
		//         + " AND ACCESS_PATH_TYPE = 'IX' AND ACCESS_PATH_UID IN "
		//         + "(SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion()+ ".OBJECTS WHERE OBJECT_NAME = '" + indexName.toUpperCase() + "' AND OBJECT_TYPE = 'IX')";
		// Constants.CPrintToConsole(indexStatCommand, Constants.DEBUG_SECOND_LEVEL_Information);

		// try {
		//     Statement stmt = createStatement();
		//     ResultSet rset = stmt.executeQuery(indexStatCommand);
		//     while(rset.next()){
		Integer RECORD_SIZE = 350; // new Integer(rset.getInt(1));

		indexStat.setRecord_size(RECORD_SIZE);

		Constants.CPrintToConsole("RECORD_SIZE", Constants.DEBUG_SECOND_LEVEL_Information);
		Constants.CPrintToConsole(RECORD_SIZE + "", Constants.DEBUG_SECOND_LEVEL_Information);
		//      }

		//     rset.close();
		//     stmt.close();
		// } catch (Exception e) {
		//     Constants.CPrintErrToConsole(e);
		//     throw new DatalessException("Exception in reading index Statistics " + indexName + " of " + relation + " Statistics of " + settings.getDbName());
		// }
		return indexStat;
	}

	@Override
	public boolean setRelationStatistics(String relation, RelationStatistics relationStatistics) throws DatalessException {
		return true;
	}

	public boolean initializeRelationStatistics(String relation) throws DatalessException {
		/*
		 *  Flush and initialize HISTOGRAMS and HISTOGRAM_INTERVALS table for all attribute.
		 */
		try{
			String flushCommand = "update statistics for table " + getCatalog() + "." + getSchema() + "." + relation + " on every column clear;";
			Constants.CPrintToConsole(flushCommand, Constants.DEBUG_SECOND_LEVEL_Information);
			Statement st = createStatement();
			st.execute(flushCommand);
			String initializeCommand =  "update statistics for table " + getCatalog() + "." + getSchema() + "." + relation + " on every column;";
			Constants.CPrintToConsole(initializeCommand, Constants.DEBUG_SECOND_LEVEL_Information);
			st.execute(initializeCommand);
			st.close();
		}catch(Exception e){
			throw new DatalessException("Exception in flushing and initializing the HISTOGRAMS and HISTOGRAM_INTERVALS table.\nThe exception is: " + e.getMessage());
		}
		return true;
	}
	@Override
	public boolean setColumnStatistics(String relation, String column, ColumnStatistics columnStatistics, BigDecimal tableCard) throws DatalessException {
		NonStopSQLColumnStatistics colStat = null;
		if (columnStatistics instanceof NonStopSQLColumnStatistics) {
			colStat = (NonStopSQLColumnStatistics) columnStatistics;
		}else{
			throw new DatalessException("Exception in updating column " + column + " (" + relation + ").\nThe exception is: ColumnStatistics object is not of NonStopSQLColumnStatistics type");
		}

		/*
		 * Return true, if TOTAL_UEC, LOW_VALUE or HIGH_VALUE don't have any value.
		 */
		if(colStat.TOTAL_UEC == null || colStat.TOTAL_UEC.compareTo(BigDecimal.ZERO)<= 0 || colStat.LOW_VALUE == null || colStat.LOW_VALUE.trim().equals("") || colStat.HIGH_VALUE == null || colStat.HIGH_VALUE.trim().equals("")){
			return true;
		}
		String[] columnTypes = colStat.getColumnType().split("\\|");
		String[] columns = column.split("\\|");
		String[] lowValues = colStat.getLOW_VALUE().split("\\|");
		String[] highValues = colStat.getHIGH_VALUE().split("\\|");
		if(columnTypes==null || columnTypes.length==0 || columns==null || columns.length!=columnTypes.length){
			throw new DatalessException("Exception in getting the data types for the column: " + column +".");
		}
		/*
		 * Update data into HISTOGRAMS table. 
		 */
		long histogramId = 0;
		try {
			Statement stmt = createStatement();
			ResultSet rs = stmt.executeQuery(getColumnHistogramId(relation, column));
			if (rs.next()) {
				histogramId = rs.getLong(1);
			} else {
				throw new DatalessException("Exception in getting histogram id's for the column: " + column +".");
			}
			rs.close();

			String updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAMS SET " 
									+ " ROWCOUNT = " + colStat.getColCard() + ","
									+ " TOTAL_UEC = " + colStat.getTOTAL_UEC() + ", LOW_VALUE = '(";
			for(int i=0; i<columnTypes.length;i++){
				if (DataType.isString(columnTypes[i])) {
					String tempStr = lowValues[i];
					//tempStr.replaceAll("'''", "''");
					if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
						tempStr = tempStr.substring(0, 30);
					}
					updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
				} else if(DataType.isDate(columnTypes[i])){
					updateCommand = updateCommand + "DATE ''" + Date.valueOf(lowValues[i]) + "'',";
				} else if(DataType.isTime(columnTypes[i])){
					updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(lowValues[i]) + "'',";
				} else {
					updateCommand = updateCommand + lowValues[i] + ",";
				}
			}
			updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")', HIGH_VALUE = '(";
			for(int i=0; i<columnTypes.length;i++){
				if (DataType.isString(columnTypes[i])) {
					String tempStr = highValues[i];
					if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
						tempStr = tempStr.substring(0, 30);
					}
					updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
				} else if(DataType.isDate(columnTypes[i])){
					updateCommand = updateCommand + "DATE ''" + Date.valueOf(highValues[i]) + "'',";
				} else if(DataType.isTime(columnTypes[i])){
					updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(highValues[i]) + "'',";
				} else {
					updateCommand = updateCommand + highValues[i] + ",";
				}
			}
			updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'";
			
			updateCommand = updateCommand + ", INTERVAL_COUNT = " + (colStat.numBuckets==0?1:colStat.numBuckets) + " WHERE HISTOGRAM_ID = " + histogramId;
			Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.executeUpdate(updateCommand);
			stmt.close();
		}catch (Exception e) {
			throw new DatalessException("Exception in updating column " + column + " (" + relation + ").\nThe exception is: " + e.getMessage());
		}

		/*
		 * Insert data into HISTOGRAM_INTERVALS table
		 */
		try {
			NonStopSQLHistObject[] hist = colStat.getNonStopSQLHistogram();
			/*
			 * Update the first two default rows of HISTOGRAM_INTERVALS table and
			 * and insert data for other rows.
			 */
			if(hist!=null && hist.length>0){
				Statement stmt = createStatement();
				// Update 0th row of HISTOGRAM_INTERVALS table
				String updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS SET INTERVAL_ROWCOUNT = 0 , "
						+ "INTERVAL_UEC = 0 , INTERVAL_BOUNDARY = '(";
				for(int i=0; i<columnTypes.length;i++){
					if (DataType.isString(columnTypes[i])) {
						String tempStr = lowValues[i];
						if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
							tempStr = tempStr.substring(0, 30);
						}
						updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
					} else if(DataType.isDate(columnTypes[i])){
						updateCommand = updateCommand + "DATE ''" + Date.valueOf(lowValues[i]) + "'',";
					} else if(DataType.isTime(columnTypes[i])){
						updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(lowValues[i]) + "'',";
					} else {
						updateCommand = updateCommand + lowValues[i] + ",";
					}
				}
				updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'" +
								" WHERE INTERVAL_NUMBER = " + 0 + " AND HISTOGRAM_ID = " + histogramId;
				Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt.execute(updateCommand);
				
				// Update 1st row of HISTOGRAM_INTERVALS table
				String[] boundaryValues = hist[0].getInterval_boundary().split("\\|");
				updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS SET "
						+ "INTERVAL_ROWCOUNT = " + hist[0].getInterval_rowcount() + ", "
						+ "INTERVAL_UEC = " + hist[0].getInterval_uec() + ", INTERVAL_BOUNDARY = '(";
				for(int i=0; i<columnTypes.length;i++){
					if (DataType.isString(columnTypes[i])) {
						String tempStr = boundaryValues[i];
						if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
							tempStr = tempStr.substring(0, 30);
						}
						updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
					} else if(DataType.isDate(columnTypes[i])){
						updateCommand = updateCommand + "DATE ''" + Date.valueOf(boundaryValues[i]) + "'',";
					} else if(DataType.isTime(columnTypes[i])){
						updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(boundaryValues[i]) + "'',";
					} else {
						updateCommand = updateCommand + boundaryValues[i] + ",";
					}
				}
				updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'" +
								" WHERE INTERVAL_NUMBER = " + 1 + " AND HISTOGRAM_ID = " + histogramId;
				Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt.execute(updateCommand);
				// Insert the rest of the rows
				Long table_uid = null;
				String query_getTable_uid = "SELECT OBJECT_UID FROM " + getCatalog() + ".DEFINITION_SCHEMA_VERSION_" + getVersion() + 
						".OBJECTS WHERE SCHEMA_UID IN (SELECT SCHEMA_UID FROM NONSTOP_SQLMX_" + getServerInstanceName() + ".SYSTEM_SCHEMA.SCHEMATA"
						+ " WHERE SCHEMA_NAME = '" + getSchema() + "') AND OBJECT_NAME = '" + relation.toUpperCase() + "'";
				ResultSet rs;

				rs = stmt.executeQuery(query_getTable_uid);
				if (rs.next()) { 
					table_uid = rs.getLong(1);
				} else {
					Constants.CPrintToConsole("CODD Error : Table_uid and histogram_id not found! ", Constants.DEBUG_SECOND_LEVEL_Information);
					throw new DatalessException("Exception in updating column " + column + " (" + relation + ").\nThe exception is: Table_uid and histogram_id not found!");
				}
				rs.close();
				
				/* Ashoke - begin - Removing already present entries in histogram_intervals. 
				 * If data is present in tables, initializeRelationStatistics() creates
				 * histogram entries based on data which needs to be purged, otherwise we run into UNIQUE constraint error. 
				 * we use sql/mx updates stats to generate the histogram "shell" as histogram_id is system generated and we
				 * don't want to figure out the logic of generating histogram_id like sql/mx in the CODD tool 
				 */
				Long interval_count = null;
				String query_getInterval_count = "SELECT COUNT(*) FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS WHERE HISTOGRAM_ID = " + histogramId + " AND TABLE_UID = " + table_uid;
				ResultSet rs2;
				rs2 = stmt.executeQuery(query_getInterval_count);
				
				if (rs2.next()) {
					interval_count = rs2.getLong(1);
				} else {
					Constants.CPrintToConsole("CODD Error : Interval count ", Constants.DEBUG_SECOND_LEVEL_Information);
					throw new DatalessException("Exception in finding interval count for column" + column + " (" + relation + ").\nThe exception is: Interval count query failed");	
				}
				rs2.close();
				if(interval_count > 2) { //Rows 0 and 1 are always present. No need to clear them.
					String delete_intervals = "DELETE FROM " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS WHERE HISTOGRAM_ID = " + histogramId + " AND TABLE_UID = " + table_uid +
							"AND INTERVAL_NUMBER NOT IN (0,1)";  //Not sure if we can hard-code (0,1), but that is how the whole of NonStop code base works for Rows 0 and 1.
					stmt.executeUpdate(delete_intervals);
				}
				/* Ashoke - end */
				for (int i = 1; i < hist.length; i++) {
					boundaryValues = hist[i].getInterval_boundary().split("\\|");
					String insertCommand = "INSERT INTO " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS VALUES "
							+ "(" + table_uid + ", " + histogramId + ", " + hist[i].interval_number + ", " + hist[i].interval_rowcount + ","
							+ hist[i].interval_uec + ", _UCS2'(";
					for(int j=0; j<columnTypes.length;j++){
						if (DataType.isString(columnTypes[j])) {
							String tempStr = boundaryValues[j];
							if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
								tempStr = tempStr.substring(0, 30);
							}
							insertCommand = insertCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
						} else if(DataType.isDate(columnTypes[j])){
							insertCommand = insertCommand + "DATE ''" + Date.valueOf(boundaryValues[j]) + "'',";
						} else if(DataType.isTime(columnTypes[j])){
							insertCommand = insertCommand + "TIMESTAMP ''" + Timestamp.valueOf(boundaryValues[j]) + "'',";
						}  else {
							insertCommand = insertCommand + boundaryValues[j] + ",";
						}
					}
					insertCommand = insertCommand.substring(0,insertCommand.length()-1) + ")', 0.00, 0, 0, 0, 0, _UCS2'(0)', _UCS2'(0)')";
					// Suresh -  Incorrect;   we need to delete all existing entries ( part from first 2 entires - 0, 1) and then insert the new internals
					// This bug happens when we try to do update statistics with data already present for the table;
					
					Constants.CPrintToConsole(insertCommand, Constants.DEBUG_SECOND_LEVEL_Information);
					stmt.execute(insertCommand);
				}
				stmt.close();
			}else{
				/*
				 * In this case, there will be two default buckets which we need to update with low value and high value
				 */
				Statement stmt = createStatement();
				// Update 0th row of HISTOGRAM_INTERVALS table
				String updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS SET INTERVAL_ROWCOUNT = 0 , "
						+ "INTERVAL_UEC = 0 , INTERVAL_BOUNDARY = '(";
				for(int i=0; i<columnTypes.length;i++){
					if (DataType.isString(columnTypes[i])) {
						String tempStr = lowValues[i];
						if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
							tempStr = tempStr.substring(0, 30);
						}
						updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
					} else if(DataType.isDate(columnTypes[i])){
						updateCommand = updateCommand + "DATE ''" + Date.valueOf(lowValues[i]) + "'',";
					} else if(DataType.isTime(columnTypes[i])){
						updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(lowValues[i]) + "'',";
					} else {
						updateCommand = updateCommand + lowValues[i] + ",";
					}
				}
				updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'" +
								" WHERE INTERVAL_NUMBER = " + 0 + " AND HISTOGRAM_ID = " + histogramId;
				Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt.execute(updateCommand);
				
				// Update 1st row of HISTOGRAM_INTERVALS table
				updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS SET INTERVAL_ROWCOUNT = " + colStat.getColCard() + ", "
						+ "INTERVAL_UEC = " + colStat.TOTAL_UEC + " , INTERVAL_BOUNDARY = '(";
				for(int i=0; i<columnTypes.length;i++){
					if (DataType.isString(columnTypes[i])) {
						String tempStr = highValues[i];
						if(tempStr.length()>30){			// We are doing this since metadata tables store only first 30 characters.
							tempStr = tempStr.substring(0, 30);
						}
						updateCommand = updateCommand + "''" + tempStr.replaceAll("'", "''") + "'',";
					} else if(DataType.isDate(columnTypes[i])){
						updateCommand = updateCommand + "DATE ''" + Date.valueOf(highValues[i]) + "'',";
					} else if(DataType.isTime(columnTypes[i])){
						updateCommand = updateCommand + "TIMESTAMP ''" + Timestamp.valueOf(highValues[i]) + "'',";
					} else {
						updateCommand = updateCommand + highValues[i] + ",";
					}
				}
				updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'" +
								" WHERE INTERVAL_NUMBER = " + 1 + " AND HISTOGRAM_ID = " + histogramId;
				Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
				stmt.execute(updateCommand);
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "Exception in updating histograms of column " + column + " (" + relation + ")", e.getMessage() , JOptionPane.INFORMATION_MESSAGE);
		}
		return true;
	}

	/**
	 * This method is specifically to be used in scaling.
	 * @param relation
	 * @param column
	 * @param columnStatistics
	 * @param tableCard
	 * @return
	 * @throws DatalessException
	 */
	public boolean setColumnStatisticsForScaling(String relation, String column, ColumnStatistics columnStatistics) throws DatalessException {
		NonStopSQLColumnStatistics colStat = null;
		if (columnStatistics instanceof NonStopSQLColumnStatistics) {
			colStat = (NonStopSQLColumnStatistics) columnStatistics;
		}else{
			throw new DatalessException("Exception in updating column " + column + " (" + relation + ").\nThe exception is: ColumnStatistics object is not of NonStopSQLColumnStatistics type");
		}

		/*
		 * Return true, if TOTAL_UEC, LOW_VALUE or HIGH_VALUE don't have any value.
		 */
		if(colStat.TOTAL_UEC == null || colStat.TOTAL_UEC.compareTo(BigDecimal.ZERO)<= 0 || colStat.LOW_VALUE == null || colStat.LOW_VALUE.trim().equals("") || colStat.HIGH_VALUE == null || colStat.HIGH_VALUE.trim().equals("")){
			return true;
		}
		String[] columnTypes = colStat.getColumnType().split("\\|");
		String[] columns = column.split("\\|");
		String[] lowValues = colStat.getLOW_VALUE().split("\\|");
		String[] highValues = colStat.getHIGH_VALUE().split("\\|");
		if(columnTypes==null || columnTypes.length==0 || columns==null || columns.length!=columnTypes.length){
			throw new DatalessException("Exception in getting the data types for the column: " + column +".");
		}
		/*
		 * Update data into HISTOGRAMS table. 
		 */
		long histogramId = 0;
		try {
			Statement stmt = createStatement();
			ResultSet rs = stmt.executeQuery(getColumnHistogramId(relation, column));
			if (rs.next()) {
				histogramId = rs.getLong(1);
			} else {
				throw new DatalessException("Exception in getting histogram id's for the column: " + column +".");
			}
			rs.close();

			String updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAMS SET " 
									+ " ROWCOUNT = " + colStat.getColCard() + ","
									+ " TOTAL_UEC = " + colStat.getTOTAL_UEC() + ", LOW_VALUE = '(";
			for(int i=0; i<columnTypes.length;i++){
				if (DataType.isString(columnTypes[i]) || DataType.isDate(columnTypes[i])) {
					updateCommand = updateCommand + lowValues[i].replace("'", "''") + ",";
				} else {
					updateCommand = updateCommand + lowValues[i] + ",";
				}
			}
			updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")', HIGH_VALUE = '(";
			for(int i=0; i<columnTypes.length;i++){
				if (DataType.isString(columnTypes[i]) || DataType.isDate(columnTypes[i])) {
					updateCommand = updateCommand + highValues[i].replace("'", "''") + ",";
				} else {
					updateCommand = updateCommand + highValues[i] + ",";
				}
			}
			updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'";
			
			updateCommand = updateCommand + ", INTERVAL_COUNT = " + (colStat.numBuckets==0?1:colStat.numBuckets) + " WHERE HISTOGRAM_ID = " + histogramId;
			Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
			stmt.execute(updateCommand);
			stmt.close();
		}catch (Exception e) {
			throw new DatalessException("Exception in updating column " + column + " (" + relation + ").\nThe exception is: " + e.getMessage());
		}

		/*
		 * Insert data into HISTOGRAM_INTERVALS table
		 */
		try {
			NonStopSQLHistObject[] hist = colStat.getNonStopSQLHistogram();
			/*
			 * Since in case of scaling, histograms are already available in database,
			 * therefore, we just need to update their values.
			 */
			if(hist!=null && hist.length>0){
				Statement stmt = createStatement();
				String updateCommand;
				//boolean flag = true;
				// scaling marker field '0' is harmless, as its zero anyways
				for (int i = 0; i < hist.length; i++) {
					String[] boundaryValues = hist[i].getInterval_boundary().split("\\|");
					updateCommand = "UPDATE " + getCatalog() + "." + getSchema() + ".HISTOGRAM_INTERVALS SET "
							+ "INTERVAL_ROWCOUNT = " + hist[i].getInterval_rowcount() + ", "
							+ "INTERVAL_UEC = " + hist[i].getInterval_uec() + ", INTERVAL_BOUNDARY = '(";
					for(int j=0; j<columnTypes.length;j++){
						if (DataType.isString(columnTypes[j]) || DataType.isDate(columnTypes[j])) {
							updateCommand = updateCommand + boundaryValues[j].replace("'", "''") + ",";
						} else {
							updateCommand = updateCommand + boundaryValues[j] + ",";
						}
					}
					updateCommand = updateCommand.substring(0,updateCommand.length()-1) + ")'" +
									" WHERE INTERVAL_NUMBER = " + i + " AND HISTOGRAM_ID = " + histogramId;
					Constants.CPrintToConsole(updateCommand, Constants.DEBUG_SECOND_LEVEL_Information);
					stmt.execute(updateCommand);
				}
				stmt.close();
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "Exception in updating histograms of column " + column + " (" + relation + ")", e.getMessage() , JOptionPane.INFORMATION_MESSAGE);
		}
		return true;
	}

	@Override
	public boolean setIndexStatistics(String relation, ArrayList<String> colNames, IndexStatistics indexStatistics) throws DatalessException {
		return true;
	}

	@Override
	public Plan getPlan(String query) throws DatalessException {
		Plan plan, revPlan;
		ResultSet rset;
		Statement stmt;
		String statementLable = null;
		PreparedStatement preparestmt = null;
		int qno = this.qno;
		// fire Query
		try {

			stmt = createStatement();
			//            stmt.execute("control query default generate_explain 'on'");
			//
			//            preparestmt = (PreparedStatement) con.prepareStatement(query.toUpperCase().replace('\r', ' ').replace('\n', ' '));
			//            statementLable = ((SQLMXStatement) preparestmt).getStatementLabel();
			stmt.executeQuery("set schema " + this.getCatalog()+ "." + this.getSchema() + ";");
			stmt.executeUpdate("prepare XYZ from " + query.toUpperCase().replace('\r', ' ').replace('\n', ' '));
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Database: Error explaining query : " + e);
		}

		// Initialize
		plan = new Plan();
		revPlan = new Plan();

		//        String planQuery = "SELECT A.SEQ_NUM, B.SEQ_NUM PARENT_ID, A.OPERATOR, A.TNAME, A.OPERATOR_COST, A.CARDINALITY, A.TOTAL_COST, A.DESCRIPTION "
		//                    + " FROM TABLE  (explain(NULL, \'" + statementLable + "\')) A, TABLE (EXPLAIN(NULL, \'" + statementLable + "\')) B"
		//                    + " WHERE A.SEQ_NUM = B.LEFT_CHILD_SEQ_NUM OR A.SEQ_NUM = B.RIGHT_CHILD_SEQ_NUM OR "
		//                    + " ( A.OPERATOR = 'ROOT' AND "
		//                    + " B.SEQ_NUM IN (SELECT MIN(C.SEQ_NUM) FROM TABLE (EXPLAIN(NULL, \'" + statementLable + "\')) C));";
		String planQuery = "SELECT A.SEQ_NUM, B.SEQ_NUM PARENT_ID, A.OPERATOR, A.TNAME, A.OPERATOR_COST, A.CARDINALITY, A.TOTAL_COST, A.DESCRIPTION "
				+ " FROM TABLE  (explain(NULL, 'XYZ')) A, TABLE (EXPLAIN(NULL, 'XYZ')) B"
				+ " WHERE A.SEQ_NUM = B.LEFT_CHILD_SEQ_NUM OR A.SEQ_NUM = B.RIGHT_CHILD_SEQ_NUM OR "
				+ " ( A.OPERATOR = 'ROOT' AND "
				+ " B.SEQ_NUM IN (SELECT MIN(C.SEQ_NUM) FROM TABLE (EXPLAIN(NULL, 'XYZ')) C));";
		try {
			// getting information from plan_table table to get plan
			// tree information
			Node node, rootNode;
			java.util.Hashtable HT;// = new java.util.Hashtable();
			stmt = createStatement();
			rset = stmt.executeQuery(planQuery);
			//System.err.println(planQuery + ";");
			int curNode = 0;    // First node is set to 1 because ROOT is not the first node in result set
			String operation, option;
			int indexId = 100;
			rootNode = new Node();
			while (rset.next()) {
				node = new Node();
				/*
				 * Warning: Update from planQuery
				 * The following is the ordering of information accessed from NonStopSQL explain tables
				 * 1: Id
				 * 2: Parent Id
				 * 3: Operation
				 * 4: Object Name
				 * 5: NodeCost
				 * 6: Cardinality
				 * 7: Total Cost
				 */
				int nodeid, parentid = -1;

				nodeid = rset.getInt(1);
				node.setId(nodeid);         //Extract NodeId from result set

				operation = rset.getString(3).trim();
				//System.out.println("operation : " + operation);
				if (operation.equals("ROOT")) {
					//System.out.println("Root Node. Set Parent -1");
					int i = -5;
					node.setParentId(i);
				} else {
					//if (curNode == 0) {
					//    node.setParentId(-1);
					//} else {
					parentid = rset.getInt(2);
					node.setParentId(parentid);   //Extract ParentId of node from result set
					//}
				}
				//Extract Node operator from resultSet
				/*
                if(operation.contains("ROOT"))
                {
                System.out.println("Set Total Cost of Root : " + rset.getDouble(7));
                node.setCost(rset.getDouble(7));
                }else{
                node.setCost(rset.getDouble(5));        //Extract Total Node Cost from result Set
                }*/

				// Set the COST of a node Equals to its Total_Cost
				node.setCost(rset.getDouble(7));
				//System.out.println("Total cost = " + rset.getDouble(7));
				//System.out.println("Node cost : " + node.getCost());



				//node.setCard(rset.getDouble(6));        //Extract Node Cardinality from Result Set

				// Set cardinality of node equals to its max_card_est
				String description = rset.getString(8).trim();
				String tname = rset.getString(4).trim();
				/*
                if(operation.contains("SCAN"));
                {

                System.out.println(description + "\n " + nodeid + " : " + tname);
                }
				 */
				long maxCard = parseForCardinality(description);
				node.setCard(maxCard);

				//System.out.println("Id : PArentId : Operator : TableName : Cost : Cardinality ");
				// System.out.println(rset.getInt(1) + " : " + rset.getInt(2) + " : "
				//         + rset.getString(3) + " : " + rset.getString(4) + " : " + rset.getDouble(5) + " : "
				//        + rset.getDouble(6));
				node.setName(operation);

				//System.out.println("Operation : " + node.getName() + "  :   Card : " + node.getCard());

				//REmove this comment if ROOT needs to be necessarily oth node of plan

				//if(operation.equals("ROOT")){
				// Adding ROOT node to plan
				//    plan.setNode(node, 0);
				//}else{
				// Adding other nodes in plan
				//if(operation.contains("ROOT")){
				//    rootNode = node;   // Add root node at end to plan
				//}else{
				/*
                plan.setNode(node, curNode);
                System.out.println("Node Name : " + node.getName() + " curNode : " + curNode );
                curNode++;
				 *
				 */
				//}
				//}

				//plan.setNode(node, curNode);
				//curNode++;


				if (operation.contains("SCAN")) {
					HT = new java.util.Hashtable();
					HT.put("" + rset.getInt(1), tname);

					ResultSet rs;
					Statement stm;
					stm = createStatement();
					rs = stm.executeQuery("SELECT SEQ_NUM FROM TABLE(EXPLAIN(NULL, \'" + statementLable + "\')) where operator = 'ROOT'");
					/* int k = 50;
                    if(rs.next()){
                    k = rs.getInt(1);           // check it - Deepali
                    }
					 *
					 */
					int k = -1;
					Node tNode;
					for (java.util.Enumeration enum1 = HT.keys(); enum1.hasMoreElements();) {
						int pid = Integer.parseInt((String) enum1.nextElement());
						tNode = new Node();
						tNode.setId(k);        // k = -1 for leaf nodes (Relation Names)
						tNode.setParentId(pid);
						String st = (String) HT.get("" + pid);
						StringTokenizer stok = new StringTokenizer(st, ".");
						stok.nextToken();
						stok.nextToken();
						tNode.setName(stok.nextToken());
						//System.out.println("node.Name : "  + node.getName());

						//                                PreparedStatement preparestmt1 = (PreparedStatement) con.prepareStatement(CARDINALITY_QUERY_A.replace("<TABLE>", st.toUpperCase()));
						//                                String stmtLable = ((SQLMXStatement) preparestmt1).getStatementLabel();
						//                                rs = stm.executeQuery(CARDINALITY_QUERY_B.replace("<TABLE>", st.toUpperCase()).replace("<LABLE>", "\'" + stmtLable + "\'"));

						stm.executeQuery(CARDINALITY_QUERY_A.replace("<TABLE>", st));
						rs = stm.executeQuery(CARDINALITY_QUERY_B.replace("<TABLE>", st));


						// rs = stm.executeQuery(CARDINALITY_QUERY.replace("<TABLE>", st));
						/*         long card = 0;
                        if(rset.next()){
                        card = (long)rset.getDouble(1);
                        }
						 */
						tNode.setCost(0.0);

						//tNode.setCard(0);
						tNode.setCard(node.getCard());   // See if it is taking card of its parent sCAN
						plan.setNode(tNode, curNode);
						//                                System.out.println("Node Name : " + tNode.getName() + "  curNode : " + curNode + "  nodeID : " + tNode.getId());
						curNode++;
						// preparestmt1.close();

					}
				}
				// If operation is SCAN then Table name will be added before scan operation
				plan.setNode(node, curNode);
				//                    System.out.println("Node Name : " + node.getName() + " curNode : " + curNode + "  nodeID : " + node.getId() );
				curNode++;

			}

			//plan.setNode(rootNode, curNode);


			//Get nodes from plan in reverse order and insert in revPlan
			Node n;
			int indexRev = 0;
			int count = 0;
			for (int i = curNode - 1; i >= 0; i--) {
				n = plan.getNode(i);
				count++;
				revPlan.setNode(n, indexRev);
				indexRev++;
			}


			//rset.close();
			//stmt.close();

			/* The below portion of code is moved inside for loop for testing */
			//            ResultSet rs;
			//            Statement stm ;
			//            stm = createStatement();
			//            rs = stm.executeQuery("SELECT SEQ_NUM FROM TABLE(EXPLAIN(NULL, \'" + statementLable + "\')) where operator = 'ROOT'");
			//           /* int k = 50;
			//            if(rs.next()){
			//                k = rs.getInt(1);           // check it - Deepali
			//            }
			//             *
			//             */
			//            int k = -1;
			//            for (java.util.Enumeration enum1 = HT.keys(); enum1.hasMoreElements();) {
			//                int pid = Integer.parseInt((String) enum1.nextElement());
			//                node = new Node();
			//                node.setId(k);        // k = -1 for leaf nodes (Relation Names)
			//                node.setParentId(pid);
			//                String st = (String) HT.get("" + pid);
			//                StringTokenizer stok = new StringTokenizer(st, ".");
			//                stok.nextToken();
			//                stok.nextToken();
			//                node.setName(stok.nextToken());
			//                //System.out.println("node.Name : "  + node.getName());
			//                stm.executeQuery(CARDINALITY_QUERY_A.replace("<TABLE>", st));
			//                rs = stm.executeQuery(CARDINALITY_QUERY_B.replace("<TABLE>", st));
			//                // rs = stm.executeQuery(CARDINALITY_QUERY.replace("<TABLE>", st));
			//                long card = 0;
			//                if(rset.next()){
			//                    card = (long)rset.getDouble(1);
			//                }
			//                node.setCost(0.0);
			//
			//                node.setCard(0);
			//                plan.setNode(node, curNode);
			//                curNode++;
			//            }


			//Get nodes from plan in reverse order and insert in revPlan
			//Node n;
			//int indexRev = 0;
			//System.out.println("curNode : " + curNode + "   count : " + count);
			for (int i = curNode - 1; i >= count; i--) {   // See if for llop condition works properly
				n = plan.getNode(i);
				revPlan.setNode(n, indexRev);
				indexRev++;
			}


			//curNode++;
			//rs.close();
			//stm.close();

			//System.out.println("plan size : " + plan.getSize());


		} catch (SQLException e) {
			Constants.CPrintErrToConsole(e);
			throw new DatalessException("Database: Error accessing plan: " + e);
		}

		//return plan;
		return revPlan;
	}

	public long parseForCardinality(String description) {
		long card1 = 0;
		//System.out.println(description);
		StringTokenizer stok = new StringTokenizer(description);
		while (stok.hasMoreTokens()) {
			if (stok.nextToken().contains("max_card_est:")) {
				//System.out.println(st.nextToken().indexOf("EST_TOTAL_COST"));
				//st.nextToken();
				String op1 = stok.nextToken();
				//System.out.println(" card : " + op1);
				//String stt = op.substring(0, op.indexOf("EST_TOTAL_COST"));
				card1 = (long) Double.parseDouble(op1.replace(",", ""));
				break;
			}
		}
		//System.out.println(card1);
		return card1;
	}

	/* Comment Added By : Deepali Nemade
	 * DO NOT MODIFY BELOW
	 *
	 */
	//    public static final String EXPLAIN_TREE_QUERY = "SELECT A.SEQ_NUM, B.SEQ_NUM PARENT_ID, A.OPERATOR, A.TOTAL_COST, A.CARDINALITY "
	//                    + " FROM TABLE  (explain(NULL, 'XYZ')) A, TABLE (EXPLAIN(NULL, 'XYZ')) B"
	//                    + " WHERE A.SEQ_NUM = B.LEFT_CHILD_SEQ_NUM OR A.SEQ_NUM = B.RIGHT_CHILD_SEQ_NUM);";

	// Suresh - We cannot have a prepare being used within a JDBC executeQuery()
	//public static final String CARDINALITY_QUERY_A = "PREPARE QRY2 FROM SELECT * FROM <TABLE> ;";
    //public static final String CARDINALITY_QUERY_B = "SELECT CARDINALITY FROM TABLE(EXPLAIN(NULL, 'QRY2')) WHERE OPERATOR = 'ROOT';";
      public static final String CARDINALITY_QUERY_A = "SELECT * FROM <TABLE> ;";
	  public static final String CARDINALITY_QUERY_B = "SELECT CARDINALITY FROM TABLE(EXPLAIN(NULL, <LABEL>)) WHERE OPERATOR = 'ROOT';";
	  public static final String CARDINALITY_QUERY = CARDINALITY_QUERY_A + " \r\n\r\n " + CARDINALITY_QUERY_B;

	@Override
	public boolean setHardwareStatistics() throws DatalessException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
}
