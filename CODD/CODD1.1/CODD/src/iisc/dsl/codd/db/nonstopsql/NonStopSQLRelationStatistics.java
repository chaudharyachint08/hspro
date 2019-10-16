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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package iisc.dsl.codd.db.nonstopsql;

import iisc.dsl.codd.db.RelationStatistics;

/**
 * Non Stop SQL statistics of a relation is represented with this class
 * @author DeepaliNemade
 */
public class NonStopSQLRelationStatistics extends RelationStatistics {

	private static final long serialVersionUID = 5430540300969401091L;
	protected String catalogName; // Catalog name (store in CAPS always and do transformation as required)

    public NonStopSQLRelationStatistics(String relName, String schema, String catalog) {
        super(relName, schema);
        catalogName = catalog.toUpperCase();
    }

    public String getCatalog() {
        return this.catalogName;
    }

    public void setCatalog(String catalog) {
        this.catalogName = catalog;
    }

    @Override
    public String toString() {
        String returnString = "";
        returnString = returnString + "relationName|catalogName|schemaName|cardinality\n";
        returnString = returnString + relationName + "|" + catalogName + "|" + schemaName + "|" +  relCard + "\n";
        return returnString;
    }
}
