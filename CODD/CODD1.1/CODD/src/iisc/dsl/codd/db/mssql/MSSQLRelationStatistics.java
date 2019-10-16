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

import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.db2.DB2RelationStatistics;
import iisc.dsl.codd.db.oracle.OracleRelationStatistics;
import iisc.dsl.codd.db.postgres.PostgresRelationStatistics;
import java.math.BigDecimal;

/**
 * MSSQL Relation Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class MSSQLRelationStatistics extends RelationStatistics{

    /**
     *  CARD - Total number of rows; -1 if statistics are not collected.
     *  PAGECOUNT
     */

    // Integer pageCount; Stored in RelationStatistics // Page Count

    /**
     * Constructs a MSSQLRelationStatistics for the specified relation with the default values.
     * @param relName Relation name
     * @param schema Schema name
     */
    public MSSQLRelationStatistics(String relName, String schema)
    {
        super(relName, schema);
        this.relCard = new BigDecimal(BigDecimal.ZERO+"");
        this.relPages = new BigDecimal(BigDecimal.ZERO+""); // pageCount
    }

    public Integer getPageCount() {
        return relPages.intValue();
    }

    public void setPageCount(Integer pageCount) {
        this.relPages = new BigDecimal(pageCount+""); // pageCount
    }

    /**
     * Initializes MSSQLRelationStatistics metadata fields with the specified RelationStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine RelationStatistics metadata to MSSQL.
     * This will not be used as there is no construct mode support for MSSQL.
     * @param relStat RelationStatistics
     */
    public void mapForPorting(RelationStatistics relStat)
    {
        super.mapForPorting(relStat);
        if(relStat instanceof DB2RelationStatistics) {
            // local metadata fields: fpages, overflow, active_blocks
            // None of these can be ported to MSSQL
        } else if(relStat instanceof OracleRelationStatistics) {
            // local metadata fields: avgRowLen
            // avgRowLen - can not be ported to MSSQL
        } else if(relStat instanceof MSSQLRelationStatistics) {
            // local metadata fields: -
        } else if(relStat instanceof PostgresRelationStatistics) {
            // local metadata fields: -
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the MSSQLRelationStatistics attributes by the scale factor.
     * This will not be used as there is no construct mode support for MSSQL.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "card|pageCount\n";
        ret = ret + this.relCard + "|"+this.relPages+"\n";
        return ret;
    }
}
