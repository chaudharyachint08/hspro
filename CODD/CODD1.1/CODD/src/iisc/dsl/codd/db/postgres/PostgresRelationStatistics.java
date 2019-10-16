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
package iisc.dsl.codd.db.postgres;

import iisc.dsl.codd.db.oracle.*;
import iisc.dsl.codd.db.db2.*;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.mssql.MSSQLRelationStatistics;
import java.math.BigDecimal;

/**
 * Postgres Relation Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class PostgresRelationStatistics extends RelationStatistics{

    /**
     * Catalog Table:  PG_CLASS
     * http://www.postgresql.org/docs/8.4/static/catalog-pg-class.html
     *  relpages - int4 - Size of the on-disk representation of this table in pages (of size BLCKSZ).
     *                    This is only an estimate used by the planner. It is updated by VACUUM, ANALYZE,
     *                    and a few DDL commands such as CREATE INDEX
     *  reltuples - float4 - Number of rows in the table. This is only an estimate used by the planner.
     *                      It is updated by VACUUM, ANALYZE, and a few DDL commands such as CREATE INDEX
     *                      - Total number of rows; Default value 0.
     */
    // reltuples stored in Relation Statistics
     // protected BigDecimal relpages; Stored in RelationStatistics // Total number of pages on which the rows of the table exist

    /**
     * Constructs a PostgresRelationStatistics for the specified relation with the default values.
     * @param relName Relation name
     * @param schema Schema name
     */
    public PostgresRelationStatistics(String relName, String schema)
    {
        super(relName, schema);
        this.relCard = new BigDecimal(BigDecimal.ZERO+"");
        this.relPages = new BigDecimal(BigDecimal.ZERO+"");
    }

    public BigDecimal getRelPages() {
        return relPages;
    }

    public void setRelPages(BigDecimal relpages) {
        this.relPages = relpages;
    }

    /**
     * Initializes PostgresRelationStatistics metadata fields with the specified RelationStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine RelationStatistics metadata to Postgres.
     * @param relStat RelationStatistics
     */
    public void mapForPorting(RelationStatistics relStat)
    {
        super.mapForPorting(relStat);
        if(relStat instanceof DB2RelationStatistics) {
            // local metadata fields: fpages, overflow, active_blocks
            // None of these can be ported to Postgres
        } else if(relStat instanceof OracleRelationStatistics) {
            // local metadata fields: avgRowLen
            // avgRowLen - can not be ported to Postgres
        } else if(relStat instanceof MSSQLRelationStatistics) {
            // local metadata fields: -
        } else if(relStat instanceof PostgresRelationStatistics) {
            // local metadata fields: -
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the PostgresRelationStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        // No Local variable to scale.
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "reltuples|relpages\n";
        ret = ret + this.relCard + "|" + this.relPages + "\n";
        return ret;
    }
}
