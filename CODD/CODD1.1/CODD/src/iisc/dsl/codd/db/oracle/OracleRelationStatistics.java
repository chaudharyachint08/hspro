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
package iisc.dsl.codd.db.oracle;

import iisc.dsl.codd.db.db2.*;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.mssql.MSSQLRelationStatistics;
import iisc.dsl.codd.db.postgres.PostgresRelationStatistics;
import java.math.BigDecimal;

/**
 * Oracle Relation Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class OracleRelationStatistics extends RelationStatistics{

    /**
     * View: ALL_TABLES, COLUMNS
     * http://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2094.htm#I1020277
     *  CARD - Total number of rows; Default value 0.
     *  BLOCKS - Total number of blocks on which the rows of the table exist; Default value 0.
     *  AVG_ROW_LEN - Average row length. Default value 0.
     */
    //protected BigDecimal blocks; - Stored in RelationStatistics // Total number of blocks on which the rows of the table exist
    protected BigDecimal avgRowLen; // Average row length

    /**
     * Constructs a OracleRelationStatistics for the specified relation with the default values.
     * @param relation Relation name
     * @param schema Schema name
     */
    public OracleRelationStatistics(String relName, String schema)
    {
        super(relName, schema);
        this.relCard = new BigDecimal(BigDecimal.ZERO+"");
        this.relPages = new BigDecimal(BigDecimal.ZERO+""); // blocks
        this.avgRowLen = new BigDecimal(BigDecimal.ZERO+"");
    }

    public BigDecimal getAvgRowLen() {
        return avgRowLen;
    }

    public void setAvgRowLen(BigDecimal avgRowLen) {
        this.avgRowLen = avgRowLen;
    }

    public BigDecimal getBlocks() {
        return relPages;
    }

    public void setBlocks(BigDecimal blocks) {
        this.relPages = blocks;
    }

    /**
     * Initializes OracleRelationStatistics metadata fields with the specified RelationStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine RelationStatistics metadata to Oracle.
     * @param relStat RelationStatistics
     */
    public void mapForPorting(RelationStatistics relStat)
    {
        super.mapForPorting(relStat);
        if(relStat instanceof DB2RelationStatistics) {
            // local metadata fields: fpages, overflow, active_blocks
            // None of these can be ported to Oracle
        } else if(relStat instanceof OracleRelationStatistics) {
            // local metadata fields: avgRowLen
            OracleRelationStatistics relationStatistics = (OracleRelationStatistics) relStat;
            this.setAvgRowLen(relationStatistics.getAvgRowLen());
        } else if(relStat instanceof MSSQLRelationStatistics) {
            // local metadata fields: -
        } else if(relStat instanceof PostgresRelationStatistics) {
            // local metadata fields: -
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the OracleRelationStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
        /*
        if(this.blocks.compareTo(BigDecimal.ZERO) > 0) {
            this.blocks = this.blocks.multiply(scalefactor);
        }
         *
         */
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "card|blocks|avg_row_len\n";
        ret = ret + this.relCard + "|" + this.relPages + "|" + this.avgRowLen + "\n";
        return ret;
    }
}
