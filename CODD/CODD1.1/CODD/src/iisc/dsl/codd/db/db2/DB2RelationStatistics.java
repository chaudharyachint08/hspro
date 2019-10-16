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
package iisc.dsl.codd.db.db2;

import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.mssql.MSSQLRelationStatistics;
import iisc.dsl.codd.db.oracle.OracleRelationStatistics;
import iisc.dsl.codd.db.postgres.PostgresRelationStatistics;
import java.math.BigDecimal;

/**
 * DB2 Relation Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class DB2RelationStatistics extends RelationStatistics{

    /**
     *  CARD - Total number of rows; -1 if statistics are not collected.
     *  NPAGES - Total number of pages on which the rows of the table exist; -1 for a view or alias, or if statistics are not collected; -2 for a subtable or hierarchy table.
     *  FPAGES - Total number of pages; -1 for a view or alias, or if statistics are not collected; -2 for a subtable or hierarchy table.
     *  OVERFLOW - Total number of overflow records in the table; -1 for a view or alias, or if statistics are not collected; -2 for a subtable or hierarchy table.
     *  ACTIVE_BLOCKS - Total number of active blocks in the table, or -1. Applies to multidimensional clustering (MDC) tables only.
     */
    //protected BigDecimal nPages;  - Stored in RelationStatistics// Total number of pages on which the rows of the table exist
    protected BigDecimal fPages; // Total number of pages
    protected BigDecimal overflow; // Total number of overflow records in the table
    protected BigDecimal active_blocks; // Total number of active blocks in the table

    /**
     * Constructs a RDB2elationStatistics for the specified relation with the default values.
     * @param relName Relation name
     * @param schema Schema name
     */
    public DB2RelationStatistics(String relName, String schema)
    {
        super(relName, schema);
        this.relCard = new BigDecimal("-1");
        this.relPages = new BigDecimal("-1"); // nPages
        this.fPages = new BigDecimal("-1");
        this.overflow = new BigDecimal("-1");
        this.active_blocks = new BigDecimal("-1");
    }

    public BigDecimal getNPages()
    {
        return this.relPages;
    }

    public void setNPages(BigDecimal npages)
    {
        this.relPages = npages;
    }

    public BigDecimal getFPages()
    {
        return this.fPages;
    }

    public void setFPages(BigDecimal fpages)
    {
        this.fPages = fpages;
    }

    public BigDecimal getOverflow()
    {
        return this.overflow;
    }

    public void setOverflow(BigDecimal overflow)
    {
        this.overflow = overflow;
    }

    public BigDecimal getActiveBlocks()
    {
        return this.active_blocks;
    }

    public void setActiveBlocks(BigDecimal activeBlocks)
    {
        this.active_blocks = activeBlocks;
    }

    /**
     * Initializes DB2RelationStatistics metadata fields with the specified RelationStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine RelationStatistics metadata to DB2.
     * @param relStat RelationStatistics
     */
    public void mapForPorting(RelationStatistics relStat)
    {
        super.mapForPorting(relStat);
        if(relStat instanceof DB2RelationStatistics) {
            // local metadata fields: fpages, overflow, active_blocks
            DB2RelationStatistics relationStatistics = (DB2RelationStatistics) relStat;
            this.setFPages(relationStatistics.getFPages());
            this.setOverflow(relationStatistics.getOverflow());
            this.setActiveBlocks(relationStatistics.getActiveBlocks());
        } else if(relStat instanceof OracleRelationStatistics) {
            // local metadata fields: avgRowLen
            OracleRelationStatistics relationStatistics = (OracleRelationStatistics) relStat;
            if(relationStatistics.getBlocks().compareTo(BigDecimal.ZERO) > 0) {
                this.setFPages(relationStatistics.getBlocks());
            } // else - default value -1
            // avgRowLen - can not be ported to DB2
        } else if(relStat instanceof MSSQLRelationStatistics) {
            // local metadata fields: -
            MSSQLRelationStatistics relationStatistics = (MSSQLRelationStatistics) relStat;
            BigDecimal FPagesBI = new BigDecimal(relationStatistics.getPageCount()+"");
            if(FPagesBI.compareTo(BigDecimal.ZERO) > 0) {
                this.setFPages(FPagesBI);
            } // else - default value -1
        } else if(relStat instanceof PostgresRelationStatistics) {
            // local metadata fields: -
            PostgresRelationStatistics relationStatistics = (PostgresRelationStatistics) relStat;
            if(relationStatistics.getRelPages().compareTo(BigDecimal.ZERO) > 0) {
                this.setFPages(relationStatistics.getRelPages());
            } // else - default value -1
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the DB2RelationStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
        if(this.active_blocks.compareTo(BigDecimal.ZERO) > 0) {
            this.active_blocks = this.active_blocks.multiply(scalefactor);
        }
        if(this.fPages.compareTo(BigDecimal.ZERO) > 0) {
            this.fPages = this.fPages.multiply(scalefactor);
        }
        if(this.overflow.compareTo(BigDecimal.ZERO) > 0) {
            this.overflow = this.overflow.multiply(scalefactor);
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "card|npages|fpages|overflow|activBlocks\n";
        ret = ret + this.relCard+"|"+relPages+"|"+fPages+"|"+overflow+"|"+this.active_blocks+"\n";
        return ret;
    }
}
