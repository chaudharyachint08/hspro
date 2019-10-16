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

import iisc.dsl.codd.db.mssql.*;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.db2.DB2IndexStatistics;
import iisc.dsl.codd.db.oracle.OracleIndexStatistics;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * Postgres Index Statistics of an index is represented with this class.
 * @author dsladmin
 */
public class PostgresIndexStatistics extends IndexStatistics{

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
    protected BigDecimal relpages; // Total number of pages on which the rows of the table exist
    protected BigDecimal reltuples; // Number of rows in the relation


    /**
     * Constructs a PostgresIndexStatistics for the specified indexedColumns of the relation with the default values.
     * @param relName Relation name
     * @param cols Indexed Columns
     */
    public PostgresIndexStatistics(String relName, ArrayList<String> cols)
    {
        super(relName.toLowerCase(), cols);
        this.reltuples = new BigDecimal(BigDecimal.ZERO+"");
        this.relpages = new BigDecimal(BigDecimal.ZERO+"");
    }

    public BigDecimal getRelpages() {
        return relpages;
    }

    public void setRelpages(BigDecimal relpages) {
        this.relpages = relpages;
    }

    public BigDecimal getReltuples() {
        return reltuples;
    }

    public void setReltuples(BigDecimal reltuples) {
        this.reltuples = reltuples;
    }

    /**
     * Initializes the PostgresIndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine IndexStatistics metadata to Postgres.
     * @param indexStat IndexStatistics
     */
    public void mapForPorting(IndexStatistics indexStat)
    {
        super.mapForPorting(indexStat);
        if(indexStat instanceof DB2IndexStatistics) {
            // local metadata fields: colCount, nLeaf, nLevels, clusterRatio, clusterFactor, density, numRIDs, numRIDsDeleted, numEmptyLeafs, indCard
            DB2IndexStatistics indexStatistics = (DB2IndexStatistics) indexStat;
            // indCard - relTuples
            this.setReltuples(indexStatistics.getIndCard());
            // nLeaf - relPages
            BigDecimal relPagesBI = new BigDecimal(indexStatistics.getnLeaf()+"");
            this.setRelpages(relPagesBI);
            // Other metadata fields can not be ported to Postgres
        } else if(indexStat instanceof OracleIndexStatistics) {
            // local metadata fields: numRows, leafBlocks, distinctKeys, avgLeafBlocksPerKey, avgDataBlocksPerKey, clusteringFactor, indLevel
            OracleIndexStatistics indexStatistics = (OracleIndexStatistics) indexStat;
            // numRows - relTuples
            this.setReltuples(indexStatistics.getNumRows());
            // leafBlocks - relPages
            BigDecimal relPagesBI = new BigDecimal(indexStatistics.getLeafBlocks()+"");
            this.setRelpages(relPagesBI);
            // Other metdata fields can not be ported to Postgres
        } else if(indexStat instanceof MSSQLIndexStatistics) {
            // local metadata fields: -
        } else if(indexStat instanceof PostgresIndexStatistics) {
            // local metadata fields: -  relPages, relTuples
            PostgresIndexStatistics indexStatistics = (PostgresIndexStatistics) indexStat;
            this.setRelpages(indexStatistics.getRelpages());
            this.setReltuples(indexStatistics.getReltuples());
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the MSSQLIndexStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf + "");
        if (this.reltuples.compareTo(BigDecimal.ZERO) > 0) {
            this.reltuples = this.reltuples.multiply(scalefactor);
        }
        if (this.relpages.compareTo(BigDecimal.ZERO) > 0) {
            this.relpages = this.relpages.multiply(scalefactor);
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "indtuples|indpages\n";
        ret = ret + this.reltuples + "|" + this.relpages + "\n";
        return ret;
    }
}
