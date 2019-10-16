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
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.mssql.MSSQLIndexStatistics;
import iisc.dsl.codd.db.postgres.PostgresIndexStatistics;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * Oracle Index Statistics of an index is represented with this class.
 * @author dsladmin
 */
public class OracleIndexStatistics extends IndexStatistics{

    /**
     * http://docs.oracle.com/cd/B13789_01/server.101/b10755/statviews_2534.htm
     * http://docs.oracle.com/cd/B12037_01/server.101/b10755/statviews_1059.htm#i1577940
     * NUM_ROWS 	NUMBER 	  	Number of rows in the index
     * LEAF_BLOCKS 	NUMBER 	  	Number of leaf blocks in the index
     * DISTINCT_KEYS 	NUMBER 	  	Number of distinct keys in the index
     * AVG_LEAF_BLOCKS_PER_KEY 	NUMBER 	  	Average number of leaf blocks per key
     * AVG_DATA_BLOCKS_PER_KEY 	NUMBER 	  	Average number of data blocks per key
     * CLUSTERING_FACTOR 	NUMBER 	  	Indicates the amount of order of the rows in the table based on the values of the index.
           If the value is near the number of blocks, then the table is very well ordered. In this case, the index entries in a single leaf block tend to point to rows in the same data blocks.
           If the value is near the number of rows, then the table is very randomly ordered. In this case, it is unlikely that index entries in the same leaf block point to rows in the same data blocks.
     * BLEVEL 	NUMBER 	  	B-Tree level
     */

    protected BigDecimal numRows; // Number of rows in the index
    protected BigDecimal leafBlocks; // Number of leaf blocks in the index
    protected BigDecimal distinctKeys; // Number of distinct keys in the index
    protected BigDecimal avgLeafBlocksPerKey; // Average number of leaf blocks per key
    protected BigDecimal avgDataBlocksPerKey; // Average number of data blocks per key
    protected Double clusteringFactor; // Indicates the amount of order of the rows in the table based on the values of the index
    protected BigDecimal indLevel; // Index Level

    /**
     * Constructs a OracleIndexStatistics for the specified indexedColumns of the relation with the default values.
     * @param relName Relation name
     * @param cols Indexed Columns
     */
    public OracleIndexStatistics(String relName, ArrayList<String> cols)
    {
        super(relName, cols);
        Integer zero = new Integer(0);
        this.numRows = new BigDecimal(BigDecimal.ZERO+"");
        this.leafBlocks = new BigDecimal(BigDecimal.ZERO+"");
        this.distinctKeys = new BigDecimal(BigDecimal.ZERO+"");
        this.avgLeafBlocksPerKey = new BigDecimal(BigDecimal.ZERO+"");
        this.avgDataBlocksPerKey = new BigDecimal(BigDecimal.ZERO+"");
        this.clusteringFactor = new Double(0.0);
        this.indLevel = new BigDecimal(BigDecimal.ZERO+"");
    }

    public BigDecimal getAvgDataBlocksPerKey() {
        return avgDataBlocksPerKey;
    }

    public void setAvgDataBlocksPerKey(BigDecimal avgDataBlocksPerKey) {
        this.avgDataBlocksPerKey = avgDataBlocksPerKey;
    }

    public BigDecimal getAvgLeafBlocksPerKey() {
        return avgLeafBlocksPerKey;
    }

    public void setAvgLeafBlocksPerKey(BigDecimal avgLeafBlocksPerKey) {
        this.avgLeafBlocksPerKey = avgLeafBlocksPerKey;
    }

    public BigDecimal getIndLevel() {
        return indLevel;
    }

    public void setIndLevel(BigDecimal indLevel) {
        this.indLevel = indLevel;
    }

    public Double getClusteringFactor() {
        return clusteringFactor;
    }

    public void setClusteringFactor(Double clusteringFactor) {
        this.clusteringFactor = clusteringFactor;
    }

    public BigDecimal getDistinctKeys() {
        return distinctKeys;
    }

    public void setDistinctKeys(BigDecimal distinctKeys) {
        this.distinctKeys = distinctKeys;
    }

    public BigDecimal getLeafBlocks() {
        return leafBlocks;
    }

    public void setLeafBlocks(BigDecimal leafBlocks) {
        this.leafBlocks = leafBlocks;
    }

    public BigDecimal getNumRows() {
        return numRows;
    }

    public void setNumRows(BigDecimal numRows) {
        this.numRows = numRows;
    }

    /**
     * Initializes the OracleIndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine IndexStatistics metadata to Oracle.
     * @param indexStat IndexStatistics
     */
    public void mapForPorting(IndexStatistics indexStat)
    {
        super.mapForPorting(indexStat);
        if(indexStat instanceof DB2IndexStatistics) {
            // local metadata fields: colCount, nLeaf, nLevels, clusterRatio, clusterFactor, density, numRIDs, numRIDsDeleted, numEmptyLeafs, indCard
            DB2IndexStatistics indexStatistics = (DB2IndexStatistics) indexStat;
            // indCard - numRows
            this.setNumRows(indexStatistics.getIndCard());
            // nLeaf - leafBlocks
            this.setLeafBlocks(indexStatistics.getnLeaf());
            // Other metadata fields can not be ported to Oracle
        } else if(indexStat instanceof OracleIndexStatistics) {
            // local metadata fields: numRows, leafBlocks, distinctKeys, avgLeafBlocksPerKey, avgDataBlocksPerKey, clusteringFactor, indLevel
            OracleIndexStatistics indexStatistics = (OracleIndexStatistics) indexStat;
            this.setNumRows(indexStatistics.getNumRows());
            this.setLeafBlocks(indexStatistics.getLeafBlocks());
            this.setDistinctKeys(indexStatistics.getDistinctKeys());
            this.setAvgLeafBlocksPerKey(indexStatistics.getAvgLeafBlocksPerKey());
            this.setAvgDataBlocksPerKey(indexStatistics.getAvgDataBlocksPerKey());
            this.setClusteringFactor(indexStatistics.getClusteringFactor());
            this.setIndLevel(indexStatistics.getIndLevel());
        } else if(indexStat instanceof MSSQLIndexStatistics) {
            // local metadata fields: -
        } else if(indexStat instanceof PostgresIndexStatistics) {
            // local metadata fields: -  relPages, relTuples
            PostgresIndexStatistics indexStatistics = (PostgresIndexStatistics) indexStat;
            // relTuples - numRows
            this.setNumRows(indexStatistics.getReltuples());
            // relPages - leafBlocks
            this.setLeafBlocks(indexStatistics.getRelpages());
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the OracleIndexStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
        if(this.numRows.compareTo(BigDecimal.ZERO) > 0) {
            this.numRows = this.numRows.multiply(scalefactor);
        }
        if(this.leafBlocks.compareTo(BigDecimal.ZERO) > 0) {
            this.leafBlocks = this.leafBlocks.multiply(scalefactor);
        }
        /*
        if(this.indLevel > 0) {
            this.indLevel = this.indLevel * sf;
        }

        // TODO: If only a primary Key Index
        if(this.distinctKeys > 0) {
            this.distinctKeys = this.distinctKeys * sf;
        }
        */
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "NUM_ROWS | LEAF_BLOCKS | DISTINCT_KEYS | AVG_LEAF_BLOCKS_PER_KEY | AVG_DATA_BLOCKS_PER_KEY | CLUSTERING_FACTOR | IND_LEVEL\n";
        ret = ret + this.numRows + " | " + this.leafBlocks + " | " + this.distinctKeys + " | " + this.avgLeafBlocksPerKey + " | " + this.avgDataBlocksPerKey + " | " + this.clusteringFactor + " | " + this.indLevel + "\n";
        return ret;
    }
}
