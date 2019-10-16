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

import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.db.mssql.MSSQLIndexStatistics;
import iisc.dsl.codd.db.oracle.OracleIndexStatistics;
import iisc.dsl.codd.db.postgres.PostgresIndexStatistics;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * DB2 Index Statistics of an index is represented with this class.
 * @author dsladmin
 */
public class DB2IndexStatistics extends IndexStatistics{

    /**
     * COLCOUNT - Number of columns in the key, plus the number of include columns, if any.
     * NLEAF - Number of leaf pages; -1 if statistics are not collected.
     * NLEVELS - Number of index levels; -1 if statistics are not collected.
     * CLUSTERRATIO - Degree of data clustering with the index; -1 if statistics are not collected or if detailed index statistics are collected (in which case, CLUSTERFACTOR will be used instead).
     * CLUSTERFACTOR - 	Finer measurement of the degree of clustering; -1 if statistics are not collected or if the index is defined on a nickname.
     * DENSITY - Ratio of SEQUENTIAL_PAGES to number of pages in the range of pages occupied by the index, expressed as a percent (integer between 0 and 100); -1 if statistics are not collected.
     * NUMRIDS - Total number of row identifiers (RIDs) or block identifiers (BIDs) in the index; -1 if not known.
     * NUMRIDS_DELETED - Total number of row identifiers (or block identifiers) in the index that are marked deleted, excluding those identifiers on leaf pages on which all the identifiers are marked deleted.
     * NUM_EMPTY_LEAFS - Total number of index leaf pages that have all of their row identifiers (or block identifiers) marked deleted.
     * INDCARD - Cardinality of the index. This might be different from the cardinality of the table for indexes that do not have a one-to-one relationship between the table rows and the index entries.
     */

    protected BigDecimal colCount; // Number of Columns in the key
    protected BigDecimal nLeaf; // Number of leaf pages
    protected BigDecimal nLevels; // Number of index levels
    protected BigDecimal clusterRatio; // Degree of data clustering with the index
    protected Double clusterFactor; // Finer measurement of the degree of clustering
    protected Double density; // Ratio of SEQUENTIAL_PAGES to number of pages in the range of pages occupied by the index, expressed as a percent (integer between 0 and 100)
    protected BigDecimal numRIDs; // Total number of row identifiers (RIDs) or block identifiers (BIDs) in the index
    protected BigDecimal numRIDsDeleted; // Total number of row identifiers (or block identifiers) in the index that are marked deleted, excluding those identifiers on leaf pages on which all the identifiers are marked deleted
    protected BigDecimal numEmptyLeafs; // Total number of index leaf pages that have all of their row identifiers (or block identifiers) marked deleted
    protected BigDecimal indCard; // Cardinality of the index

    /**
     * Constructs a DB2IndexStatistics for the specified indexedColumns of the relation with the default values.
     * @param relName Relation name
     * @param cols Indexed Columns
     */
    public DB2IndexStatistics(String relName, ArrayList<String> cols)
    {
        super(relName, cols);
        Integer minusOne = new Integer(-1);
        BigDecimal minusOneBI = new BigDecimal("-1");
        this.colCount = new BigDecimal(minusOneBI+"");
        this.nLeaf = new BigDecimal(minusOneBI+"");
        this.nLevels = new BigDecimal(minusOneBI+"");
        this.clusterRatio = new BigDecimal(minusOneBI+"");
        this.clusterFactor = new Double(-1);
        this.density = new Double(-1);
        this.numRIDs = new BigDecimal(minusOneBI+"");
        this.numRIDsDeleted = new BigDecimal(minusOneBI+"");
        this.numEmptyLeafs = new BigDecimal(minusOneBI+"");
        this.indCard = new BigDecimal(minusOneBI+"");
    }

    public Double getClusterFactor() {
        return clusterFactor;
    }

    public void setClusterFactor(Double clusterFactor) {
        this.clusterFactor = clusterFactor;
    }

    public BigDecimal getClusterRatio() {
        return clusterRatio;
    }

    public void setClusterRatio(BigDecimal clusterRatio) {
        this.clusterRatio = clusterRatio;
    }

    public BigDecimal getColCount() {
        return colCount;
    }

    public void setColCount(BigDecimal colCount) {
        this.colCount = colCount;
    }

    public Double getDensity() {
        return density;
    }

    public void setDensity(Double density) {
        this.density = density;
    }

    public BigDecimal getIndCard() {
        return indCard;
    }

    public void setIndCard(BigDecimal indCard) {
        this.indCard = indCard;
    }

    public BigDecimal getnLeaf() {
        return nLeaf;
    }

    public void setnLeaf(BigDecimal nLeaf) {
        this.nLeaf = nLeaf;
    }

    public BigDecimal getnLevels() {
        return nLevels;
    }

    public void setnLevels(BigDecimal nLevels) {
        this.nLevels = nLevels;
    }

    public BigDecimal getNumEmptyLeafs() {
        return numEmptyLeafs;
    }

    public void setNumEmptyLeafs(BigDecimal numEmptyLeafs) {
        this.numEmptyLeafs = numEmptyLeafs;
    }

    public BigDecimal getNumRIDs() {
        return numRIDs;
    }

    public void setNumRIDs(BigDecimal numRIDs) {
        this.numRIDs = numRIDs;
    }

    public BigDecimal getNumRIDsDeleted() {
        return numRIDsDeleted;
    }

    public void setNumRIDsDeleted(BigDecimal numRIDsDeleted) {
        this.numRIDsDeleted = numRIDsDeleted;
    }

    /**
     * Initializes the DB2IndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine IndexStatistics metadata to DB2.
     * @param indexStat IndexStatistics
     */
    public void mapForPorting(IndexStatistics indexStat)
    {
        super.mapForPorting(indexStat);
        if(indexStat instanceof DB2IndexStatistics) {
            // local metadata fields: colCount, nLeaf, nLevels, clusterRatio, clusterFactor, density, numRIDs, numRIDsDeleted, numEmptyLeafs, indCard
            DB2IndexStatistics indexStatistics = (DB2IndexStatistics) indexStat;
            this.setColCount(indexStatistics.getColCount());
            this.setnLeaf(indexStatistics.getnLeaf());
            this.setnLevels(indexStatistics.getnLevels());
            this.setClusterRatio(indexStatistics.getClusterRatio());
            this.setClusterFactor(indexStatistics.getClusterFactor());
            this.setDensity(indexStatistics.getDensity());
            this.setNumRIDs(indexStatistics.getNumRIDs());
            this.setNumRIDsDeleted(indexStatistics.getNumRIDsDeleted());
            this.setNumEmptyLeafs(indexStatistics.getNumEmptyLeafs());
            this.setIndCard(indexStatistics.getIndCard());
        } else if(indexStat instanceof OracleIndexStatistics) {
            // local metadata fields: numRows, leafBlocks, distinctKeys, avgLeafBlocksPerKey, avgDataBlocksPerKey, clusteringFactor, indLevel
            OracleIndexStatistics indexStatistics = (OracleIndexStatistics) indexStat;
            // numRows - indCard
            if(indexStatistics.getNumRows().compareTo(BigDecimal.ZERO) > 0) {
                this.setIndCard(indexStatistics.getNumRows());
            } // else - default value -1
            // leafBlocks - nLeaf
            if(indexStatistics.getLeafBlocks().compareTo(BigDecimal.ZERO) > 0) {
                this.setnLeaf(indexStatistics.getLeafBlocks());
            }  // else - default value -1
            // Other metdata fields can not be ported to DB2
        } else if(indexStat instanceof MSSQLIndexStatistics) {
            // local metadata fields: -
        } else if(indexStat instanceof PostgresIndexStatistics) {
            // local metadata fields: -  relPages, relTuples
            PostgresIndexStatistics indexStatistics = (PostgresIndexStatistics) indexStat;
            // relTuples - indCard
            if(indexStatistics.getReltuples().compareTo(BigDecimal.ZERO) > 0) {
                this.setIndCard(indexStatistics.getReltuples());
            } // else - default value -1
            // relPages - nLeaf
            if(indexStatistics.getRelpages().intValue() > 0) {
                this.setnLeaf(indexStatistics.getRelpages());
            } // else - default value -1
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the DB2IndexStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
        if(this.indCard.compareTo(BigDecimal.ZERO) > 0) {
            this.indCard = this.indCard.multiply(scalefactor);
        }
        if(this.nLeaf.compareTo(BigDecimal.ZERO) > 0) {
            this.nLeaf = this.nLeaf.multiply(scalefactor);
        }
        /*
        if(this.nLevels > 0) {
            this.nLevels = this.nLevels * sf;
        }
         *
         */
        if(this.numEmptyLeafs.compareTo(BigDecimal.ZERO) > 0) {
            this.numEmptyLeafs = this.numEmptyLeafs.multiply(scalefactor);
        }
        if(this.numRIDs.compareTo(BigDecimal.ZERO) > 0) {
            this.numRIDs = this.numRIDs.multiply(scalefactor);
        }
        if(this.numRIDsDeleted.compareTo(BigDecimal.ZERO) > 0) {
            this.numRIDsDeleted = this.numRIDsDeleted.multiply(scalefactor);
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "INDCARD|NLEAF|NLEVELS|DENSITY|NUMRIDS|CLUSTERFACTOR|NUM_EMPTY_LEAFS\n";
        ret = ret + this.indCard+"|"+ this.nLeaf+"|"+ this.nLevels+"|"+ this.density+"|"+ this.numRIDs+"|"+ this.clusterFactor+"|"+ this.numEmptyLeafs+"\n";
        return ret;
    }
}
