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

import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.db2.DB2IndexStatistics;
import iisc.dsl.codd.db.oracle.OracleIndexStatistics;
import iisc.dsl.codd.db.postgres.PostgresIndexStatistics;
import java.util.ArrayList;

/**
 * MSSQL Index Statistics of an index is represented with this class.
 * @author dsladmin
 */
public class MSSQLIndexStatistics extends IndexStatistics{

    /**
     * Constructs a MSSQLIndexStatistics for the specified indexedColumns of the relation with the default values.
     * @param relName Relation name
     * @param cols Indexed Columns
     */
    public MSSQLIndexStatistics(String relName, ArrayList<String> cols)
    {
        super(relName, cols);
    }

    /**
     * Initializes the MSSQLIndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine IndexStatistics metadata to MSSQL.
     * This will not be used as there is no construct mode support for MSSQL.
     * @param indexStat IndexStatistics
     */
    public void mapForPorting(IndexStatistics indexStat)
    {
        super.mapForPorting(indexStat);
        if(indexStat instanceof DB2IndexStatistics) {
            // local metadata fields: colCount, nLeaf, nLevels, clusterRatio, clusterFactor, density, numRIDs, numRIDsDeleted, numEmptyLeafs, indCard
        } else if(indexStat instanceof OracleIndexStatistics) {
            // local metadata fields: numRows, leafBlocks, distinctKeys, avgLeafBlocksPerKey, avgDataBlocksPerKey, clusteringFactor, indLevel
        } else if(indexStat instanceof MSSQLIndexStatistics) {
            // local metadata fields: -
        } else if(indexStat instanceof PostgresIndexStatistics) {
            // local metadata fields: -  relPages, relTuples
        } //else if(relStat instanceof SybaseRelationStatistics) { }
    }

    /**
     * Scale (size based) the MSSQLIndexStatistics attributes by the scale factor.
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
        return ret;
    }
}
