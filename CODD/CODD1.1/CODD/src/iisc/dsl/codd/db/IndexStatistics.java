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
package iisc.dsl.codd.db;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Index Statistics of an index is represented with this class.
 * Each of the derived subclass should have scale and mapForPorting function
 * and call the super class function.
 * @author dsladmin
 */
public class IndexStatistics implements Serializable {

    protected ArrayList<String> indexedColumns; // Indexed Columns of the index in an ArrayList (store in CAPS always and do transformation as required)
    protected String relationName; // Relation name in which the index is present (store in CAPS always and do transformation as required)

    /**
     * Constructs a IndexStatistics for the specified indexedColumns of the relation with the default values.
     * @param relName Relation name in which the index is present
     * @param indexedColumns Indexed Columns of the index in an ArrayList
     */
    public IndexStatistics(String relName, ArrayList<String> indexedColumns)
    {
        this.indexedColumns = indexedColumns;
        this.relationName = relName;
    }

    public ArrayList<String> getIndexedColumns() {
        return indexedColumns;
    }

    public void setIndexedColumns(ArrayList<String> indexedColumns) {
        this.indexedColumns = indexedColumns;
    }

    public String getRelationName()
    {
        return this.relationName;
    }

    public void setRelationName(String relName)
    {
        this.relationName = relName;
    }

    /**
     * Initializes the super class IndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * At Non-Engine Specific index level, all metadata fields are portable.
     * @param indexStat IndexStatistics
     */
    public void mapForPorting(IndexStatistics indexStat)
    {
        //this.setIndexedColumns(indexStat.getIndexedColumns());
        //this.setRelationName(indexStat.getRelationName());
    }

    /**
     * Scale (size based) the IndexStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        // No Local variables to scale
    }
}
