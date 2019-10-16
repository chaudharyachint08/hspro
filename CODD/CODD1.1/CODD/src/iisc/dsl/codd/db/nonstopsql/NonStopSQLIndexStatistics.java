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

import iisc.dsl.codd.db.IndexStatistics;
import java.util.ArrayList;

/**
 * NonStop SQL Index Statistics of an index is represented with this class.
 * @author DeepaliNemade
 */
public class NonStopSQLIndexStatistics extends IndexStatistics {

    // Assumption :  At most ONE index on each column. Either system generated or user defined

    /**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 1087020202960613843L;
	/*
     * RECORD_SIZE : Number of bytes in each logical record    ( <= Record size of table)
     *
     */
    protected Integer record_size;

    /*
     * Constructs NonStopSQLIndexStatistics for the specified indexedColumns of the relation with the default values.
     */
    public NonStopSQLIndexStatistics(String relName, ArrayList<String> cols) {
        super(relName, cols);
        this.record_size = 0;
    }

    public Integer getRecord_size() {
        return record_size;
    }

    public void setRecord_size(Integer record_size) {
        this.record_size = record_size;
    }

    /**
     * Initializes the NonStopSQLIndexStatistics metadata fields with the specified IndexStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine IndexStatistics metadata to NonStopSQL.
     * @param indexStat IndexStatistics
     */
    @Override
    public void mapForPorting(IndexStatistics indexStat) {
        // Porting possible only if cardinality of index is used in NonStopSQLIndexStatistics
        // Porting is not implemented for NonStopSQL.
    }

    /**
     * Scale (size based) the NonStopSQLIndexStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    //Nothing to scale. If we include cardinality of index then we may have to scale it.
    @Override
    public void scale(int sf) {
        super.scale(sf);
    }

    @Override
    public String toString() {
        String ret = new String();
        ret = ret + "RECORD_SIZE\n";
        ret = ret + this.record_size + "\n";
        return ret;
    }
}
