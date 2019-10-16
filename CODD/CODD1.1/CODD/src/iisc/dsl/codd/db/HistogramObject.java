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

import iisc.dsl.codd.db.db2.*;
import java.io.Serializable;

/**
 * A bucket of the histogram is represented by this class.
 * @author dsladmin
 */
public class HistogramObject implements Serializable{
    String colValue; // Column Value
    Double valCount; // Number of values in this bucket
    Double distCount; // Number of distinct values in this bucket

    /**
     * Constructs a HistogramObject with the specified values.
     * @param colValue Column value of the bucket
     * @param valCount Number values in the bucket
     * @param distCount Number of distinct values in the bucket
     */
    public HistogramObject(String colValue, Double valCount, Double distCount)
    {
        this.colValue = colValue;
        this.valCount = valCount;
        this.distCount = distCount;
    }

    /**
     * Clones (Deep Copy) a histogram object.
     * @param hist
     */
    public HistogramObject(HistogramObject hist) {
        this.colValue = new String(hist.getColValue());
        this.valCount = new Double(hist.getValCount());
        if(hist.getDistCount() != null) {
            this.distCount = new Double(hist.getDistCount());
        } else {
            this.distCount = null;
        }

    }

   /**
     * Returns the column value of the bucket.
     * @return the column value of the bucket
     */
    public String getColValue() {
        return colValue;
    }

    /**
     * Replaces the column value with the specified column value.
     * @param colValue Column value
     */
    public void setColValue(String colValue) {
        this.colValue = colValue;
    }

    /**
     * Returns the number of values in the bucket.
     * @return the number of values in the bucket
     */
    public Double getValCount() {
        return valCount;
    }

    /**
     * Replaces the number of values in the bucket with the specified value.
     * @param valCount Number of values in the bucket
     */
    public void setValCount(Double valCount) {
        this.valCount = valCount;
    }

    /**
     * Returns the number distinct of values in the bucket.
     * @return the number distinct of values in the bucket
     */
    public Double getDistCount() {
        return distCount;
    }

    /**
     * Replaces the number of distinct values in the bucket with the specified value.
     * @param valCount Number of distinct values in the bucket
     */
    public void setDistCount(Double distCount) {
        this.distCount = distCount;
    }
}
