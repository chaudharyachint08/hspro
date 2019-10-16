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
package iisc.dsl.codd.graphhistogram;

import iisc.dsl.codd.ds.DataType;

/**
 * Class BucketItem represents a histogram bucket.
 * @author dsladmin
 */
public class BucketItem
{
    // Left Value (Lower bound) of the bucket.
    DataType lvalue;
    // Value (Upper bound) of the bucket.
    DataType value;
    // freq represents the number of tuples belong to this bucket.
    long freq;
    // distinctCount represents the number of unique values in this bucket.
    long distinctCount;
    // frequency in percentage.
    double freqPercent;
    // distinctCount in percentage.
    double distinctCountPercent;
    /**
     * Constructor for BucketItem
     * @param lvalue Left bucket value
     * @param value Value of the bucket
     * @param freq  Number of rows in this bucket
     * @param distinctCount Number of unique values in this bucket
     * @param freqPercent freq in percentage
     * @param distinctCountPercent distinct count in percentage
     */
    public BucketItem(DataType lvalue, DataType value, double freq, double distinctCount, double freqPercent, double distinctCountPercent)
    {
        this.lvalue = lvalue;
        this.value = value;
        this.freq = Math.round(freq);
        this.distinctCount = Math.round(distinctCount);
        this.freqPercent = freqPercent;
        this.distinctCountPercent = distinctCountPercent;
    }

    /**
     * Constructor for BucketItem. Creates a new BucketItem and initializes it values from an existing BucketItem.
     * @param bucket
     */
    public BucketItem(BucketItem bucket)
    {
        this.lvalue = bucket.getLValue();
        this.value = bucket.getValue();
        this.freq = Math.round(bucket.getFreq());
        this.distinctCount = Math.round(bucket.getDistinctCount());
        this.freqPercent = bucket.getFreqPercent();
        this.distinctCountPercent = bucket.getDistinctCountPercent();
    }

    /**
     * Returns the distinct count of this histogram BucketItem.
     * @return distinctCount
     */
    public double getDistinctCount() {
        return distinctCount;
    }

    /**
     * Replaces the distinct count of this histogram BucketItem with the specified distinct count.
     * @param distinctCount
     */
    public void setDistinctCount(double distinctCount) {
        this.distinctCount = Math.round(distinctCount);
    }

    /**
     * Returns the distinct count percentage of this histogram BucketItem.
     * @return distinctCountPercent
     */
    public double getDistinctCountPercent() {
        return distinctCountPercent;
    }

    /**
     * Replaces the distinct count percentage of this histogram BucketItem with the specified distinct count percentage.
     * @param distinctCountPercent
     */
    public void setDistinctCountPercent(double distinctCountPercent) {
        this.distinctCountPercent = distinctCountPercent;
    }

    /**
     * Returns the frequency of this histogram BucketItem.
     * @return freq
     */
    public double getFreq() {
        return freq;
    }

    /**
     * Replaces the frequency of this histogram BucketItem with the specified frequency.
     * @param freq
     */
    public void setFreq(double freq) {
        this.freq = Math.round (freq);
    }

    /**
     * Returns the frequency percent of this histogram BucketItem.
     * @return freqPercent
     */
    public double getFreqPercent() {
        return freqPercent;
    }

    /**
     * Replaces the frequency percent of this histogram BucketItem with the specified freqPercent.
     * @param freqPercent
     */
    public void setFreqPercent(double freqPercent) {
        this.freqPercent = freqPercent;
    }

    /**
     * Returns the value of this histogram BucketItem.
     * @return value
     */
    public DataType getValue() {
        return value;
    }

    /**
     * Replaces the value of this histogram BucketItem with the specified value.
     * @param value
     */
    public void setValue(DataType value) {
        this.value = value;
    }

    /**
     * Returns the lvalue of this histogram BucketItem.
     * @return lvalue
     */
    public DataType getLValue() {
        return lvalue;
    }

    /**
     * Replaces the lvalue of this histogram BucketItem with the specified lvalue.
     * @param lvalue
     */
    public void setLValue(DataType lvalue) {
        this.lvalue = lvalue;
    }
}