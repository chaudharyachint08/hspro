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

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * DB2 Frequency Histogram is represented by this class.
 * @author dsladmin
 */
public class DB2FreqHistObject implements Serializable{
    String colValue; // Column value
    BigDecimal valCount; // Number of values equals to colValue

    /**
     * Constructs a Frequency Histogram bucket with the specified value.
     * @param colValue Column Value
     * @param valCount  Number of values equals to colValue
     */
    public DB2FreqHistObject(String colValue, BigDecimal valCount)
    {
        this.colValue = colValue;
        this.valCount = valCount;
    }

    public String getColValue() {
        return colValue;
    }

    public void setColValue(String colValue) {
        this.colValue = colValue;
    }

    public BigDecimal getValCount() {
        return valCount;
    }

    public void setValCount(BigDecimal valCount) {
        this.valCount = valCount;
    }

    public int compareToByVALCOUNT(DB2FreqHistObject object) {
        return this.valCount.compareTo(object.getValCount());
    }
}
