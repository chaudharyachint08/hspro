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
import java.io.Serializable;

/**
 * Oracle Histogram is represented by this class.
 * @author dsladmin
 */
public class OracleHistObject implements Serializable{
    String endPointNumber;  // End Point Number
    String endPointValue;  // End Point Value
    String endPointActualValue; // End Point Actual Value

    /**
     * Constructs a Histogram bucket with the specified value.
     * @param endPointNumber End Point Number
     * @param endPointValue End Point Value
     * @param endPointActualValue End Point Actual Value
     */
    public OracleHistObject(String endPointNumber, String endPointValue, String endPointActualValue)
    {
        this.endPointNumber = endPointNumber;
        this.endPointValue = endPointValue;
        this.endPointActualValue = endPointActualValue;
    }

    public String getEndPointActualValue() {
        return endPointActualValue;
    }

    public void setEndPointActualValue(String endPointActualValue) {
        this.endPointActualValue = endPointActualValue;
    }

    public String getEndPointNumber() {
        return endPointNumber;
    }

    public void setEndPointNumber(String endPointNumber) {
        this.endPointNumber = endPointNumber;
    }

    public String getEndPointValue() {
        return endPointValue;
    }

    public void setEndPointValue(String endPointValue) {
        this.endPointValue = endPointValue;
    }
}