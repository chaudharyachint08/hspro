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

import java.io.Serializable;
import java.math.BigDecimal;

/**
 *
 * @author DeepaliNemade
 */
public class NonStopSQLHistObject implements Serializable {

    /**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 282396962580827L;
	Integer interval_number;
    BigDecimal interval_rowcount;
    BigDecimal interval_uec;
    String interval_boundary;

    public NonStopSQLHistObject(Integer interval_number, BigDecimal interval_rowcount, BigDecimal interval_uec, String interval_boundary) {
        this.interval_number = interval_number;
        this.interval_rowcount = interval_rowcount;
        this.interval_uec = interval_uec;
        this.interval_boundary = interval_boundary;
    }

    public String getInterval_boundary() {
        return interval_boundary;
    }

    public void setInterval_boundary(String interval_boundary) {
        this.interval_boundary = interval_boundary;
    }

    public BigDecimal getInterval_rowcount() {
        return interval_rowcount;
    }

    public void setInterval_rowcount(BigDecimal interval_rowcount) {
        this.interval_rowcount = interval_rowcount;
    }

    public Integer getInterval_number() {
        return interval_number;
    }

    public void setInterval_number(Integer interval_number) {
        this.interval_number = interval_number;
    }

    public BigDecimal getInterval_uec() {
        return interval_uec;
    }

    public void setInterval_uec(BigDecimal interval_uec) {
        this.interval_uec = interval_uec;
    }
}