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

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;

import java.math.BigDecimal;
import java.util.StringTokenizer;

/**
 *
 * @author DeepaliNemade
 */
public class NonStopSQLColumnStatistics extends ColumnStatistics {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -2860775036154577689L;
	protected BigDecimal ROWCOUNT;
	protected BigDecimal TOTAL_UEC;
	protected String LOW_VALUE;
	protected String HIGH_VALUE;
	protected Integer numBuckets;
	public static int DEFAULT_BUCKET_SIZE = 20;      // Check what is this!!!!!!!
	public static int MAX_BUCKET_SIZE = 254;
	NonStopSQLHistObject[] nonstopsqlHistogram;

	public NonStopSQLColumnStatistics(String relName, String colName, String columnType, Constraint constraint) {
		super(relName, colName, columnType, constraint, DEFAULT_BUCKET_SIZE, MAX_BUCKET_SIZE);
		this.colCard = BigDecimal.valueOf(100);
		this.TOTAL_UEC = BigDecimal.valueOf(-1);
		this.LOW_VALUE = new String("");
		this.HIGH_VALUE = new String("");
		this.numBuckets = new Integer(2);
		this.nonstopsqlHistogram = null;
	}

	public String getHIGH_VALUE() {
		return HIGH_VALUE;
	}

	public void setHIGH_VALUE(String HIGH_VALUE) {
		this.HIGH_VALUE = HIGH_VALUE;
	}

	public String getLOW_VALUE() {
		return LOW_VALUE;
	}

	public void setLOW_VALUE(String LOW_VALUE) {
		this.LOW_VALUE = LOW_VALUE;
	}

	public BigDecimal getTOTAL_UEC() {
		return TOTAL_UEC;
	}

	public void setTOTAL_UEC(BigDecimal TOTAL_UEC) {
		this.TOTAL_UEC = TOTAL_UEC;
	}

	public NonStopSQLHistObject[] getNonStopSQLHistogram() {
		return nonstopsqlHistogram;
	}

	public void setNonStopSQLHistogram(NonStopSQLHistObject[] nonStopSQLHistogram) {
		this.nonstopsqlHistogram = nonStopSQLHistogram;
	}

	public Integer getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(Integer numBuckets) {
		this.numBuckets = numBuckets;
	}

	@Override
	public String toString() {
		String ret = "";
		ret = ret + "row_count|total_uec|low_value|high_value\n";
		ret = ret + this.colCard + "|" + this.TOTAL_UEC + "|" + this.LOW_VALUE + "|" + this.HIGH_VALUE + "\n";
		if(nonstopsqlHistogram!=null){
			ret = ret + "\n Histogram \n";
			ret = ret + "INTERVAL_NUMBER|INTERVAL_ROWCOUNT|INTERVAL_UEC|INTERVAL_BOUNDARY\n";
			for (int i = 0; i < nonstopsqlHistogram.length; i++) {
				ret = ret + nonstopsqlHistogram[i].getInterval_number() + "|" + nonstopsqlHistogram[i].getInterval_rowcount() + "|" + nonstopsqlHistogram[i].getInterval_uec() + "|" + nonstopsqlHistogram[i].getInterval_boundary()+ "\n";
			}			
		}
		return ret;
	}

	/**
	 * Scale (size based) the NonStopSQLColumnStatistics attributes by the scale factor.
	 * @param sf scale factor
	 */
	@Override
	public void scale(int sf) throws DatalessException {
		BigDecimal scaleFactor = BigDecimal.valueOf(sf);
		colCard = colCard.multiply(scaleFactor);
		
		try
		{   // TODO ? - Why gate the longValueExact() check in the 2nd part of && - Shouldn't we execute this for all datatypes and
			// NOT just BiGDecimal .. Also we support LARGEINT ( which is SIGNED by default) but we handle this only as SIGNED_LARGEINT
			// and not too sure if the column is defined just as a LARGEINT ( without mentioning the SIGNED_ prefix ? 
			if ((DataType.isBigDecimal(getColumnType()) && colCard.longValueExact() > Long.MAX_VALUE))
			{
				//DatatypeValueExact will raise an exception if datatype range is exceeded.
			}
		} catch (Exception e)
		{
			throw new DatalessException ("Column cardinality exceeds allowed range for SQL/MX "+columnName);
		}
		
		try
		{   // Suresh - This check for interger is reqd, only if its a standalone PK ( not part of a composite key)
			// TODO: Do we need to handle this for other data types ??
			if ((constraint.isPK() && DataType.isInteger(getColumnType()) && colCard.intValueExact() > Integer.MAX_VALUE)) 
			{
					//DatatypeValueExact will raise an exception if datatype range is exceeded.
			}
		} catch (Exception e)
		{
			throw new DatalessException ("Column cardinality exceeds allowed column datatype range for column "+columnName);
		}
		
		if(LOW_VALUE==null || HIGH_VALUE==null){
			return;
		}
		//Scaling column stats
		/*
		 * For multicolumn histograms:
		 * 1. tokenize columns...
		 * 2. The column which is part of primary key and is double or integer.... set low and high values..
		 */
		boolean incrUEC = false;
		if (columnName.contains("|")) {
			String H_Value = HIGH_VALUE;
			String L_Value = LOW_VALUE;

			// For multicolumn
			StringTokenizer tok1 = new StringTokenizer(columnName, "|");
			StringTokenizer tok2 = new StringTokenizer(constraint.toString(), "|");
			StringTokenizer tok3 = new StringTokenizer(getColumnType(), "|");
			StringTokenizer tok4 = new StringTokenizer(H_Value, "|");
			StringTokenizer tok5 = new StringTokenizer(L_Value, "|");
			String highVal = "";
			String lowVal = "";

			while (tok1.hasMoreTokens() && tok2.hasMoreTokens() && tok3.hasMoreTokens() 
					&& tok4.hasMoreTokens() && tok5.hasMoreTokens()) {
				tok1.nextToken();
				String constraintType = tok2.nextToken().trim();
				String datatype = tok3.nextToken().trim();
				String hvalue = tok4.nextToken().trim();
				String lvalue = tok5.nextToken().trim();
				if(lvalue==null || lvalue.trim().isEmpty() || hvalue==null || hvalue.trim().isEmpty()){
					return;
				}
				if (constraintType.equals(Constraint.PRIMARYKEY) || constraintType.equals(Constraint.PRIMARY_KEY) || 
					constraintType.equals(Constraint.COMPOSITE_PK) || constraintType.equals(Constraint.COMPOSITE_PK_FK) || 
					constraintType.equals(Constraint.FOREIGNKEY) || constraintType.equals(Constraint.FOREIGN_KEY)) {
					if (DataType.isDouble(datatype)) {
						hvalue = "" + (Double.parseDouble(hvalue) * sf + sf - 1);
						lvalue = "" + (Double.parseDouble(lvalue) * sf);
					} else if (DataType.isInteger(datatype)) {
						hvalue = "" + (Integer.parseInt(hvalue) * (int)sf + (int)sf - 1);
						lvalue = "" + (Integer.parseInt(lvalue) * (int)sf);
					} else if (DataType.isBigDecimal(datatype) || DataType.isNumeric(datatype)){
						hvalue = "" + ((new BigDecimal(hvalue)).multiply(scaleFactor).add(scaleFactor).subtract(BigDecimal.ONE));
						lvalue = "" + ((new BigDecimal(lvalue)).multiply(scaleFactor));
					}
					incrUEC = true;
				}
				highVal = highVal + hvalue + "|";
				lowVal = lowVal + lvalue + "|";
			}
			if(!highVal.trim().isEmpty()){
				HIGH_VALUE = highVal.substring(0, highVal.length() - 1);				
			}else{
				HIGH_VALUE = "";
			}
			if(!lowVal.trim().isEmpty()){
				LOW_VALUE = lowVal.substring(0, lowVal.length() - 1);				
			}else{
				LOW_VALUE = "";
			}

			if (incrUEC) {
				TOTAL_UEC = TOTAL_UEC.multiply(scaleFactor);
			}
		} else{
			if(LOW_VALUE.trim().isEmpty() || HIGH_VALUE.trim().isEmpty()){
				return;
			}
			if(constraint.isPartOfPK() || constraint.isFK()){
				if(DataType.isDouble(getColumnType())){
					HIGH_VALUE = "" + (Double.parseDouble(HIGH_VALUE) * sf + sf - 1);
					LOW_VALUE = "" + (Double.parseDouble(LOW_VALUE) * sf);
				}else if(DataType.isInteger(getColumnType())){
					HIGH_VALUE = "" + (Integer.parseInt(HIGH_VALUE) * (int)sf + (int)sf - 1);
					LOW_VALUE = "" + (Integer.parseInt(LOW_VALUE) * (int)sf);
				}else if(DataType.isBigDecimal(getColumnType()) || DataType.isNumeric(getColumnType())){
					HIGH_VALUE = "" + (new BigDecimal(HIGH_VALUE)).multiply(scaleFactor);
					LOW_VALUE = "" + (new BigDecimal(LOW_VALUE)).multiply(scaleFactor).add(scaleFactor).subtract(BigDecimal.ONE);
				}
				TOTAL_UEC = TOTAL_UEC.multiply(scaleFactor);									
			}
		}

		//Scaling histogram Statistics
		NonStopSQLHistObject[] hist = this.nonstopsqlHistogram;
		if(hist==null){
			return;
		}
		if (columnName.contains("|")) {
			// There will be two default buckets in this case
			hist[0].setInterval_number(0);
			hist[0].setInterval_rowcount(BigDecimal.ZERO);
			hist[0].setInterval_uec(BigDecimal.ZERO);
			hist[0].setInterval_boundary(LOW_VALUE);
			
			hist[1].setInterval_number(1);
			hist[1].setInterval_rowcount(colCard);
			hist[1].setInterval_uec(TOTAL_UEC);
			hist[1].setInterval_boundary(HIGH_VALUE);
		}else{
			for (int i = 0; i < hist.length; i++) {
				Integer interval_number = hist[i].getInterval_number();
				BigDecimal interval_rowcount = hist[i].getInterval_rowcount();
				BigDecimal interval_uec = hist[i].getInterval_uec();
				String interval_boundary = hist[i].getInterval_boundary();

				if(interval_boundary==null || interval_boundary.trim().isEmpty()){
					return;
				}
				interval_rowcount = interval_rowcount.multiply(BigDecimal.valueOf((long)sf));

				//We scale interval boundary for primary key column which is of either integer or double data type
				if(i==0){
					// This bucket has low value as boundary and interval_uec = 0
					interval_boundary = LOW_VALUE;
				}else if (constraint.isPartOfPK() || constraint.isFK()) {
					if (DataType.isDouble(getColumnType())) {
						interval_boundary = "" + (Double.parseDouble(interval_boundary) * sf + sf - 1);
					} else if (DataType.isInteger(getColumnType())){
						interval_boundary = "" + (Integer.parseInt(interval_boundary) * (int)sf + (int)sf - 1);
					}else if(DataType.isBigDecimal(getColumnType()) || DataType.isNumeric(getColumnType())){
						interval_boundary = "" + (new BigDecimal(interval_boundary)).multiply(scaleFactor).add(scaleFactor).subtract(BigDecimal.ONE);
					}
					interval_uec = interval_uec.multiply(BigDecimal.valueOf((long)sf));
				}
				hist[i].setInterval_number(interval_number);
				hist[i].setInterval_rowcount(interval_rowcount);
				hist[i].setInterval_uec(interval_uec);
				hist[i].setInterval_boundary(interval_boundary);
			}			
		}
		// Zero'th bucket is not consider as a bucket.
		this.numBuckets = hist.length-1;
		this.nonstopsqlHistogram = hist;
	}
}
