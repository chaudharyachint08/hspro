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


import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.mssql.MSSQLColumnStatistics;
import iisc.dsl.codd.db.oracle.OracleColumnStatistics;
import iisc.dsl.codd.db.postgres.PostgresColumnStatistics;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * DB2 Column Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class DB2ColumnStatistics extends ColumnStatistics{

    /**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 3797387654037148757L;
	/**
     * COLNAME - Name of the column.
     * COLCARD - Number of distinct values in the column; -1 if statistics are not collected; -2 for inherited columns and columns of hierarchy tables.
     * HIGH2KEY - Second-highest data value. Representation of numeric data changed to character literals. Empty if statistics are not collected. Empty for inherited columns and columns of hierarchy tables.
     * LOW2KEY - Second-lowest data value. Representation of numeric data changed to character literals. Empty if statistics are not collected. Empty for inherited columns and columns of hierarchy tables.
     * AVGCOLLEN - Average space (in bytes) required for the column; -1 if a long field or LOB, or statistics have not been collected; -2 for inherited columns and columns of hierarchy tables.
     * NUMNULLS - Number of null values in the column; -1 if statistics are not collected.
     * SUB_COUNT - Average number of sub-elements in the column. Applicable to character string columns only.
     * SUB_DELIM_LENGTH - Average length of the delimiters that separate each sub-element in the column. Applicable to character string columns only.
     */

    /**
     * High2Key - Second Highest value in the column
     */
    protected String high2key;
    /**
     * Low2Key - Second Lowest value in the column
     */
    protected String low2key;
    protected Integer avgColLen; // Avg Column Length
    protected Integer subCount; // Average number of sub elements in the column
    protected Integer subDelimLength; // Average length of the delimiters

    public static int DefaultQuantileBucketSize = 0; // Default Quantile Histogram Bucket size
    public static int DefaultFrequencyBucketSize = 0; // Default Frequency Histogram Bucket size
    /**
     * We have limited the number of buckets. DB2 allows any valid integer value to be the number of buckets.
     */
    public static int MaxQuantileBucketSize = 254; // Maximum Quantile Histogram Bucket size
    public static int MaxFrequencyBucketSize = 254; // Maximum Frequency Histogram Bucket size
    /**
     * COLDIST Statistics
     *  Uses superClass histogram for Quantile Histogram.
     *  Super class does not stores the aggregated sum (less than or equal to).
     *  Processing is done before and after update/read from database.
     *  FrequencyHistogram
     */
    /**
     * Frequency Histogram is stored as a mapping of Sequence number to DB2FreqHistObject/
     * <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
     */
    Map<Integer,DB2FreqHistObject> frequencyHistogram;
    /**
     * Constructs a DB2ColumnStatistics for the specified column with the default values.
     * @param relName Relation name
     * @param colName Column name
     * @param colType Data type of the column
     * @param constraint Integrity Constraint
     */
    public DB2ColumnStatistics(String relName, String colName, String colType, Constraint constraint)
    {
        super(relName, colName, colType, constraint, DefaultQuantileBucketSize, MaxQuantileBucketSize);
        Integer minusOne = new Integer(-1);
        BigDecimal minusOneBI = new BigDecimal("-1");
        this.colCard = new BigDecimal(minusOneBI+"");
        this.high2key = new String("");
        this.low2key = new String("");
        this.avgColLen = minusOne;
        this.numNulls = new BigDecimal(minusOneBI+"");
        this.subCount = minusOne;
        this.subDelimLength = minusOne;
        this.frequencyHistogram = null;
    }

    public String getHigh2key() {
        return high2key;
    }

    public void setHigh2key(String high2key) {
        this.high2key = high2key;
    }

    public String getLow2key() {
        return low2key;
    }

    public void setLow2key(String low2key) {
        this.low2key = low2key;
    }

        public Integer getAvgColLen() {
        return avgColLen;
    }

    public void setAvgColLen(Integer avgColLen) {
        this.avgColLen = avgColLen;
    }

    public Integer getSubCount() {
        return subCount;
    }

    public void setSubCount(Integer subCount) {
        this.subCount = subCount;
    }

    public Integer getSubDelimLength() {
        return subDelimLength;
    }

    public void setSubDelimLength(Integer subDelimLength) {
        this.subDelimLength = subDelimLength;
    }

    public Map<Integer,DB2FreqHistObject> getFrequencyHistogram() {
        return frequencyHistogram;
    }

    public void setFrequencyHistogram(Map<Integer,DB2FreqHistObject> frquencyHistogram) {
        this.frequencyHistogram = frquencyHistogram;
    }

    public Map<Integer,HistogramObject> getQuantileHistogram() {
        return histogram;
    }

    public void setQuantileHistogram(Map<Integer,HistogramObject> quantileHistogram) {
        this.histogram = quantileHistogram;
    }

    /**
     * Initializes the DB2ColumnStatistics metadata fields with the specified ColumnStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine ColumnStatistics metadata to DB2.
     * @param colStat ColumnStatistics
     * @param tableCard table Cardinality
     */
    public void mapForPorting(ColumnStatistics colStat, BigDecimal tableCard) throws DatalessException
    {
        super.mapForPorting(colStat, tableCard);
        if(colStat instanceof DB2ColumnStatistics) {
            // local metadata fields: high2key, low2key, avgColLen, subCount, subDelimLength, Frequency Histogram
            DB2ColumnStatistics columnStatistics = (DB2ColumnStatistics) colStat;
            this.setHigh2key(columnStatistics.getHigh2key());
            this.setLow2key(columnStatistics.getLow2key());
            this.setAvgColLen(columnStatistics.getAvgColLen());
            this.setSubCount(columnStatistics.getSubCount());
            this.setSubDelimLength(columnStatistics.getSubDelimLength());
            this.setFrequencyHistogram(columnStatistics.getFrequencyHistogram());
            this.setQuantileHistogram(columnStatistics.getQuantileHistogram());
        } else if(colStat instanceof OracleColumnStatistics) {
            // local metadata fields: minValue, maxValue, density, avgColLen, histogramType, numBucket, oralceHistogram
            OracleColumnStatistics columnStatistics = (OracleColumnStatistics) colStat;
            // avgColLen - avgColLen
            this.setAvgColLen(columnStatistics.getAvgColLen());
            if (DataType.isDouble(columnStatistics.getColumnType()) || DataType.isInteger(columnStatistics.getColumnType()) || DataType.isNumeric(columnStatistics.getColumnType()) || columnStatistics.isIsActualValuePresent()) {
                // Histogram is portable
                this.setHistogram(columnStatistics.getHistogram());
            } else {
                this.setHistogram(null); // Oracle histograms are not portable
            }
            // Other metdata fields can not be ported to DB2
        } else if(colStat instanceof MSSQLColumnStatistics) {
            // local metadata fields: -
        } else if(colStat instanceof PostgresColumnStatistics) {
            // local metadata fields: n_distinct, null_frac, avgWidth, most_common_vals, most_common_freqs, FreqHistogram, histogram_bounds, correlation
            PostgresColumnStatistics columnStatistics = (PostgresColumnStatistics) colStat;
            // avgWidth - avgColLen
            this.setAvgColLen(columnStatistics.getAvgWidth()-1);
            // FreqHistogram - FrequencyHistogram
                Map<Integer,DB2FreqHistObject> freqHist = columnStatistics.getFrequencyHistogram();
                if (freqHist != null) {
                    // Keep only the DefaultFrequencyBucketSize greatest values
                    ArrayList<DB2FreqHistObject> sortedObjects = new ArrayList<DB2FreqHistObject>();
                    Set<Entry<Integer, DB2FreqHistObject>> set = freqHist.entrySet();
                    Iterator<Entry<Integer, DB2FreqHistObject>> i = set.iterator();
                    while (i.hasNext()) {
                        Map.Entry<Integer, DB2FreqHistObject> me = i.next();
                        DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();

                        // Insert in order
                        boolean added = false;
                        for (int ind = 0; ind < sortedObjects.size(); ind++) {
                            // if the element you are looking at is greater than or equal to freqHistObject,
                            // go to the next element
                            if (sortedObjects.get(ind).compareToByVALCOUNT(freqHistObject) >= 0) {
                                continue;
                            }
                            // otherwise, we have found the location to add freqHistObjectx
                            sortedObjects.add(ind, freqHistObject);
                            added = true;
                        }
                        if(!added) {
                            // we looked through all of the elements, and they were all
                            // greater than or equal to freqHistObject, so we add freqHistObject to the end of the list
                            sortedObjects.add(freqHistObject);
                        }

                        // Remove last entry if size is greater than deafult frquency histogram size
                        if(sortedObjects.size() > DB2ColumnStatistics.DefaultFrequencyBucketSize) {
                            sortedObjects.remove(sortedObjects.size()-1);
                        }
                    }

                    // Create a new map out of the sortedObjects.
                    TreeMap<Integer, DB2FreqHistObject> map = new TreeMap<Integer, DB2FreqHistObject>();
                    for (int ind = 0; ind < sortedObjects.size(); ind++) {
                        Integer seqno = new Integer(ind);
                        map.put(seqno, sortedObjects.get(ind));
                    }
                    this.setFrequencyHistogram(map);
                } else {
                    this.setFrequencyHistogram(columnStatistics.getFrequencyHistogram());
                }
            // Other metdata fields can not be ported to DB2
        } //else if(relStat instanceof SybaseRelationStatistics) { }
        this.adjustHistogram(tableCard, MaxQuantileBucketSize);
    }

    /**
     * Scale (size based) the DB2ColumnStatistics attributes by the scale factor.
     * @param sf scale factor
     * @throws DatalessException 
     */
    public void scale(int sf) throws DatalessException
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
		if(constraint.isPartOfPK() || constraint.isFK()){
			if(DataType.isDouble(getColumnType())){
				if(high2key != null && !high2key.isEmpty()) {
					high2key = "" + Double.parseDouble(high2key) * sf;
	            }
	            if(low2key != null  && !high2key.isEmpty()) {
	            	low2key = "" + Double.parseDouble(low2key) * sf;
	            }
			}else if(DataType.isInteger(getColumnType())){
				if(high2key != null && !high2key.isEmpty()) {
					high2key = "" + Integer.parseInt(high2key) * sf;
	            }
	            if(low2key != null  && !high2key.isEmpty()) {
	            	low2key = "" + Integer.parseInt(low2key) * sf;
	            }
			}else if(DataType.isBigDecimal(getColumnType()) || DataType.isNumeric(getColumnType())){
				if(high2key != null && !high2key.isEmpty()) {
					high2key = "" + (new BigDecimal(high2key)).multiply(new BigDecimal(""+sf));
	            }
	            if(low2key != null  && !high2key.isEmpty()) {
	            	low2key = "" + (new BigDecimal(low2key)).multiply(new BigDecimal(""+sf));
	            }
			}
		}
        // Freq Hist
        TreeMap<Integer, DB2FreqHistObject> map = (TreeMap<Integer, DB2FreqHistObject>) this.frequencyHistogram;
        if (map != null) {
            Set<Entry<Integer, DB2FreqHistObject>> set = map.entrySet();
            Iterator<Entry<Integer, DB2FreqHistObject>> i = set.iterator();
            while (i.hasNext()) {
                Map.Entry<Integer, DB2FreqHistObject> me = i.next();
                // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                Integer seqno = (Integer) me.getKey();
                DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                BigDecimal valCount = freqHistObject.getValCount();
                valCount = valCount.multiply(scalefactor);
                freqHistObject.setValCount(valCount);
                map.put(seqno, freqHistObject);
            }
            this.frequencyHistogram = map;
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "col_card|null_cnt|sub_count|sub_delim_len|high2key|low2key|avg_col_len\n";
        ret = ret + this.colCard + "|" + this.numNulls + "|" + this.subCount + "|" + this.subDelimLength + "|" + high2key + "|" + low2key + "|" + this.avgColLen + "\n";
        // Freq Hist
        TreeMap<Integer, DB2FreqHistObject> map = (TreeMap<Integer, DB2FreqHistObject>) this.frequencyHistogram;
        if (map != null) {
            ret = ret+ "\n Frequency Histogram \n";
            ret = ret+ "SEQNO|COLVALUE|VALCOUNT \n";
            Set<Entry<Integer, DB2FreqHistObject>> set = map.entrySet();
            Iterator<Entry<Integer, DB2FreqHistObject>> i = set.iterator();
            while (i.hasNext()) {
                Map.Entry<Integer, DB2FreqHistObject> me = i.next();
                // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                Integer seqno = (Integer) me.getKey();
                DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                String colValue = freqHistObject.getColValue();
                BigDecimal valCount = freqHistObject.getValCount();
                ret = ret+ seqno + "|" + colValue + "|" + valCount+" \n";
            }
        }
        // Quant Hist
        TreeMap<Integer, HistogramObject> mapQ = (TreeMap<Integer, HistogramObject>) this.getHistogram();
        if (mapQ != null) {
            ret = ret+ "\n Quantile Histogram \n";
            ret = ret+ "SEQNO|COLVALUE|VALCOUNT|DISTCOUNT \n";
            Integer prevValueCount = 0;
            Integer prevDistCount = 0;

            Set<Entry<Integer, HistogramObject>> set = mapQ.entrySet();
            Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
            while (i.hasNext()) {
                Map.Entry<Integer, HistogramObject> me = i.next();
                Integer seqno = (Integer) me.getKey();
                HistogramObject histogramObject = (HistogramObject) me.getValue();
                String colValue = histogramObject.getColValue();
                if (colValue == null) {
                    break; //No more values
                }
                // Read the intValue. For DB2, the value has to be integer
                Integer valCount = histogramObject.getValCount().intValue() + prevValueCount;
                Integer distCount = null;
                if (histogramObject.getDistCount() != null) {
                    distCount = histogramObject.getDistCount().intValue();
                }
                if (distCount != null && prevDistCount != null) //If DISTCOUNT is present
                {
                    distCount = distCount + prevDistCount;
                }
                ret = ret+ seqno + "|" + colValue + "|" + valCount+"|"+distCount+" \n";
            }
        }
        return ret;
    }
}
