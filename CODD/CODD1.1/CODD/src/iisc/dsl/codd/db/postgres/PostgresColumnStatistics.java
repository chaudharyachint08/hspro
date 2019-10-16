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
package iisc.dsl.codd.db.postgres;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.mssql.*;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.db2.DB2ColumnStatistics;
import iisc.dsl.codd.db.db2.DB2FreqHistObject;
import iisc.dsl.codd.db.oracle.OracleColumnStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import javax.swing.JOptionPane;
import java.math.RoundingMode;

/**
 * MSSQL Column Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class PostgresColumnStatistics extends ColumnStatistics{

    /**
     * PG_STATS View
     * schemaname - name - pg_namespace.nspname	name of schema containing table
     * tablename - name - pg_class.relname name of table
     * attname 	name 	pg_attribute.attname 	name of the column described by this row
     * null_frac - real - fraction of column entries that are null
     * avg_width - integer - average width in bytes of column's entries
     * n_distinct - real - If greater than zero, the estimated number of distinct values in the column. If less than zero, the negative of the number of distinct values divided by the number of rows. (The negated form is used when ANALYZE believes that the number of distinct values is likely to increase as the table grows; the positive form is used when the column seems to have a fixed number of possible values.) For example, -1 indicates a unique column in which the number of distinct values is the same as the number of rows.
     * most_common_vals - anyarray - A list of the most common values in the column. (NULL if no values seem to be more common than any others.)
     * most_common_freqs - real[] - A list of the frequencies of the most common values, i.e., number of occurrences of each divided by total number of rows. (NULL when most_common_vals is.)
     * histogram_bounds - anyarray - A list of values that divide the column's values into groups of approximately equal population. The values in most_common_vals, if present, are omitted from this histogram calculation. (This column is NULL if the column data type does not have a < operator or if the most_common_vals list accounts for the entire population.)
     * correlation - real - Statistical correlation between physical row ordering and logical ordering of the column values. This ranges from -1 to +1. When the value is near -1 or +1, an index scan on the column will be estimated to be cheaper than when it is near zero, due to reduction of random access to the disk. (This column is NULL if the column data type does not have a < operator.)
     */

    // null_frac stored in numNulls after transforming into actual null values. i.e numNulls = null_fac * tableCard
    protected Double null_frac; // used in case of inter-transfer
    protected Integer avgWidth;
    // n_distinct stored in colCard. if postive, stored as such, if negative, transformation is done.
    protected Double n_distinct; // used in case of inter-transfer
    // most_common_vals, most_common_freqs are represented in DB2FreqHistObject (since DB2 freq hist is similar to this)
    protected String most_common_vals; // used in case of inter-transfer
    protected String most_common_freqs; // used in case of inter-transfer
     /**
     * Frequency Histogram is stores as a mapping of Sequence number to DB2FreqHistObject/
     * <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
     */
    Map<Integer,DB2FreqHistObject> frequencyHistogram;

    // histogram_bounds is stored in ColumnStatistics histogram, after transformation.
    protected String histogram_bounds; // used in case of inter-transfer

    protected Double correlation; // used in case of inter-transfer

    public static int DefaultHistogramBoundSize = 0; // Default Quantile Histogram Bucket size
    public static int DefaultFrequencyBucketSize = 0; // Default Frequency Histogram Bucket size
    public static int MaxHistogramBoundSize = 254; // Maximum Quantile Histogram Bucket size
    public static int MaxFrequencyBucketSize = 254; // Maximum Frequency Histogram Bucket size

    /**
     * Constructs a OracleColumnStatistics for the specified column with the default values.
     * @param relName Relation name
     * @param colName Column name
     * @param colType Data type of the column
     * @param constraint Integrity Constraint
     */
    public PostgresColumnStatistics(String relName, String colName, String colType, Constraint constraint)
    {
        super(relName, colName, colType, constraint, DefaultHistogramBoundSize, MaxHistogramBoundSize);
        this.colCard = new BigDecimal(BigDecimal.ZERO+"");
        this.numNulls = new BigDecimal(BigDecimal.ZERO+"");
        this.n_distinct = 0.0;
        this.null_frac = 0.0;
        this.avgWidth = 0;
        this.correlation = 0.0;
        this.most_common_vals = null;
        this.most_common_freqs = null;
        this.histogram_bounds = null;

    }

    public Map getHistogramBounds() {
        return histogram;
    }

    public void setHistogramBounds(Map quantileHistogram) {
        this.histogram = quantileHistogram;
    }
    public Integer getAvgWidth() {
        return avgWidth;
    }

    public void setAvgWidth(Integer avgWidth) {
        this.avgWidth = avgWidth;
    }

    public String getHistogram_bounds() {
        return histogram_bounds;
    }

    public void setHistogram_bounds(String histogram_bounds) {
        this.histogram_bounds = histogram_bounds;
    }

    public String getMost_common_freqs() {
        return most_common_freqs;
    }

    public void setMost_common_freqs(String most_common_freqs) {
    	/*if (most_common_freqs.equals(""))
    		most_common_freqs = null;*/
        this.most_common_freqs = most_common_freqs;
    }

    public String getMost_common_vals() {
        return most_common_vals;
    }

    public void setMost_common_vals(String most_common_vals) {
        this.most_common_vals = most_common_vals;
    }

    public Double getN_distinct() {
        return n_distinct;
    }

    public void setN_distinct(Double n_distinct) {
        this.n_distinct = n_distinct;
    }

    public Double getNull_frac() {
        return null_frac;
    }

    public void setNull_frac(Double null_frac) {
        this.null_frac = null_frac;
    }

    public Double getCorrelation() {
        return correlation;
    }

    public void setCorrelation(Double correlation) {
        this.correlation = correlation;
    }

    public Map<Integer, DB2FreqHistObject> getFrequencyHistogram() {
        return frequencyHistogram;
    }

    public void setFrequencyHistogram(Map<Integer, DB2FreqHistObject> frequencyHistogram) {
        this.frequencyHistogram = frequencyHistogram;
    }

    /**
     * Initializes the PostgresColumnStatistics metadata fields with the specified ColumnStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine ColumnStatistics metadata to Postgres.
     * @param colStat ColumnStatistics
     * @param tableCard table Cardinality
     */
    public void mapForPorting(ColumnStatistics colStat, BigDecimal tableCard) throws DatalessException
    {
        if(Constants.status == -1 && !(colStat instanceof PostgresColumnStatistics)) {
        Constants.status = JOptionPane.showConfirmDialog(null,"You have chosen other DB engine statistics to transfer to Postgres. "
                                    + "Clicking on Yes will use input histogram column values as Postgres histogram bounds, No will set histogram bounds to null.", "Choose the option.", JOptionPane.YES_NO_OPTION);
        }
        super.mapForPorting(colStat, tableCard);
        if (Constants.status == JOptionPane.NO_OPTION) {
            this.setHistogram(null);
        }
        if(colStat instanceof DB2ColumnStatistics) {
            // local metadata fields: high2key, low2key, avgColLen, subCount, subDelimLength, Frequency Histogram
            DB2ColumnStatistics columnStatistics = (DB2ColumnStatistics) colStat;
            // avgColLen - avgWidth
            this.setAvgWidth(columnStatistics.getAvgColLen());
            // Frequency Histogram - FreqHistogram
            this.setFrequencyHistogram(columnStatistics.getFrequencyHistogram());
            // Other metdata fields can not be ported to Postgres
        } else if(colStat instanceof OracleColumnStatistics) {
            // local metadata fields: minValue, maxValue, density, avgColLen, histogramType, numBucket, oralceHistogram
            OracleColumnStatistics columnStatistics = (OracleColumnStatistics) colStat;
            // avgColLen - avgColLen
            this.setAvgWidth(columnStatistics.getAvgColLen());
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
        	//System.out.println("Postgresql Column Statistics");
            // local metadata fields: n_distinct, null_frac, avgWidth, most_common_vals, most_common_freqs, FreqHistogram, histogram_bounds, correlation
            PostgresColumnStatistics columnStatistics = (PostgresColumnStatistics) colStat;
            this.setN_distinct(columnStatistics.getN_distinct());
            this.setNull_frac(columnStatistics.getNull_frac());
            this.setAvgWidth(columnStatistics.getAvgWidth());
            this.setMost_common_vals(columnStatistics.getMost_common_vals());
            this.setMost_common_freqs(columnStatistics.getMost_common_freqs());
            this.setFrequencyHistogram(columnStatistics.getFrequencyHistogram());
            this.setHistogram_bounds(columnStatistics.getHistogram_bounds());
            this.setCorrelation(columnStatistics.getCorrelation());
        } //else if(relStat instanceof SybaseRelationStatistics) { }
        if (Constants.status == JOptionPane.NO_OPTION) {
            this.setHistogram(null);
        }
        this.adjustHistogram(tableCard, this.DEFAULT_BUCKET_SIZE);
        // Other engine to Postgres. Convert to Postgres Specific format
        if(!(colStat instanceof PostgresColumnStatistics)) {
            this.convert_CoddFormat2PostgresFormat(tableCard);
        }
    }

    /**
     * Scale (size based) the MSSQLColumnStatistics attributes by the scale factor.
     * @param sf scale factor
     * @throws DatalessException 
     */
    public void scale(int sf) throws DatalessException
    {
        super.scale(sf);
        BigDecimal scalefactor = new BigDecimal(sf+"");
        // n_distinct
        if(constraint.isPartOfPK()) {
            if(this.n_distinct > 0) {
                 this.n_distinct = this.n_distinct * sf;
            } else {
                // Ratio will remain same
            }
        } else {
            if(this.n_distinct > 0) {
                 // Distinct element remains same
            } else { // Distinct element remains same for scaled relation
                this.n_distinct = this.n_distinct / sf;
            }
        }
        // null_frac ratio will remain same
        // avgWidth  assumed to be same
        // correlation - not required to change

        // frequency histogram
        TreeMap<Integer, DB2FreqHistObject> map = (TreeMap) this.frequencyHistogram;
        // In case PK column has Freq Hist, Do not scale for it
        if (map != null && !constraint.isPartOfPK()) {
            Set set = map.entrySet();
            Iterator i = set.iterator();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                Integer seqno = (Integer) me.getKey();
                DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                String colValue = freqHistObject.getColValue();
                BigDecimal valCount = freqHistObject.getValCount();
                valCount = valCount.multiply(scalefactor);
                freqHistObject.setValCount(valCount);
                map.put(seqno, freqHistObject);
            }
            this.frequencyHistogram = map;
        }

        // most_common_vals - Values will remain same

        // most_common_freqs - Ratio of frequency will remain same for non-PK columns
    /*    if(constraint.isPartOfPK() && this.most_common_freqs != null) {
        	String vals = this.most_common_freqs;
        	// Remove '{' and '}'
        	if(this.most_common_freqs.startsWith("{"))
        	{
        		vals = this.most_common_freqs.substring(1, this.most_common_freqs.length()-1);
        	}
        	
            // Seperate by comma
            String[] temp = vals.split(",");
            String newFreqs = new String();
            for(int te=0;te<temp.length;te++) {
                Double tem = Double.parseDouble(temp[te]) * sf;
                if(te != 0) {
                    newFreqs = newFreqs+",";
                }
                newFreqs = newFreqs+tem.intValue();
            }
            //newFreqs = "{"+newFreqs+"}";
            this.most_common_freqs = newFreqs;
        } */

        //histogram_bounds - Change only for PK columns
        if(constraint.isPartOfPK() && ( DataType.isDouble(getColumnType()) || DataType.isInteger(getColumnType()) || DataType.isNumeric(getColumnType())) && (this.histogram_bounds != null) ) {
        	String vals = this.histogram_bounds;
        	// Remove '{' and '}'
        	if(this.histogram_bounds.startsWith("{"))
        	{
        		vals = this.histogram_bounds.substring(1, this.histogram_bounds.length()-1);
        	}
        	
            // Seperate by comma
            String[] temp = vals.split(",");
            String newBounds = new String();
            for(int te=0;te<temp.length;te++) {
                Double tem = Double.parseDouble(temp[te]) * sf;
                if(te != 0) {
                    newBounds = newBounds+",";
                }
                if(DataType.isInteger(getColumnType())) {
                    newBounds = newBounds+tem.intValue();
                } else {
                    newBounds = newBounds+tem;
                }
            }
            //newBounds = "{"+newBounds+"}";
            this.histogram_bounds = newBounds;
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "distinctValues|null_fraction|avgWidth|correlation\n";
        ret = ret + this.n_distinct + "|" + this.null_frac + "|" + this.avgWidth + "|" + this.correlation + "\n";
        ret = ret + "col_card|null_cnt\n";
        ret = ret + this.colCard + "|" + this.numNulls + "\n";

        // Freq Hist
        ret = ret + "mostCommonVals|mostCommonFreqs\n";
        ret = ret + this.most_common_vals + "|" + this.most_common_freqs + "\n";
        TreeMap<Integer, DB2FreqHistObject> map = (TreeMap) this.frequencyHistogram;
        if (map != null) {
            ret = ret+ "\n Frequency Histogram \n";
            ret = ret+ "SEQNO|COLVALUE|VALCOUNT \n";
            Set set = map.entrySet();
            Iterator i = set.iterator();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                Integer seqno = (Integer) me.getKey();
                DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                String colValue = freqHistObject.getColValue();
                BigDecimal valCount = freqHistObject.getValCount();
                ret = ret+ seqno + "|" + colValue + "|" + valCount+" \n";
            }
        }
        // Quant Hist
        ret = ret + "histogramBoudns\n";
        ret = ret + this.histogram_bounds + "\n";
        TreeMap<Integer, HistogramObject> mapQ = (TreeMap) this.getHistogram();
        if (mapQ != null) {
            ret = ret+ "\n Quantile Histogram \n";
            ret = ret+ "SEQNO|COLVALUE|VALCOUNT|DISTCOUNT \n";

            Set set = mapQ.entrySet();
            Iterator i = set.iterator();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                Integer seqno = (Integer) me.getKey();
                HistogramObject histogramObject = (HistogramObject) me.getValue();
                String colValue = histogramObject.getColValue();
                if (colValue == null) {
                    break; //No more values
                }
                // Read the intValue. For DB2, the value has to be integer
                Integer valCount = histogramObject.getValCount().intValue();
                Integer distCount = null;
                if (histogramObject.getDistCount() != null) {
                    distCount = histogramObject.getDistCount().intValue();
                }
                ret = ret+ seqno + "|" + colValue + "|" + valCount+"|"+distCount+" \n";
            }
        }
        return ret;
    }

    /**
     * Given the string format of frequency histogram values, converts into DB2-style format.
     * @param tableCard Table cardinality
     */
    private void freqHist_PostgresFormat2CoddFormat(BigDecimal tableCard) throws DatalessException {
     //@param mostCommonVals String representation of most common vals (read from postgres stats)
     //@param mostCommonFreqs String representation of most common freqs (read from postgres stats)
        if(this.most_common_vals != null) {
            // Remove '{' and '}'
            String vals = this.most_common_vals;
            String freqs = this.most_common_freqs;
        	if(most_common_vals.startsWith("{")){
                vals = vals.substring(1, vals.length()-1);        		
        	}
        	if(most_common_freqs.startsWith("{")){
                freqs = freqs.substring(1, freqs.length()-1);        		
        	}
            // Split by comma
            ArrayList<String> valsArr = new ArrayList<String>();
            String temp = vals;
            while(!temp.isEmpty()) {
                String col;
                if(temp.startsWith("\"")) {
                    int endInd = temp.indexOf("\"",1);
                    col = temp.substring(1, endInd);
                    temp = temp.substring(endInd+1);
                    if(temp.startsWith(",")) { // Remove comma
                        temp = temp.substring(1);
                    }
                } else {
                    int endInd = temp.indexOf(",");
                    if(endInd == -1) {
                        col = temp;
                        temp = "";
                    } else {
                        col = temp.substring(0, endInd);
                        temp = temp.substring(endInd+1); // Comma also removed
                    }

                }
                valsArr.add(col);
            }

            String[] freqsArr;
            if(freqs.isEmpty()){
            	freqsArr = new String[0];
            }else{
            	freqsArr = freqs.split(",");
            }
            		
            System.out.println("Most commmon vals: "+"("+valsArr.size()+") :: "+vals);
            System.out.println("Most commmon freqs: "+"("+freqsArr.length+") :: "+freqs);
            if(valsArr.size() != freqsArr.length) {
                throw new DatalessException(" Value, Frequency anyarray length count is not matching.");
            }
            TreeMap<Integer, DB2FreqHistObject> map = new TreeMap();
            Constants.CPrintToConsole("Frequency Histogram", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT", Constants.DEBUG_SECOND_LEVEL_Information);
            int count = 0;
            while (count < valsArr.size()) {
                Integer seqno = new Integer(count+1);
                String col = valsArr.get(count);
                Double freq = tableCard.doubleValue() * Double.parseDouble(freqsArr[count]);
                //BigDecimal valCount = new BigDecimal(freq.intValue()+"");
                BigDecimal valCount = BigDecimal.valueOf(freq);
                Constants.CPrintToConsole(seqno + "|" + col + "|" + valCount, Constants.DEBUG_SECOND_LEVEL_Information);
                DB2FreqHistObject freqHistObject = new DB2FreqHistObject(col,valCount);
                map.put(seqno, freqHistObject);
                count++;
            }
            this.setFrequencyHistogram(map);
        }
    }

    /**
     * Converts the CODD / DB2-style format back to postgres string format
     * @param tableCard
     */
    private void freqHist_CoddFormat2PostgresFormat(BigDecimal tableCard) throws DatalessException
    {

        TreeMap<Integer, DB2FreqHistObject> mapFrequency = (TreeMap) this.getFrequencyHistogram();
        if (mapFrequency != null) {
            Set set = mapFrequency.entrySet();
            Iterator i = set.iterator();
            String mostCommonVals = new String();
            String mostCommonFreqs = new String();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                Integer seqno = (Integer) me.getKey();
                DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                String colValue = freqHistObject.getColValue();
                if (colValue == null) {
                    break; //No more values
                }
                /*if(DataType.isString(this.getColumnType())) {
                    colValue = "\""+colValue+"\"";
                } */
                if(DataType.isString(this.getColumnType()) && colValue.startsWith("\"")) {
                	
                	colValue = colValue.substring(1, colValue.length()-1);
                }
                BigDecimal valCount = freqHistObject.getValCount();
                Double freq_frac = valCount.doubleValue() / tableCard.doubleValue();
                if(!mostCommonVals.isEmpty()) { // Put comma
                    mostCommonVals = mostCommonVals+","+colValue;
                    mostCommonFreqs = mostCommonFreqs+","+freq_frac;
                } else { // Do not add comma for first value
                    mostCommonVals = colValue;
                    mostCommonFreqs = ""+freq_frac;
                }
            }
            //mostCommonVals = "{"+mostCommonVals+"}";
            //mostCommonFreqs = "{"+mostCommonFreqs+"}";
            Constants.CPrintToConsole("mostCommonVals|mostCommonFreqs", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole(mostCommonVals + "|" + mostCommonFreqs, Constants.DEBUG_SECOND_LEVEL_Information);
            this.setMost_common_freqs(mostCommonFreqs);
            this.setMost_common_vals(mostCommonVals);
        }
    }

    private void histogramBounds_PostgresFormat2CoddFormat(BigDecimal tableCard) throws DatalessException
    {
        /**
         * A list of values that divide the column's values into groups of approximately equal population.
         * The values in most_common_vals, if present, are omitted from this histogram calculation.
         * (This column is NULL if the column data type does not have a < operator or if the most_common_vals list accounts for the entire population.)
         */
        if(this.histogram_bounds != null) {
            BigDecimal sum = BigDecimal.ZERO;
            // If Frequency Histogram is present, add those values to sum
            TreeMap<Integer, DB2FreqHistObject> mapFrequency = (TreeMap) this.getFrequencyHistogram();
            if (mapFrequency != null) {
                Set set = mapFrequency.entrySet();
                Iterator i = set.iterator();
                while (i.hasNext()) {
                    Map.Entry me = (Map.Entry) i.next();
                    // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                    Integer seqno = (Integer) me.getKey();
                    DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                    String colValue = freqHistObject.getColValue();
                    if (colValue == null) {
                        break; //No more values
                    }
                    BigDecimal valCount = freqHistObject.getValCount();
                    sum = sum.add(valCount);
                }
            }
            
            // Determine the #tuples represented by the histogram (LeftOut = Card - Sum)
            BigDecimal leftOut = tableCard.subtract(sum);

            if(leftOut.compareTo(BigDecimal.ZERO) < 0) {
                throw new DatalessException(" Frequency Histogram has tuples more than table cardinality.");
            }
            // Formulate the initial histogram, where the valcount is LeftOut / #bins
            // Remove '{' and '}'
            String vals = this.histogram_bounds;
            if(histogram_bounds.startsWith("{")){
                vals = vals.substring(1, vals.length()-1);        		
        	}
            // Split by comma
            ArrayList<String> valsArr = new ArrayList();
            String temp = vals;
            while(!temp.isEmpty()) {
                String col;
                if(temp.startsWith("\"")) {
                    int endInd = temp.indexOf("\"",1);
                    col = temp.substring(1, endInd);
                    temp = temp.substring(endInd+1);
                    if(temp.startsWith(",")) { // Remove comma
                        temp = temp.substring(1);
                    }
                } else {
                    int endInd = temp.indexOf(",");
                    if(endInd == -1) {
                        col = temp;
                        temp = "";
                    } else {
                        col = temp.substring(0, endInd);
                        temp = temp.substring(endInd+1); // Comma also removed
                    }

                }
                valsArr.add(col);
            }

            BigDecimal[] valCount = new BigDecimal[valsArr.size()];
            BigDecimal defaultValue = leftOut.divide(new BigDecimal(valsArr.size()+""), 2, RoundingMode.HALF_UP); // Increase round off precision (currently 2) if needed
            for(int bin=0;bin<valsArr.size();bin++) {
                valCount[bin] = defaultValue;
            }

            // Add the frequency histogram values to this.
            if (mapFrequency != null) {
                Set set = mapFrequency.entrySet();
                Iterator i = set.iterator();
                while (i.hasNext()) {
                    Map.Entry me = (Map.Entry) i.next();
                    // <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
                    Integer seqno = (Integer) me.getKey();
                    DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
                    String colValue = freqHistObject.getColValue();
                    if (colValue == null) {
                        break; //No more values
                    }
                    BigDecimal valCnt = freqHistObject.getValCount();

                    // Determine the bucket and add valcount
                       // Move until the bucket boundary is lesser than the colValue
                    int binIndex = 0;
                    while(binIndex < valsArr.size()-1 && valsArr.get(binIndex).compareTo(colValue) < 0) {
                        binIndex++;
                    }
                    valCount[binIndex] = valCount[binIndex].add(valCnt);
                }
            } // End if

            // Construct the histogram
            TreeMap<Integer, HistogramObject> map = new TreeMap();
            Constants.CPrintToConsole("Histogram", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT", Constants.DEBUG_SECOND_LEVEL_Information);
            for(int bin=0;bin<valsArr.size();bin++) {
                Integer seqno = new Integer(bin+1);
                Constants.CPrintToConsole(seqno + "|" + valsArr.get(bin) + "|" + valCount[bin], Constants.DEBUG_SECOND_LEVEL_Information);
                HistogramObject histogramObject;
                histogramObject = new HistogramObject(valsArr.get(bin),valCount[bin].doubleValue(),null);
                map.put(seqno, histogramObject);
            }
            this.setHistogramBounds(map);
        }
    }

    /**
     * Converts the CODD / DB2-style format back to postgres string format
     * @param tableCard
     * @return
     */
    private void histogramBounds_CoddFormat2PostgresFormat(BigDecimal tableCard)
    {
    	System.out.println("Inside histogramBounds_Codd2Postgres");
        // Get the histogram into array and use the colValues as bounds.

        TreeMap<Integer, HistogramObject> mapQuantile = (TreeMap) this.getHistogramBounds();
        if(mapQuantile != null)
        {
            Set set = mapQuantile.entrySet();
            Iterator i = set.iterator();
            String histogramBounds = new String();
            while (i.hasNext()) {
                Map.Entry me = (Map.Entry) i.next();
                Integer seqno = (Integer) me.getKey();
                HistogramObject histogramObject = (HistogramObject) me.getValue();
                String colValue = histogramObject.getColValue();
                if (colValue == null) {
                    break; //No more values
                }
                /*if(DataType.isString(this.getColumnType())) {
                    colValue = "\""+colValue+"\"";
                } */
                if(DataType.isString(this.getColumnType()) && colValue.startsWith("\"")) {
                	System.out.println("Inside + Inside");
                	colValue = colValue.substring(1, colValue.length()-1);
                }
                if(!histogramBounds.isEmpty()) { // Put comma
                    histogramBounds = histogramBounds+","+colValue;
                } else { // Do not add comma for first value
                    histogramBounds = colValue;
                }
            }
            //histogramBounds = "{"+histogramBounds+"}";
            Constants.CPrintToConsole("histogramBounds", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole(histogramBounds, Constants.DEBUG_SECOND_LEVEL_Information);
            this.setHistogram_bounds(histogramBounds);
        }
    }

    /**
     * Converts Double null_frac to BigDecimal.
     * @param null_frac Null Fraction
     * @param tableCard Relation Cardinality
     * @return CODD format Null Fraction value
     */
    public static BigDecimal convert_Postgres2Codd_NullFrac(Double null_frac, BigDecimal tableCard) {
        Double numNullsD = tableCard.longValue() * null_frac;
        BigDecimal numNullsBI = new BigDecimal(numNullsD.longValue()+"");
        return numNullsBI;
    }
    /**
     * Converts null_frac from BigDecimal to Double
     * @param numNulls Number of Null values present in the column
     * @param tableCard Relation cardinality
     * @return Postgres format Null Fraction value
     */
    public static Double convert_Codd2Postgres_NullFrac(BigDecimal numNulls, BigDecimal tableCard) {
        return numNulls.doubleValue() / tableCard.doubleValue();
    }

    /**
     * Converts Double n_distinct to BigDecimal.
     * @param n_distinct
     * @param tableCard
     * @return CODD format distinct value
     */
    public static BigDecimal convert_Postgres2Codd_DistinctValue(Double n_distinct, BigDecimal tableCard) {
        BigDecimal colCardBI;
        if(n_distinct > 0) {
             colCardBI = new BigDecimal(n_distinct.longValue()+"");
        } else {
            Double negated = -1 * n_distinct;
            Double DistinctD = tableCard.longValue() * negated;
            colCardBI = new BigDecimal(DistinctD.longValue()+"");
        }
        return colCardBI;
    }
    /**
     * Converts n_distinct from BigDecimal to Double
     * @param colCard
     * @param tableCard
     * @return Postgres format distinct value
     */
    public static Double convert_Codd2Postgres_DistinctValue(BigDecimal colCard, BigDecimal tableCard) {
        return colCard.doubleValue();
    }

    /**
     * Converts the frequency histogram and histogram bounds to CODD-style format
     * Called after reading the statistics (getColumnStatisitcs of postgres) from catalogs
     * @param tableCard table cardinality
     */
    public void convert_PostgresFormat2CoddFormat(BigDecimal tableCard) throws DatalessException
    {
        this.setNumNulls(PostgresColumnStatistics.convert_Postgres2Codd_NullFrac(null_frac, tableCard));
        this.setColCard(PostgresColumnStatistics.convert_Postgres2Codd_DistinctValue(n_distinct, tableCard));
        Constants.CPrintToConsole("null_frac -> NumNULLS : "+null_frac+" -> "+this.getNumNulls(), Constants.DEBUG_SECOND_LEVEL_Information);
        Constants.CPrintToConsole("n_distinct -> ColCard : "+n_distinct+" -> "+this.getColCard(), Constants.DEBUG_SECOND_LEVEL_Information);
        freqHist_PostgresFormat2CoddFormat(tableCard);
        histogramBounds_PostgresFormat2CoddFormat(tableCard);
    }

    /**
     * Converts the frequency histogram and histogram bounds to postgres format
     * Called only when other Engine metadata is transferred
     * @param tableCard table cardinality
     */
    public void convert_CoddFormat2PostgresFormat(BigDecimal tableCard) throws DatalessException
    {
        this.null_frac = PostgresColumnStatistics.convert_Codd2Postgres_NullFrac(this.getNumNulls(), tableCard);
        this.n_distinct = PostgresColumnStatistics.convert_Codd2Postgres_DistinctValue(this.getColCard(),tableCard);
        Constants.CPrintToConsole("NumNULLS -> null_frac : "+this.getNumNulls()+" -> "+null_frac, Constants.DEBUG_SECOND_LEVEL_Information);
        Constants.CPrintToConsole("ColCard -> n_distinct : "+this.getColCard()+" -> "+n_distinct, Constants.DEBUG_SECOND_LEVEL_Information);
        freqHist_CoddFormat2PostgresFormat(tableCard);
        histogramBounds_CoddFormat2PostgresFormat(tableCard);
    }

}
