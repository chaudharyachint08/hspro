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

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.db2.DB2ColumnStatistics;
import iisc.dsl.codd.db.mssql.MSSQLColumnStatistics;
import iisc.dsl.codd.db.postgres.PostgresColumnStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;
import java.math.BigDecimal;
import java.util.Map;
import javax.swing.JOptionPane;

/**
 * Oracle Column Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class OracleColumnStatistics extends ColumnStatistics{

    /**
     * Views: USER_TAB_COL_STATISTICS, USER_TAB_HISTOGRAMS
     * http://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2094.htm#I1020277
     * docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2092.htm#i1590494
     * http://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_2096.htm#i1590954
     * COLUMN_NAME - Name of the column.
     * NUM_DISTICT - Number of distinct values in the column;
     * LOW_VALUE, HIGH_VALUE - Minimum and Maximum value. Right there is no support min, max value in CODD. TODO: Add it.
     * AVG_COL_LEN - Average length of the column (in bytes)
     * NUM_NULLS - Number of null values in the column;
     * HISTOGRAM - Identifies the kind of histogram. (FREQUENCY, HEIGHT BALANCED, NONE)
     */

    protected DataType minValue; // Minimum Value
    protected DataType maxValue; // Maximum Value
    protected Double density; // Density
    protected Integer avgColLen; // Average Column Length
    protected String histogramType; // Histogram type [HeightBalance / Frequency / None]
    protected Integer numBuckets; // Number of Buckets

    public static String HeightBalanced = "HEIGHT BALANCED"; // Height Balanced Histogram
    public static String Frequency = "FREQUENCY"; // Frequency Histogram
    public static String None = "NONE"; // No Histogram

    public static int DefaultHeightBalancedBucketSize = 0; // Default Height Balanced Histogram Bucket size
    public static int DefaultFrequencyBucketSize = 0; // Default Frequency Histogram Bucket size
    public static int MaxQuantileBucketSize = 254; // Maximum Height Balanced Histogram Bucket size
    public static int MaxFrequencyBucketSize = 254; // Maximum Frequency Histogram Bucket size

    /**
     *  We always use the oralceHistogram for both HeightBalanced as well as Frequency Histogram.
     *  Because Oracle maintains any one of the histograms.
     *  Buckets are ordered by Sequence Number.
     *  Super class Histogram is never used in Oracle.
     */
    OracleHistObject[] oralceHistogram;

    // Indicates whether the actualValue of histogram is present.
    boolean isActualValuePresent;

    // Indicates whether the statiscs are inputted by the user (Construct from scratch / User). It is used in preparing the statistics before updating to catalogs.
    boolean userInput;

    /**
     * Constructs a OracleColumnStatistics for the specified column with the default values.
     * @param relName Relation name
     * @param colName Column name
     * @param colType Data type of the column
     * @param constraint Integrity Constraint
     */
    public OracleColumnStatistics(String relName, String colName, String colType, Constraint constraint)
    {
        super(relName, colName, colType, constraint, DefaultHeightBalancedBucketSize, MaxQuantileBucketSize);
        Integer zero = new Integer(0);
        this.colCard = new BigDecimal(BigDecimal.ZERO+"");
        this.minValue = null;
        this.maxValue = null;
        this.density = new Double(0.0);
        this.avgColLen = zero;
        this.numNulls = new BigDecimal(BigDecimal.ZERO+"");
        this.histogramType = OracleColumnStatistics.None;
        this.numBuckets = zero;
        this.histogram = null;
        this.oralceHistogram = null;
        this.isActualValuePresent = false;
        this.userInput = false;
    }


    public DataType getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(DataType maxValue) {
        this.maxValue = maxValue;
    }

    public DataType getMinValue() {
        return minValue;
    }

    public void setMinValue(DataType minValue) {
        this.minValue = minValue;
    }

    public Double getDensity() {
        return density;
    }

    public void setDensity(Double density) {
        this.density = density;
    }

    public Integer getAvgColLen() {
        return avgColLen;
    }

    public void setAvgColLen(Integer avgColLen) {
        this.avgColLen = avgColLen;
    }

    public String getHistogramType() {
        return histogramType;
    }

    public void setHistogramType(String histogramType) {
        this.histogramType = histogramType;
    }

    public Integer getNumBuckets() {
        return numBuckets;
    }

    public void setNumBuckets(Integer numBuckets) {
        this.numBuckets = numBuckets;
    }

    public Map getQuantileHistogram() {
        return histogram;
    }

    public void setQuantileHistogram(Map quantileHistogram) {
        this.histogram = quantileHistogram;
    }

    public boolean isIsActualValuePresent() {
        return isActualValuePresent;
    }

    public void setIsActualValuePresent(boolean isActualValuePresent) {
        this.isActualValuePresent = isActualValuePresent;
    }

    public OracleHistObject[] getOralceHistogram() {
        return oralceHistogram;
    }

    public void setOralceHistogram(OracleHistObject[] oralceHistogram) {
        this.oralceHistogram = oralceHistogram;
    }

    public boolean isUserInput() {
        return userInput;
    }

    public void setUserInput(boolean userInput) {
        this.userInput = userInput;
    }

    /**
     * Initializes the OracleColumnStatistics metadata fields with the specified ColumnStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine ColumnStatistics metadata to Oracle.
     * @param colStat ColumnStatistics
     * @param tableCard table Cardinality
     */
    public void mapForPorting(ColumnStatistics colStat, BigDecimal tableCard) throws DatalessException
    {
        if(Constants.status == -1 && !(colStat instanceof OracleColumnStatistics)) {
        Constants.status = JOptionPane.showConfirmDialog(null,"You have chosen other DB engine statistics to transfer to Oracle. "
                                    + "Clicking on Yes will use input histogram column values as Oracle height balanced histogram end points, No will set Oracle Histogram to be null.", "Choose the option.", JOptionPane.YES_NO_OPTION);
        }
        super.mapForPorting(colStat, tableCard);
        if (Constants.status == JOptionPane.NO_OPTION) {
            this.setHistogram(null);
        }
        if(this.getHistogram() != null) { // We always port to QuantileHistogram
            this.histogramType = OracleColumnStatistics.HeightBalanced;
        }
        if(colStat instanceof DB2ColumnStatistics) {
            // local metadata fields: high2key, low2key, avgColLen, subCount, subDelimLength, Frequency Histogram
            DB2ColumnStatistics columnStatistics = (DB2ColumnStatistics) colStat;
            // avgColLen - avgColLen
            this.setAvgColLen(columnStatistics.getAvgColLen());
            // Other metdata fields can not be ported to Oracle
        } else if(colStat instanceof OracleColumnStatistics) {
            // local metadata fields: minValue, maxValue, density, avgColLen, histogramType, numBucket, oralceHistogram
            OracleColumnStatistics columnStatistics = (OracleColumnStatistics) colStat;
            this.setMinValue(columnStatistics.getMinValue());
            this.setMaxValue(columnStatistics.getMaxValue());
            this.setDensity(columnStatistics.getDensity());
            this.setAvgColLen(columnStatistics.getAvgColLen());
            this.setHistogramType(columnStatistics.getHistogramType());
            this.setIsActualValuePresent(columnStatistics.isIsActualValuePresent());
            this.setUserInput(columnStatistics.isUserInput());
            this.setNumBuckets(columnStatistics.getNumBuckets());
            this.setOralceHistogram(columnStatistics.getOralceHistogram());
        } else if(colStat instanceof MSSQLColumnStatistics) {
            // local metadata fields: -
        } else if(colStat instanceof PostgresColumnStatistics) {
            // local metadata fields: n_distinct, null_frac, avgWidth, most_common_vals, most_common_freqs, FreqHistogram, histogram_bounds, correlation
            PostgresColumnStatistics columnStatistics = (PostgresColumnStatistics) colStat;
            // avgWidth - avgColLen
            this.setAvgColLen(columnStatistics.getAvgWidth());
            // Other metdata fields can not be ported to Oracle
        } //else if(relStat instanceof SybaseRelationStatistics) { }
        this.adjustHistogram(tableCard, this.DEFAULT_BUCKET_SIZE);
    }

    /**
     * Scale (size based) the OracleColumnStatistics attributes by the scale factor.
     * @param sf scale factor
     * @throws DatalessException 
     */
    public void scale(int sf) throws DatalessException
    {
        super.scale(sf);
        // Histogram
        if(!this.histogramType.equals(OracleColumnStatistics.None))
        {
            OracleHistObject[] hist = this.oralceHistogram;
            for (int i = 0; i < this.numBuckets; i++) {
                String endpoint_number = hist[i].getEndPointNumber();
                String endpoint_value = hist[i].getEndPointValue();
                if(this.histogramType.equals(OracleColumnStatistics.HeightBalanced)) {
                    if (constraint.isPartOfPK() && (DataType.isDouble(getColumnType()) || DataType.isInteger(getColumnType()) || DataType.isNumeric(getColumnType()))) {
                        Double Double_endpoint_value = Double.parseDouble(endpoint_value);
                        Double_endpoint_value = Double_endpoint_value * sf;
                        endpoint_value = Double_endpoint_value+"";
                    }
                } else { // Frequency Histogram
                    if (! constraint.isPartOfPK()) {
                        Double Double_endpoint_number = Double.parseDouble(endpoint_number);
                        Double_endpoint_number = Double_endpoint_number * sf;
                        endpoint_number = Double_endpoint_number+"";
                    }
                }
                hist[i].setEndPointNumber(endpoint_number);
                hist[i].setEndPointValue(endpoint_value);
            }
            this.oralceHistogram = hist;
        }
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "col_card|null_cnt|avg_col_len|density|histogramType\n";
        ret = ret + this.colCard + "|" + this.numNulls + "|" + this.avgColLen + "|" + this.density + "|" + this.histogramType + "\n";

        ret = ret + "num_bukcets|minVal|maxVal\n";
        ret = ret + this.numBuckets + "|" + this.minValue.getString() + "|" + this.maxValue.getString() + "\n";
        ret = ret + "\n Histogram \n";
        ret = ret + "no|endpointnumber|endpointvalue|endpointactualvalue\n";
        if(!histogramType.equals(OracleColumnStatistics.None))
        {
            // Read OracleHistObject and update endpoint_number, endpoint_value
            OracleHistObject[] hist = this.getOralceHistogram();
            for (int i = 0; i < this.numBuckets; i++) {
                ret  = ret + (i+1) +"|"+ hist[i].getEndPointNumber() +"|"+ hist[i].getEndPointValue() +"|"+hist[i].getEndPointActualValue()+"\n";
            }
         }
        return ret;
    }

}
