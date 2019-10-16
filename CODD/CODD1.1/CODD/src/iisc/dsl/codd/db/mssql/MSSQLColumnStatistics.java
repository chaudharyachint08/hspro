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
package iisc.dsl.codd.db.mssql;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.db2.DB2ColumnStatistics;
import iisc.dsl.codd.db.oracle.OracleColumnStatistics;
import iisc.dsl.codd.db.postgres.PostgresColumnStatistics;
import iisc.dsl.codd.ds.Constraint;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * MSSQL Column Statistics of a relation is represented with this class.
 * @author dsladmin
 */
public class MSSQLColumnStatistics extends ColumnStatistics{

    /**
     * COLNAME - Name of the column.
     * COLCARD - Number of distinct values in the column; -1 if statistics are not collected; -2 for inherited columns and columns of hierarchy tables.
     */

    public static int DefaultBukcetSize = 200; // Default Bucket Size
    public static int MaxBukcetSize = 200; // Maximum Bucket Size
    /**
     * Constructs a OracleColumnStatistics for the specified column with the default values.
     * @param relName Relation name
     * @param colName Column name
     * @param colType Data type of the column
     * @param constraint Integrity Constraint
     */
    public MSSQLColumnStatistics(String relName, String colName, String colType, Constraint constraint)
    {
        super(relName, colName, colType, constraint, DefaultBukcetSize, MaxBukcetSize);
        BigDecimal minusOneBI = new BigDecimal("-1");
        this.colCard = new BigDecimal(minusOneBI+"");
        this.numNulls = new BigDecimal(minusOneBI+"");
    }

    public Map getQuantileHistogram() {
        return histogram;
    }

    public void setQuantileHistogram(Map quantileHistogram) {
        this.histogram = quantileHistogram;
    }

    /**
     * Initializes the MSSQLColumnStatistics metadata fields with the specified ColumnStatistics object.
     * This is used in case of porting metadata.
     * Maps the metadata field of other engine ColumnStatistics metadata to MSSQL.
     * This will not be used as there is no construct mode support for MSSQL.
     * @param colStat ColumnStatistics
     * @param tableCard table Cardinality
     */
    public void mapForPorting(ColumnStatistics colStat, BigDecimal tableCard) throws DatalessException
    {
        super.mapForPorting(colStat, tableCard);
        if(colStat instanceof DB2ColumnStatistics) {
            // local metadata fields: high2key, low2key, avgColLen, subCount, subDelimLength, Frequency Histogram
        } else if(colStat instanceof OracleColumnStatistics) {
            // local metadata fields: minValue, maxValue, density, avgColLen, histogramType, numBucket, oralceHistogram
        } else if(colStat instanceof MSSQLColumnStatistics) {
            // local metadata fields: -
        } else if(colStat instanceof PostgresColumnStatistics) {
            // local metadata fields: n_distinct, null_frac, avgWidth, most_common_vals, most_common_freqs, FreqHistogram, histogram_bounds, correlation
        } //else if(relStat instanceof SybaseRelationStatistics) { }
        this.adjustHistogram(tableCard, this.DEFAULT_BUCKET_SIZE);
    }

    /**
     * Scale (size based) the MSSQLColumnStatistics attributes by the scale factor.
     * This will not be used as there is no construct mode support for MSSQL.
     * @param sf scale factor
     * @throws DatalessException 
     */
    public void scale(int sf) throws DatalessException
    {
        super.scale(sf);
    }

    @Override
    public String toString()
    {
        String ret = new String();
        ret = ret + "col_card|null_cnt\n";
        ret = ret + this.colCard + "|" + this.numNulls + "\n";
        // Quant Hist
        TreeMap<Integer, HistogramObject> mapQ = (TreeMap) this.getHistogram();
        if (mapQ != null) {
            ret = ret+ "\n Histogram \n";
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
                if (distCount != null) //If DISTCOUNT is present
                {
                    distCount = distCount;
                }
                ret = ret+ seqno + "|" + colValue + "|" + valCount+"|"+distCount+" \n";
            }
        }
        return ret;
    }
}
