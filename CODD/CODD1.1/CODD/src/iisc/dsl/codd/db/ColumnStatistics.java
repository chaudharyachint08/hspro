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

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Column Statistics of a relation is represented with this class.
 * Each of the derived subclass should have scale and mapForPorting function
 * and call the super class function.
 * @author dsladmin
 */
public class ColumnStatistics implements Serializable {
	
	private static final long serialVersionUID = 5069467056990308672L;
	
	/**
     * Column Name of the statistics being stored (store in CAPS always and do transformation as required)
     */
    protected String columnName;
    
    /**
     * Relation Name in which this column is present (store in CAPS always and do transformation as required)
     */
    protected String relationName;
    
    /**
     * String representation of the data type of the column
     */
    protected String columnType;
    
    /**
     * Number of NULL values in the column
     */
    protected BigDecimal numNulls;
    
    /**
     * Number of distinct values in the column
     */
    protected BigDecimal colCard;
    
    /**
     * Integrity Constraint associated with the column
     */
    protected Constraint constraint;
    
    /**
     * Default number of histogram buckets
     */
    protected int DEFAULT_BUCKET_SIZE;
    
    /**
     * Maximum allowed number of histogram buckets
     */
    protected int MAX_BUCKET_SIZE;

    /**
     * Histogram
     * It is a Mapping of Seq no (DB2) / Bucket No (Other DB Engine) to the HistogramObject.
     * <Integer(Seqno [DB2] / bucketNo ), HistogramObject(ColValue, Frequency, DistCount)>
     * OracleColumnStatistics does not use this.
     */
    protected Map<Integer, HistogramObject> histogram;

    /**
     * Constructs a ColumnStatistics for the specified column with the default values.
     * @param relName Relation name
     * @param colName Column name
     * @param columnType Data type of the column
     * @param constraint Integrity Constraint
     * @param DEFAULT_BUCKET_SIZE Default bucket size
     * @param MAX_BUCKET_SIZE maximum bucket size
     */
    public ColumnStatistics(String relName, String colName, String columnType, Constraint constraint, int DEFAULT_BUCKET_SIZE, int MAX_BUCKET_SIZE)
    {
        this.columnName = colName;
        this.relationName = relName;
        this.columnType = columnType;
        this.numNulls = new BigDecimal("-1");
        this.colCard = new BigDecimal("-1");
        this.histogram = null;
        this.constraint = constraint;
        this.DEFAULT_BUCKET_SIZE = DEFAULT_BUCKET_SIZE;
        this.MAX_BUCKET_SIZE = MAX_BUCKET_SIZE;
    }

    /**
     * Deep Cloning (copy) of the passed object.
     * @param colStat ColumnStatistics object
     */
    public ColumnStatistics(ColumnStatistics colStat)
    {
        this.columnName = colStat.getColumnName();
        this.relationName = colStat.getRelationName();
        this.columnType = colStat.getColumnType();
        this.numNulls = new BigDecimal(colStat.getNumNulls().toString());
        this.colCard = new BigDecimal(colStat.getColCard().toString());
        this.constraint = new Constraint(colStat.getConstraint());
        this.DEFAULT_BUCKET_SIZE = colStat.getDEFAULT_BUCKET_SIZE();
        this.MAX_BUCKET_SIZE = colStat.getMAX_BUCKET_SIZE();


        this.histogram = null;
        TreeMap<Integer, HistogramObject> mapQuantile = (TreeMap<Integer, HistogramObject>) colStat.getHistogram();
        if (mapQuantile != null) {
            this.histogram = new TreeMap<Integer, HistogramObject>();
            Set<Entry<Integer, HistogramObject>> set = mapQuantile.entrySet();
            Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
            while (i.hasNext()) {
                Map.Entry<Integer, HistogramObject> me = i.next();
                Integer seqno = (Integer) me.getKey();
                HistogramObject histogramObject = (HistogramObject) me.getValue();
                this.histogram.put(new Integer(seqno), new HistogramObject(histogramObject));
            }
        }
    }

    public int getDEFAULT_BUCKET_SIZE() {
        return DEFAULT_BUCKET_SIZE;
    }

    public void setDEFAULT_BUCKET_SIZE(int DEFAULT_BUCKET_SIZE) {
        this.DEFAULT_BUCKET_SIZE = DEFAULT_BUCKET_SIZE;
    }

    public int getMAX_BUCKET_SIZE() {
        return MAX_BUCKET_SIZE;
    }

    public void setMAX_BUCKET_SIZE(int MAX_BUCKET_SIZE) {
        this.MAX_BUCKET_SIZE = MAX_BUCKET_SIZE;
    }

    public BigDecimal getColCard() {
        return colCard;
    }

    public void setColCard(BigDecimal colCard) {
        this.colCard = colCard;
    }

    public String getColumnName()
    {
        return this.columnName;
    }

    public void setColumnName(String colName)
    {
        this.columnName  = colName;
    }

    public String getRelationName()
    {
        return this.relationName;
    }

    public void setRelationName(String relName)
    {
        this.relationName = relName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public BigDecimal getNumNulls()
    {
        return this.numNulls;
    }

    public void setNumNulls(BigDecimal numNulls)
    {
        this.numNulls = numNulls;
    }

    public Map<Integer, HistogramObject> getHistogram() {
        return histogram;
    }

    public void setHistogram(Map<Integer, HistogramObject> histogram) {
        this.histogram = histogram;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public void setConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    /**
     * Initializes the super class ColumnStatistics metadata fields with the specified ColumnStatistics object.
     * This is used in case of porting metadata.
     * At Non-Engine Specific column level (i.e. Super class), all metadata fields are portable.
     * @param colStat ColumnStatistics
     * @param tableCard table Cardinality
     * @throws DatalessException
     */
    public void mapForPorting(ColumnStatistics colStat, BigDecimal tableCard) throws DatalessException
    {
        //this.setColumnName(colStat.getColumnName());
        //this.setRelationName(colStat.getRelationName());
        //this.setColumnType(colStat.getColumnType());
        //this.setConstraint(colStat.getConstraint());
        //this.setMAX_BUCKET_SIZE(colStat.getMAX_BUCKET_SIZE());
        //this.setDEFAULT_BUCKET_SIZE(colStat.getDEFAULT_BUCKET_SIZE());
        // All the above values are initialized with the destination constructor.
        if(colStat.getColCard().compareTo(BigDecimal.ZERO) > 0) {
            this.setColCard(colStat.getColCard());
        } // else default value
        this.setNumNulls(colStat.getNumNulls());
        this.setHistogram(colStat.getHistogram());
        // Do Histogram Adjustment in sub class level.
    }

    /**
     * Scale (size based) the ColumnStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf) throws DatalessException
    {
        BigDecimal scalefactor = new BigDecimal(((int)sf)+"");
        if(constraint.isPartOfPK() && this.colCard.compareTo(BigDecimal.ZERO) > 0) {
            this.colCard = this.colCard.multiply(scalefactor);
        }
        if(this.numNulls.compareTo(BigDecimal.ZERO) > 0) {
            this.numNulls = this.numNulls.multiply(scalefactor);
        }

        // Histogram
        TreeMap<Integer, HistogramObject> map = (TreeMap<Integer, HistogramObject>) this.histogram;
        if(map != null)
        {
            Set<Entry<Integer, HistogramObject>> set = map.entrySet();
            Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
            while (i.hasNext()) {
                Map.Entry<Integer, HistogramObject> me = i.next();
                Integer seqno = (Integer) me.getKey();
                HistogramObject histogramObject = (HistogramObject) me.getValue();
                String colValue = histogramObject.getColValue();
                Double valCount = histogramObject.getValCount();
                Double distCount = null;
                if (histogramObject.getDistCount() != null) {
                    distCount = histogramObject.getDistCount();
                }
                if (constraint.isPartOfPK() || constraint.isFK()) {
					if (DataType.isDouble(getColumnType())) {
						colValue = "" + (Double.parseDouble(colValue) * sf);
					} else if (DataType.isInteger(getColumnType())){
						colValue = "" + (Integer.parseInt(colValue) * (int)sf);
					}else if(DataType.isBigDecimal(getColumnType()) || DataType.isNumeric(getColumnType())){
						colValue = "" + (new BigDecimal(colValue)).multiply(new BigDecimal(sf+""));
					}
                    if (distCount != null) {
                        distCount = distCount * sf;
                    }
                }
                valCount = valCount * sf;

                histogramObject.setColValue(colValue);
                histogramObject.setValCount(valCount);
                histogramObject.setDistCount(distCount);
                map.put(seqno, histogramObject);
            }
            this.histogram = map;
        }
    }
    
    /**
     * Adjust the histogram to have DEFAULT_BUCKET_SIZE buckets.
     * Adjust double value to integer value and still sums upto tableCard.
     *
     * @param tableCard Table Cardinality
     * @param bucketSize Number of buckets in the destination histogram. Use DEFAULT_BUCKET_SIZE if < 1
     */
    public void adjustHistogram(BigDecimal tableCard, int bucketSize)
    {
        if(bucketSize < 1 || bucketSize > this.MAX_BUCKET_SIZE) {
            bucketSize = this.DEFAULT_BUCKET_SIZE;
        }
        TreeMap<Integer, HistogramObject> map = (TreeMap<Integer, HistogramObject>) this.histogram;
        TreeMap<Integer, HistogramObject> newMap = new TreeMap<Integer, HistogramObject>();
        int size = -1;
        if(map != null ) {
            size = map.size();
        }
        if(map != null && size > bucketSize)
        {
            // Leaving the first and last bucket, choose any (DEFAULT_BUCKET_SIZE - 2) bukcets to keep those values.
            ArrayList<Integer> randomBucketNumbersToKeep = new ArrayList<Integer>();
            randomBucketNumbersToKeep.add(new Integer(1));
            randomBucketNumbersToKeep.add(new Integer(size));
            Random randomGenerator = new Random();
            for( int i = 1; i< bucketSize -1; i++)
            {
                Integer randomInteger;
                do {
                    // Get a random integer between 2 and size -1
                    // The above function geneares a number between 0 and size-3. Increment by 2 to get from 2 to size-1 numbers
                    int randomInt = randomGenerator.nextInt(size-2);
                     randomInteger = new Integer(randomInt+2);
                }while(randomBucketNumbersToKeep.contains(randomInteger));
                randomBucketNumbersToKeep.add(randomInteger);
            }
            Integer prevValueCount = 0;
            Integer prevDistCount = 0;
            Set<Entry<Integer, HistogramObject>> set = map.entrySet();
            Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
            int addedCount = 0;
            int sumValCount = 0;
            int sumDistCount = 0;
            Constants.CPrintToConsole(" Converting to Histogram to have "+bucketSize+" buckets.", Constants.DEBUG_SECOND_LEVEL_Information);
            Constants.CPrintToConsole("SEQNO|COLVALUE|VALCOUNT|distCount", Constants.DEBUG_SECOND_LEVEL_Information);
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
                if(randomBucketNumbersToKeep.contains(seqno))
                {
                    // Add it to the new Map
                    addedCount ++;
                    valCount = valCount.intValue();
                    sumValCount = sumValCount + valCount;
                    if (distCount != null) {
                        distCount = distCount.intValue();
                        sumDistCount = sumDistCount + distCount;
                    }
                    if (addedCount ==  bucketSize) // last Bucket, Adjust table Card
                    {
                        Double remaining = tableCard.doubleValue() - sumValCount;
                        valCount = valCount + remaining.intValue();
                        if (distCount != null) {
                            distCount = distCount.intValue();
                            remaining = colCard.doubleValue() - sumDistCount;
                            distCount = distCount + remaining.intValue();;
                        }
                    }
                    HistogramObject histogramObjectNew;
                    if(distCount != null) {
                        histogramObjectNew = new HistogramObject(colValue,valCount.doubleValue(),distCount.doubleValue());
                    } else {
                        histogramObjectNew = new HistogramObject(colValue,valCount.doubleValue(),null);
                    }
                    Constants.CPrintToConsole(addedCount + "|" + colValue + "|" + valCount + "| " + distCount, Constants.DEBUG_SECOND_LEVEL_Information);
                    newMap.put(new Integer(addedCount), histogramObjectNew);
                    // Initialize "prev" values
                    prevValueCount = 0;
                    if (prevDistCount != null) {
                        prevDistCount = 0;
                    }
                }
            }
            this.histogram = newMap;
        }  // end if
    }
}
