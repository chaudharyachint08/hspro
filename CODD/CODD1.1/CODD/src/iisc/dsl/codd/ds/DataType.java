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
package iisc.dsl.codd.ds;

import java.io.Serializable;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import iisc.dsl.codd.utils.DateParser;

/**
 * Implementation of Data Type of the Relational Columns.
 * Each columns of a relation is associated with a data type (INT, CHAR, VARCHAR, ..).
 * This class represents the data type. An object of this class is associated with each column
 * statistics, to identify the data type present on the column.
 *
 * The tool "supports" the following data types. i.e each column will belong to
 * "any one of the data type" listed below.
 *   data types from DB2
 * 1) BOOLEAN
 * 2) VARCHAR
 * 3) NUMERIC
 * 4) INTEGER
 * 5) REAL
 * 6) DECIMAL
 * 7) DOUBLE
 * 8) FLOAT
 * 9) DATE
 * 10) TIME
 * 11) CHARACTER
 *   New (other than above list) data types from ORACLE
 * 12) NUMBER
 * 13) VARCHAR2
 * 14) CHAR
 * 15) TIMESTAMP
 *   New (other than above list) data types from Postgres
 * 16) int2
 * 17) int4
 * 18) int8
 * 19) float4
 * 20) float8
 * 21) timet
 * 22) timestamptz
 * 23) bpchar
 *  New (other than above list) data types from NonStopSQL
 * 24) DATETIME
 * 25) SIGNED DECIMAL
 * 26) UNSIGNED DECIMAL
 * 27) SIGNED INTEGER
 * 28) INTERVAL
 * 29) UNSIGNED BP INT
 * 30) UNSIGNED INTEGER
 * 31) SIGNED LARGEINT
 * 32) SIGNED NUMERIC
 * 33) UNSIGNED NUMERIC
 * 34) SIGNED SMALLINT
 * 35) UNSIGNED SMALLINT
 * 36) LONG VARCHAR
 * @author dsladmin
 */
public class DataType implements Serializable{

    /**
     * String Constant for BOOLEAN data type.
     */
    public final static String BOOLEAN = "boolean";
    /**
     * String Constant for VARCHAR data type.
     */
    public final static String VARCHAR = "varchar";
    /**
     * String Constant for NUMERIC data type.
     */
    public final static String NUMERIC = "numeric";
    /**
     * String Constant for INTEGER data type.
     */
    public final static String INTEGER = "integer";
    /**
     * String Constant for REAL data type.
     */
    public final static String REAL = "real";
    /**
     * String Constant for DECIMAL data type.
     */
    public final static String DECIMAL = "decimal";
    /**
     * String Constant for DOUBLE data type.
     */
    public final static String DOUBLE = "double";
    /**
     * String Constant for FLOAT data type.
     */
    public final static String FLOAT = "float";
    /**
     * String Constant for DATE data type.
     */
    public final static String DATE = "date";
    /**
     * String Constant for TIME data type.
     */
    public final static String TIME = "time";
    /**
     * String Constant for DATETIME data type.
     */
    public final static String DATETIME = "DATETIME";
    /**
     * String Constant for CHARACTER data type.
     */
    public final static String CHARACTER = "character";
    /**
     * String Constant for NUMBER data type.
     */
    public final static String NUMBER = "number";
    /**
     * String Constant for VARCHAR2 data type.
     */
    public final static String VARCHAR2 = "varchar2";
    /**
     * String Constant for CHAR data type.
     */
    public final static String CHAR = "char";
    /**
     * String Constant for TIMESTAMP data type.
     */
    public final static String TIMESTAMP = "timestamp";

    /**
     * String Constant for int2 data type.
     */
    public final static String INT2 = "int2";
    /**
     * String Constant for int4 data type.
     */
    public final static String INT4 = "int4";
    /**
     * String Constant for int8 data type.
     */
    public final static String INT8 = "int8";
    /**
     * String Constant for float4 data type.
     */
    public final static String FLOAT4 = "float4";
    /**
     * String Constant for float8 data type.
     */
    public final static String FLOAT8 = "float8";
    /**
     * String Constant for timetx data type.
     */
    public final static String TIMETZ = "timetz";
    /**
     * String Constant for timestamptz data type.
     */
    public final static String TIMESTAMPTZ = "timestamptz";
    /**
     * String Constant for bpchar data type.
     */
    public final static String BPCHAR = "bpchar";
    /**
     * String Constant for SIGNED DECIMAL data type.
     */
    public final static String SIGNED_DECIMAL = "SIGNED DECIMAL";
    /**
     * String Constant for UNSIGNED DECIMAL data type.
     */
    public final static String UNSIGNED_DECIMAL = "UNSINGED DECIMAL";
    /**
     * String Constant for SIGNED INTEGER data type.
     */
    public final static String SIGNED_INTEGER = "SIGNED INTEGER";
    /**
     * String Constant for INTERVAL data type.
     */
    public final static String INTERVAL = "INTERVAL";
    /**
     * String Constant for UNSINGED BP INT data type.
     */
    public final static String UNSIGNED_BP_INT = "UNSINGED BP INT";
    /**
     * String Constant for UNSIGNED INTEGER data type.
     */
    public final static String UNSIGNED_INTEGER = "UNSIGNED INTEGER";
    /**
     * String Constant for SIGNED LARGEINT data type.
     */
    public final static String SIGNED_LARGEINT = "SIGNED LARGEINT";
    /**
     * String Constant for SIGNED_NUMERIC data type.
     */
    public final static String SIGNED_NUMERIC = "SIGNED NUMERIC";
    /**
     * String Constant for UNSIGNED NUMERIC data type.
     */
    public final static String UNSIGNED_NUMERIC = "UNSIGNED NUMERIC";
    /**
     * String Constant for SIGNED SMALLINT data type.
     */
    public final static String SIGNED_SMALLINT = "SIGNED SMALLINT";
    /**
     * String Constant for UNSIGNED SMALLINT data type.
     */
    public final static String UNSIGNED_SMALLINT = "UNSIGNED SMALLINT";
    /**
     * String Constant for LONG VARCHAR data type.
     */
    public final static String LONG_VARCHAR = "LONG VARCHAR";
  
    /**
     * String attribute representing the data type (one of the String Constants)
     */
    private String type;
    /**
     * Represents a value, which is of 'type' data type.
     */
    private Object value;

    /**
     * Constructor for DataType initialized from another DataType object.
     * @param dt - DataType object.
     */
    public DataType(DataType dt)
    {
        this(dt.type,dt.value.toString());
    }
    /**
     * Constructor for DataType.
     * @param dataType - String Constant representing the data type.
     * @param val - value in String format
     */
    public DataType(String dataType, String val)
    {
        this.type = dataType;
        if(dataType.equalsIgnoreCase(DataType.BOOLEAN))
        {
            if(val.equalsIgnoreCase("true"))
                this.value = true;
            else
                this.value = false;
        }
        else if(isString())
        {
            this.value = new String(val);
        }
        else if(isBigInteger()){
            BigInteger temp = new BigInteger(val);
            this.value = temp;
        }
        else if(isInteger())
        {
            this.value = Integer.parseInt(val);
        }
        else if(isNumeric())
        {
        	System.out.println("Value :" + val);
        	try
        	{
        		DecimalFormat decimalFormat = new DecimalFormat();
        		decimalFormat.setParseBigDecimal(true);
        		BigDecimal bd = (BigDecimal) decimalFormat.parse(val);
        		this.value = bd;
        	} catch (java.text.ParseException e)
        	{
        		Constants.CPrintErrToConsole(e);
        	}
        }
        else if(isDouble())
        {
            this.value = new Double(val);
        }
        else if(isDate())
        {
        	/*
        	 * @dk Created DateParser which can accept date strings of different format
        	 * */
            this.value = java.sql.Date.valueOf(DateParser.parse(val));
        }
        else if(isTime())
        {
            this.value = java.sql.Timestamp.valueOf(val);
        }
    }

    /**
     * Returns true, if the object represents a Double object.
     * @return true, if data type is of type double / decimal / float / real / number.
     *         false, otherwise.
     */
    public boolean isDouble()
    {
    	/* Ashoke - SIGNED_NUMERIC and UNSIGNED_NUMERIC are specific to NonStop and are similar to DECIMAL. Hence moved here. */
        if (type.equalsIgnoreCase(DataType.REAL) || type.equalsIgnoreCase(DataType.DECIMAL) || type.equalsIgnoreCase(DataType.DOUBLE) || type.equalsIgnoreCase(DataType.FLOAT) || type.equalsIgnoreCase(DataType.NUMBER)  || type.equalsIgnoreCase(DataType.FLOAT4)  || type.equalsIgnoreCase(DataType.FLOAT8) || type.equalsIgnoreCase(DataType.SIGNED_DECIMAL) || type.equalsIgnoreCase(DataType.UNSIGNED_DECIMAL) || type.equalsIgnoreCase(DataType.SIGNED_NUMERIC) || type.equalsIgnoreCase(DataType.UNSIGNED_NUMERIC)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the object represents a Integer object.
     * @return true, if data type is of type integer / number.
     *         false, otherwise.
     */
    public boolean isInteger()
    {
        if (type.equalsIgnoreCase(DataType.INTEGER) || type.equalsIgnoreCase(DataType.INT2) || type.equalsIgnoreCase(DataType.INT4) || type.equalsIgnoreCase(DataType.INT8) || type.equalsIgnoreCase(SIGNED_INTEGER) 
                || type.equalsIgnoreCase(UNSIGNED_BP_INT) || type.equalsIgnoreCase(UNSIGNED_INTEGER) || type.equalsIgnoreCase(SIGNED_INTEGER) || type.equalsIgnoreCase(SIGNED_SMALLINT) || type.equalsIgnoreCase(UNSIGNED_SMALLINT)) {
            return true;
        }
        return false;
    }
    
    /**
     * Returns true, if the object represents a Numeric object.
     * @return true, if data type is of type numeric
     *         false, otherwise.
     */
    public boolean isNumeric()
    {
		/* Ashoke - Moved to isDouble() */
        if (type.equalsIgnoreCase(DataType.NUMERIC))
        		return true;

        return false;
    }
    
    
    /**
     * Returns true, if the object represents a BigInteger object.
     * @return true, if data type is of type LArgeInt.
     *         false, otherwise.
     */
    public boolean isBigInteger()
    {
        if (type.equalsIgnoreCase(SIGNED_LARGEINT)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the object represents a String object.
     * @return true, if data type is of type varchar / varchar2 / char / character / bpchar.
     *         false, otherwise.
     */
    public boolean isString()
    {
        if(type.equalsIgnoreCase(DataType.VARCHAR) || type.equalsIgnoreCase(DataType.VARCHAR2) || type.equalsIgnoreCase(DataType.CHAR) || type.equalsIgnoreCase(DataType.CHARACTER)  || type.equalsIgnoreCase(DataType.BPCHAR) || type.equalsIgnoreCase(LONG_VARCHAR)) {
            return true;
        }
        return false;
    }

     /**
     * Returns true, if the object represents a Date object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type date / datetime.
     *         false, otherwise.
     */
    public boolean isDate()
    {
        if(type.equalsIgnoreCase(DataType.DATE) || type.equalsIgnoreCase(DataType.DATETIME) ) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the object represents a TIME object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type time / datetime /timetz / timestamptz.
     *         false, otherwise.
     */
    public boolean isTime()
    {
        if(type.equalsIgnoreCase(DataType.TIME) || type.equalsIgnoreCase(DataType.DATETIME)  || type.equalsIgnoreCase(DataType.TIMESTAMP) || type.equalsIgnoreCase(DataType.TIMESTAMPTZ) || type.equalsIgnoreCase(DataType.TIMETZ)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the parameter 'type' represents a Double object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type double / decimal / float / real / number.
     *         false, otherwise.
     */
    public static boolean isDouble(String type)
    {
    	/* Ashoke SIGNED_NUMERIC and UNSIGNED_NUMERIC are specific to NonStop */ 
        if (type.equalsIgnoreCase(DataType.REAL) || type.equalsIgnoreCase(DataType.DECIMAL) || type.equalsIgnoreCase(DataType.DOUBLE) || type.equalsIgnoreCase(DataType.FLOAT) || type.equalsIgnoreCase(DataType.NUMBER)  || type.equalsIgnoreCase(DataType.FLOAT4)  || type.equalsIgnoreCase(DataType.FLOAT8)
                || type.equalsIgnoreCase(SIGNED_DECIMAL) || type.equalsIgnoreCase(UNSIGNED_DECIMAL) ||  type.equalsIgnoreCase(SIGNED_NUMERIC) || type.equalsIgnoreCase(UNSIGNED_NUMERIC)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the parameter 'type' represents a Integer object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type  integer / number.
     *         false, otherwise.
     */
    public static boolean isInteger(String type)
    {
        if (type.equalsIgnoreCase(DataType.INTEGER) || type.equalsIgnoreCase(DataType.INT2) || type.equalsIgnoreCase(DataType.INT4) || type.equalsIgnoreCase(DataType.INT8) || type.equalsIgnoreCase(SIGNED_INTEGER) 
                || type.equalsIgnoreCase(UNSIGNED_BP_INT) || type.equalsIgnoreCase(UNSIGNED_INTEGER) || type.equalsIgnoreCase(SIGNED_INTEGER) || type.equalsIgnoreCase(SIGNED_SMALLINT) || type.equalsIgnoreCase(UNSIGNED_SMALLINT)) {
            return true;
        }
        return false;
    }
    
    /**
     * Returns true, if the parameter 'type' represents a Numeric object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type numeric.
     *         false, otherwise.
     */
    public static boolean isNumeric(String type)
    {
    	/* Ashoke - Moved SIGNED_NUMERIC and UNSIGNED_NUMERIC to isDouble */
        if (type.equalsIgnoreCase(DataType.NUMERIC)) {
            return true;
        }
        return false;
    }

/**
     * Returns true, if the parameter 'type' represents a Integer object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type numeric / integer / number.
     *         false, otherwise.
     */
    public static boolean isBigDecimal(String type)
    {
        if (type.equalsIgnoreCase(DataType.SIGNED_LARGEINT) ) {
            return true;
        }
        return false;
    }
    /**
     * Returns true, if the parameter 'type' represents a String object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type varchar / varchar2 / char / character.
     *         false, otherwise.
     */
    public static boolean isString(String type)
    {
        if(type.equalsIgnoreCase(DataType.VARCHAR) || type.equalsIgnoreCase(DataType.VARCHAR2) || type.equalsIgnoreCase(DataType.CHAR) || type.equalsIgnoreCase(DataType.CHARACTER)  || type.equalsIgnoreCase(DataType.BPCHAR)|| type.equalsIgnoreCase(LONG_VARCHAR)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the parameter 'type' represents a Date object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type date / datetime..
     *         false, otherwise.
     */
    public static boolean isDate(String type)
    {
        if(type.equalsIgnoreCase(DataType.DATE) || type.equalsIgnoreCase(DataType.DATETIME) ) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if the parameter 'type' represents a Time object.
     * @param type - String Constant representing the data type
     * @return true, if data type is of type time / datetime..
     *         false, otherwise.
     */
    public static boolean isTime(String type)
    {
        if(type.equalsIgnoreCase(DataType.TIME) || type.equalsIgnoreCase(DataType.DATETIME)  || type.equalsIgnoreCase(DataType.TIMESTAMP) || type.equalsIgnoreCase(DataType.TIMESTAMPTZ) || type.equalsIgnoreCase(DataType.TIMETZ)) {
            return true;
        }
        return false;
    }

    /**
     * Returns the Double value represented by the attribute 'value';
     * @return value
     */
    public Double getDouble()
    {
        return (Double)value;
    }

    /**
     * Replaces the Double value represented by the attribute 'value' with the specified double value;
     * @param d - double value d
     */
    public void setDouble(double d)
    {
        value = d;
    }

    /**
     * Returns the Integer value represented by the attribute 'value';
     * @return value
     */
    public Integer getInteger()
    {
        return (Integer)value;
    }
    
    /**
     * Returns the BigDecimal value represented by the attribute 'value';
     * Used to convert Numeric datatype of PostgreSQL
     * @return value
     */
    public BigDecimal getBigDecimal()
    {
        return (BigDecimal)value;
    }
    
    /**
     * Replaces the BigDecimal value represented by the attribute 'value' with the specified Bigdecimal value;
     * @param d - BigDecimal value d
     */
    public void setBigDecimal(BigDecimal d)
    {
        value = d;
    }

    /**
     * Replaces the Integer value represented by the attribute 'value' with the specified Integer value;
     * @param d - integer value d
     */
    public void setInteger(Integer d)
    {
        value = d;
    }
    
    /**
     * Returns the Integer value represented by the attribute 'value';
     * @return value
     */
    public BigInteger getBigInteger()
    {
        return new BigInteger(value+"");
    }

    /**
     * Replaces the Integer value represented by the attribute 'value' with the specified Integer value;
     * @param d - integer value d
     */
    public void setBigInteger(BigInteger d)
    {
        value = d;
    }

    /**
     * Returns the String value represented by the attribute 'value';
     * @return value
     */
    public String getString()
    {
        return value.toString();
    }

    /**
     * Replaces the String value represented by the attribute 'value' with the specified String value;
     * @param d - string value d
     */
    public void setString(String d)
    {
        value = d;
    }

    /**
     * Returns the data type value represented by the attribute 'type';
     * @return type
     */
    public String getType()
    {
        return type;
    }

    /**
     * Replaces the data type represented by the attribute 'type' with the specified type;
     * @param d - data type in String format
     */
    public void setType(String d)
    {
        type = d;
    }


    /**
     * Subtracts the value represented by the parameter object (dt.value) from object current value.
     * @param dt - data type object
     */
    public void subtract(DataType dt)
    {
        if (isInteger()) {
                this.setInteger(this.getInteger() - (dt.getInteger()));
        } else if (isNumeric()) {
            this.setBigDecimal(this.getBigDecimal().subtract(dt.getBigDecimal()));
        } else if (isDouble()) {
            this.setDouble(this.getDouble() - dt.getDouble());
        } else if(isBigInteger()) {
            this.setBigInteger(this.getBigInteger().subtract(dt.getBigInteger()));
        }
    }

    /**
     * Adds the value represented by the parameter object (dt.value) to the object current value.
     * @param dt - data type object
     */
    public void add(DataType dt)
    {
        if (isInteger()) {
            this.setInteger(this.getInteger() + dt.getInteger());
        } else if (isNumeric()) {
            this.setBigDecimal(this.getBigDecimal().add(dt.getBigDecimal()));
        } else if (isDouble()) {
            this.setDouble(this.getDouble() + dt.getDouble());
        } else if(isBigInteger()){
            this.setBigInteger(this.getBigInteger().add(dt.getBigInteger()));
        }
    }

    /**
     * Compares the value represented by the parameter object (dt.value) with the object current value.
     * @param dt - data type object
     * @return  0, if value represented by two objects are equal
     *          >0, if value represented object current value is greater than parameter object value.
     *          <0, if value represented object current value is lesser than parameter object value.
     */
    public int compare(DataType dt)
    {
        if(isBigInteger()) {
            return this.getBigInteger().compareTo(dt.getBigInteger());
        }else if (isInteger()) {
            return this.getInteger().compareTo(dt.getInteger());
        }else if (isNumeric()) {
                return this.getBigDecimal().compareTo(dt.getBigDecimal());
        } else if (isDouble()) {
            return this.getDouble().compareTo(dt.getDouble());
        }
        else
        {
            return this.getString().compareTo(dt.getString());
        }
    }
}