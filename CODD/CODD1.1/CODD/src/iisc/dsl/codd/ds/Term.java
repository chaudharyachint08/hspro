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

import java.util.ArrayList;

/**
 * Cost Scaling models the cost of a plan as a function of scaling factors of relations.
 * This class represents a single term in the cost function. Each term can have a value (double),
 * one or more scaling factors (product) and multiple log terms (log term is a term by itself).
 * @author dsladmin
 */
public class Term {
    /**
     * str represents the set of scaling factors belonging to this term in a string object.
     * In case of multiple scaling factors, they are appended together with a 'space' in between.
     */
    String str;
    /**
     * value represents the double value of  the term.
     */
    double value; // represents the coefficients
    /**
     * logTerms represents the list of log terms belonging to this term.
     */
    ArrayList<Term> logTerms;

    /**
     * Constructor for Term.
     * @param str - scaling factors represented in a String with a 'space' in between the different scaling factors
     */
    public Term(String str) {
        this.str = str;
        this.value = 1;
        this.logTerms = new ArrayList();
    }

    /**
     * Constructor for Term.
     * @param str - scaling factors represented in a String with a 'space' in between the different scaling factors
     * @param logTerm - a log term
     */
    public Term(String str, Term logTerm) {
        this.str = str;
        this.value = 1;
        this.logTerms = new ArrayList();
        logTerms.add(logTerm);
    }

    /**
     * Constructor for Term.
     * @param val - double value of the term
     */
    public Term(Double val) {
        this.str = new String();
        this.value = val;
        this.logTerms = new ArrayList();
    }

    /**
     * Constructor for Term.
     * @param str - scaling factors represented in a String with a 'space' in between the different scaling factors
     * @param val - double value of the term
     */
    public Term(String str, Double val) {
        this.str = str;
        this.value = val;
        this.logTerms = new ArrayList();
    }

    /**
     * Constructor for Term.
     * @param str - scaling factors represented in a String with a 'space' in between the different scaling factors
     * @param val - double value of the term
     * @param logTerm - a log term
     */
    public Term(String str, Double val, Term logTerm) {
        this.str = str;
        this.value = val;
        this.logTerms = new ArrayList();
        logTerms.add(logTerm);
    }

    /**
     * Constructor for Term.
     * @param str - scaling factors represented in a String with a 'space' in between the different scaling factors
     * @param val - double value of the term
     * @param logTerms - ArrayList of log terms of the term
     */
    public Term(String str, Double val, ArrayList<Term> logTerms) {
        this.str = str;
        this.value = val;
        this.logTerms = logTerms;
    }

    /**
     * Constructor for Term, initialized with a Term object.
     * @param term - Term object to initialize the new Term object
     */
    public Term(Term term) {
        this.str = new String(term.getStr());
        this.value = term.getValue();
        this.logTerms = new ArrayList(term.getLogTerms());
    }

    /**
     * Returns the scaling factors of the term.
     * @return string representation of the scaling factors
     */
    public String getStr() {
        return str;
    }

    /**
     * Replaces the scaling factors of the term with the specified scaling factors.
     * @param str - string representation of the scaling factors
     */
    public void setStr(String str) {
        this.str = str;
    }

    /**
     * Returns the double value of the term.
     * @return the double value of the term
     */
    public double getValue() {
        return value;
    }

    /**
     * Replaces the double value of the term with the specified double value.
     * @param value - double value of the term
     */
    public void setValue(double value) {
        this.value = value;
    }

    /**
     * Returns the list of log terms.
     * @return ArrayList of log terms
     */
    public ArrayList<Term> getLogTerms() {
        return logTerms;
    }

    /**
     * Replaces the list of log terms with the specified ArrayList.
     * @param logTerms - ArrayList of log terms
     */
    public void setLogTerm(ArrayList<Term> logTerms) {
        this.logTerms = logTerms;
    }

    /**
     * Includes the specified term into the current term.
     * value of the term is multiplied with the argument term value.
     * str of the term is appended with the argument term str.
     * log terms of the term is added with the argument term log terms.
     * @param term - term to multiply
     */
    public void include(Term term)
    {
        this.value = this.value * term.getValue();
        this.str = this.str+" "+term.str;
        // Assuming no repetition of scaling factors - TODO
        // This will be called only at the time of finding relations for Lemma 1, where FK relations are added one by one.
        /*

        String[] sf1 = this.str.split(" ");
        String[] sf2 = term.getStr().split(" ");
        String[] sf3 = new String[sf1.length + sf2.length];

        // Do Sort-Merge
        int first = 0;
        int second = 0;
        int third = 0;
        while(first < sf1.length && second < sf2.length)
        {
            String strLeft = sf1[first];
            String strRight = sf2[second];
            int comp = strLeft.compareToIgnoreCase(strRight);
            if(comp < 0) // strLeft is smaller
            {
                sf3[third] = strLeft;
                first++;
            }
            else // strRight is smaller
            {
                sf3[third] = strRight;
                second++;
            }
            third++;
        }
        if(first < sf1.length)
        {
            for(int i=first;i<sf1.length;i++)
            {
                sf3[third] = sf1[i];
                third++;
            }
        }
        else if(second < sf2.length)
        {
            for(int i=second;i<sf2.length;i++)
            {
                sf3[third] = sf2[i];
                third++;
            }
        }
        // End Sort-Merge

        String newStr = new String();
        for(int j=0;j<sf3.length-1;j++)
        {
            newStr = newStr+sf3[j]+" ";
        }
        newStr = newStr+sf3[sf3.length - 1];
        if(Constants.DEBUG)
        {
            Constants.CPrintToConsole(this.str+" + "+term.getStr()+" = "+newStr, Constants.DEBUG_SECOND_LEVEL_Information);
        }

        this.str = newStr;
        */

        ArrayList<Term> term_logTerms = term.getLogTerms();
        for(int lt=0;lt<term_logTerms.size();lt++) {
            Term logTerm1 = term_logTerms.get(lt);
            //Term term1 = new Term(logTerm1);
            this.logTerms.add(logTerm1);
        }
    }

    /**
     * Multiplies the term value with argument double value.
     * value of the term is multiplied with the argument double value.
     *
     * It is used in computing the scaled Operator cost. Multiplied by the cost function.
     * @param val - double value to multiply.
     */
    public void multiplyValue(double val)
    {

        this.value = this.value * val;
    }

    /**
     * Adds the term value with argument double value.
     * value of the term is added with the argument double value.
     *
     * It is used in the summing of individual operators scaled cost. If the term.str, logStr are same, then the values can be added.
     * @param val - double value to add.
     */
    public void addValue(double val)
    {
        this.value = this.value + val;
    }

    /**
     * Two terms can be added, if they have same scaling factors and log terms.
     * This function returns the 'str' and 'logTerms' as a key to group similar terms and add them into one term.
     * @return String representation of scale factors and log terms.
     */
    public String getKey()
    {
        // Used in CostFunction HashMap
        return this.str+"::"+getLogStr();
    }

    /**
     * Returns the string representation of the term.
     * We get the string representation as follows:
     * value + str + getLogStr()
     * @return string representation of the term.
     */
    @Override
    public String toString()
    {
        return this.value+" "+this.str+" "+getLogStr();
    }

    /**
     * Returns the log terms in a string representation.
     * Each Term can represented as a sting using toString() function.
     * Log term is written as 'log(term.toString())', which gives the string representation for log term.
     * Here we append the log terms string representation to get a single string representation of log terms.
     * @return string representation of the log terms.
     */
    public String getLogStr()
    {
        String logStr = new String();
        for(int lt=0;lt<logTerms.size();lt++) {
            logStr = logStr+ " log("+logTerms.get(lt) +") ";
        }
        return logStr;
    }
}