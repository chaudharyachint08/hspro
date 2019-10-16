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


package iisc.dsl.codd.db.nonstopsql;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author DeepaliNemade
 */
public class MXTable {

    private int rows;
    private int cols;

    public MXTable(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
        array = new ArrayList[rows];
        for(int i = 0 ; i < rows ; i++){
            array[i] = new ArrayList<String>(cols);
        }
    }

    public int getRows() {
        return rows;
    }

    public int getCols() {
        return cols;
    }
    
    
    
    private ArrayList<String>[] array;
    
    public void importData(List<List<String>> data){
        if(data.size() != rows){
            throw new UnknownError("No of rows do not match with Table column count");
        }
        for(int i =0 ; i < rows ; i++){
            if(data.get(i).size() != cols){
                throw new UnknownError("No oc columns do not match with Table column count");
            }
            array[i].clear();
            array[i].addAll(data.get(i));
        }
    }
    
    public String getCellData(int row , int col){
        return array[row].get(col);
    }
    
    public void setCellData(int row , int col, String data){
        array[row].remove(col);
        array[row].add(col, data);
    }
    public String[] getColumn(int columnIndex){
        String[] retValue = new String[rows];
        for(int i =0 ; i < rows ; i++){
            retValue[i] = "" + array[i].get(columnIndex); 
        }
        return retValue;
    }
    
    public String[] getRow(int rowIndex){
        String[] retValue = new String[cols];
        for(int i =0 ; i < cols ; i++){
            retValue[i] = "" + array[i].toArray();
        }
        return retValue;
    }
    
    private int currentPosition = 0;
    
    public boolean next(){
        if(currentPosition < rows){
            currentPosition++;
            return true;
        }else{
            return false;
        }
    }
    public void reset(){
        currentPosition = 0;
    }

    public int getCurrentPosition() {
        return currentPosition;
    }
    
    
    public int getInt(int columnNumber) throws SQLException{
        if(columnNumber < cols || columnNumber > cols){
            throw new SQLException("Invalid Column Number (" + cols +") : "+columnNumber);
        }
        return Integer.parseInt( getCellData(currentPosition, columnNumber - 1).replace('(', ' ').replace(')', ' ').trim());
    }
    
    public long getLong(int columnNumber) throws SQLException{
        if(columnNumber < cols || columnNumber > cols){
            throw new SQLException("Invalid Column Number (" + cols +") : "+columnNumber);
        }
        return Long.parseLong( getCellData(currentPosition, columnNumber - 1).replace('(', ' ').replace(')', ' ').trim());
    }
    public float getFloat(int columnNumber) throws SQLException{
        if(columnNumber < cols || columnNumber > cols){
            throw new SQLException("Invalid Column Number (" + cols +") : "+columnNumber);
        }
        return Float.parseFloat( getCellData(currentPosition, columnNumber - 1).replace('(', ' ').replace(')', ' ').trim());
    }
    public double getDouble(int columnNumber) throws SQLException{
        if(columnNumber < cols || columnNumber > cols){
            throw new SQLException("Invalid Column Number (" + cols +") : "+columnNumber);
        }
        return Double.parseDouble( getCellData(currentPosition, columnNumber - 1).replace('(', ' ').replace(')', ' ').trim());
    }
    
    public String getString(int columnNumber) throws SQLException{
        if(columnNumber < cols || columnNumber > cols){
            throw new SQLException("Invalid Column Number (" + cols +") : "+columnNumber);
        }
        String temp = getCellData(currentPosition, columnNumber - 1);
        if( temp.startsWith("'") && temp.endsWith("'")){
            temp = temp.substring(1);
            temp = temp.substring(0 , temp.length());
        }
        return temp;
    }
    
    
}
