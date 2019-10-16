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

/*
 * NonStopSQLConstructModeGUI.java
 *
 * Created on Dec 24, 2012, 11:54:26 AM
 */
package iisc.dsl.codd.db.nonstopsql;

import iisc.dsl.codd.client.ConstructMode;
import iisc.dsl.codd.client.ConstructModeFrame;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.RelationStatistics;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.DataType;
import iisc.dsl.codd.ds.Statistics;
import iisc.dsl.codd.graphhistogram.BucketItem;
import iisc.dsl.codd.graphhistogram.GraphHistogram;
import iisc.dsl.codd.client.gui.ExcelAdapter;
import iisc.dsl.codd.client.gui.UIUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.table.DefaultTableModel;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout;
import javax.swing.JComboBox;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingConstants;

import java.awt.Component;

import javax.swing.JLabel;

import java.awt.Font;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.ListSelectionModel;

import java.awt.Color;

/**
 * @author DeepaliNemade
 */
public class NonStopSQLConstructModeGUI extends ConstructModeFrame {

	private static final long serialVersionUID = -5617923240628911949L;
	// Used in the ComboBox
	String emptyString;
	Database database;
	// To avoid handling changing event when relations are being populated
	boolean populatingRels;
	// To avoid handling changing event when columns are being populated
	boolean populatingCols;
	// Number of Frequency Histogram Buckets
	int histRows;
	int numBuckets;
	boolean graph_flag;
	/**
	 * List of Statistics of all relations to be updated.
	 * It has the initial values.
	 * New Values will be overwritten in this.
	 * This will be used in the metadata validation and database updates at the end.
	 * <RelationName, Statistics>
	 */
	HashMap<String, Statistics> stats;
	/**
	 * Maintains the list of statistics to be updated.
	 * Entries are removed as soon as they are updated.
	 * It helps in keep tracking of updated things.
	 * <String, Set> - <Relation,<Columns>>
	 */
	HashMap<String, HashMap<String,Boolean>> updateableStats;
	/**
	 * Keep tracks of update relation statistics.
	 * <Relation, Integer (1/0) updated>
	 */
	HashMap<String, Integer> relUpdated;
	/**
	 * Keep tracks of indexed columns and a single column representative
	 * <String representativeColumn, String colNames>
	 * e.g <L_LINENUMBER, L_LINENUMBER+L_ORDERKEY>
	 * Index Statistics will be enabled for L_LINENUMBER
	 * Outer HashMap is for relation of allStats.
	 */
	HashMap<String, HashMap<String, String>> indexColumns;

	/** Creates new form NonStopSQLConstructModeGUI */
	public NonStopSQLConstructModeGUI(Database database, Statistics[] stats) {
		super("NonStopSQL Construct Mode");
		histRows = 0;
		initComponents();
		setLocationRelativeTo(null);
		emptyString = "";
		this.database = database;
		this.stats = new HashMap<String, Statistics>();
		// Fill updateableStats.
		updateableStats = new HashMap<String, HashMap<String,Boolean>>();
		relUpdated = new HashMap<String, Integer>();
		indexColumns = new HashMap<String, HashMap<String, String>>();
		for (int i = 0; i < stats.length; i++) {
			String relationName = stats[i].getRelationName();
			this.stats.put(relationName, stats[i]);
			relUpdated.put(relationName, 0);
			HashMap<String,Boolean> columns = new HashMap<String, Boolean>();
			try {
				String[] cols = database.getAttributes(relationName);
				String[] multiColumnCols = database.getMultiColumnAttributes(relationName);
				for (int j = 0; j < cols.length; j++) {
					columns.put(cols[j],false);
				}
				for (int j = 0 ; j < multiColumnCols.length; j++){
					columns.put(multiColumnCols[j],false);
				}
				updateableStats.put(relationName, columns);
				//IndexCols
				HashMap<String, String> indexCols = new HashMap<String, String>();
				// Initialize with null, then add representative columns.
				for (int j = 0; j < cols.length; j++) {
					indexCols.put(cols[j], null);
				}
				String colNames[] = stats[i].getIndexColumnsColNames();
				for (int j = 0; j < colNames.length; j++) {
					String key = colNames[j];
					String repColumn;
					if (key.contains("+")) {
						repColumn = new String(key.substring(0, key.indexOf("+")));
					} else {
						repColumn = new String(key);
					}
					// Need clarification:
					// What if there is an index on representative column as well as a set of columns containing representative column??? 
					// Since index statistics are not supported in CODD, this may be useless also.
					indexCols.put(repColumn, key);
				}
				indexColumns.put(relationName, indexCols);
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, "Exception Caught: Could not get columns of relation.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
		}
		populatingRels = false;
		populatingCols = false;
		// Populate Relations
		this.populateRelationsComboBox();
		this.setEnabledRelationLevelMetadata(false);
		this.comboBoxAttribute.setEnabled(false);
		this.setEnabledColumnLevelMetadata(false);
	}

	/**
	 * Populates the set of relations from updateableStats to the relationsCombobox.
	 */
	private void populateRelationsComboBox() {
		populatingRels = true;
		this.comboBoxRelations.removeAllItems();
		this.comboBoxRelations.addItem(emptyString);
		Set<String> set = updateableStats.keySet();
		Iterator<String> iter = set.iterator();
		while (iter.hasNext()) {
			String rel = (String) iter.next();
			this.comboBoxRelations.addItem(rel);
		}
		this.comboBoxRelations.setSelectedItem(emptyString);
		this.comboBoxAttribute.removeAllItems();
		this.comboBoxAttribute.setEnabled(false);
		populatingRels = false;
	}

	/*
	 * Returns the current relation chosen for update
	 */
	private String getCurrentRelation() {
		String currRel = (String) this.comboBoxRelations.getSelectedItem();
		return currRel;
	}

	/*
	 * Returns the current attribute chosen for update
	 */
	private String getCurrentAttribute() {
		String column = (String) this.comboBoxAttribute.getSelectedItem();
		return column;
	}

	/*
	 * Populates the set of columns of the given relation from updateableStats
	 */
	private void populateColumnsComboBox(String relationName) {
		populatingCols = true;
		this.comboBoxAttribute.removeAllItems();
		this.comboBoxAttribute.addItem(emptyString);
		Set<String> set = updateableStats.get(relationName).keySet();
		Iterator<String> iter = set.iterator();
		while (iter.hasNext()) {
			String col = (String) iter.next();
			this.comboBoxAttribute.addItem(col);
		}
		this.comboBoxAttribute.setSelectedItem(emptyString);
		populatingCols = false;
	}

	/**
	 * Relation Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeRelationLevelMetadata(String relationName) {
		if (relationName != null) {
			RelationStatistics relationStatistics = (NonStopSQLRelationStatistics) this.stats.get(relationName).getRelationStatistics();
			this.textCard.setText(relationStatistics.getCardinality() + "");
		} else {
			this.textCard.setText("");
		}
	}

	/**
	 * Relation Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledRelationLevelMetadata(boolean enable) {
		this.textCard.setEnabled(enable);
		this.buttonUpdateRelation.setEnabled(enable);
	}

	/**
	 * Column-Index Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeColumnLevelMetadata(String columnName) {
		try {		
			histRows = 0;
			if (columnName != null) {
				String relationName = this.getCurrentRelation();
				NonStopSQLColumnStatistics columnStatistics = (NonStopSQLColumnStatistics) this.stats.get(relationName).getColumnStatistics(columnName);
				if (columnName.equalsIgnoreCase("SYSKEY")) {
					String highValue = columnStatistics.getHIGH_VALUE();
					String lowValue = columnStatistics.getLOW_VALUE();
					if(highValue!=null && highValue.startsWith("(") && highValue.endsWith(")") && lowValue!=null && lowValue.startsWith("(") && lowValue.endsWith(")")){
						textTotaluec.setText(textCard.getText().trim());
						textLowvalue.setText(columnStatistics.getLOW_VALUE().substring(1,lowValue.length()-1));
						textHighvalue.setText(columnStatistics.getHIGH_VALUE().substring(1,highValue.length()-1));
					}else{
						BigInteger b = new BigInteger(48, new Random());
						textTotaluec.setText(textCard.getText().trim());
						textLowvalue.setText(b.toString()); 
						textHighvalue.setText(b.add(new BigInteger(textCard.getText().trim())).subtract(new BigInteger("2")).toString());
					}
					histRows = Integer.parseInt(textCard.getText().trim());					
					textTotaluec.setEditable(false);
					textLowvalue.setEditable(false);
					textHighvalue.setEditable(false);
				}else{
					textTotaluec.setText(columnStatistics.getTOTAL_UEC() + "");
					textHighvalue.setText(columnStatistics.getHIGH_VALUE() + "");						
					textLowvalue.setText(columnStatistics.getLOW_VALUE() + "");
					textHighvalue.setEditable(true);
					textTotaluec.setEditable(true);
					textLowvalue.setEditable(true);
				}
				NonStopSQLHistObject[] hist = columnStatistics.getNonStopSQLHistogram();
				if (hist != null && hist.length > 0) {
					int count = 3;
					if(!columnName.contains("|")){
						count++;
					}else{
						StringTokenizer stok = new StringTokenizer(columnName, "|");
						while(stok.hasMoreTokens()){
							stok.nextToken();
							count++;
						}
					}
					Object obj[][] = new Object[0][count];
					String identifiers[] = new String[count];
					identifiers[0] = "INTERVAL_NUMBER";
					identifiers[1] = "INTERVAL_ROWCOUNT";
					identifiers[2] = "INTERVAL_UEC";
					if(count>4){
						StringTokenizer stk = new StringTokenizer(columnName, "|");
						for(int k = 3; k < count; k++){
							identifiers[k] = "INTERVAL_BOUNDARY_"+stk.nextToken();
						}	    	
					}else{
						identifiers[3] = "INTERVAL_BOUNDARY";	    	
					}
					tableHist.setModel(new javax.swing.table.DefaultTableModel(obj, identifiers));	    	

					histRows = hist.length;
					String[] data = new String[count];
					for (int r = 0; r < histRows; r++) {
						((DefaultTableModel) tableHist.getModel()).addRow(data);
					}
					for (int i = 0; i < histRows; i++) {
						Integer interval_number = hist[i].getInterval_number();
						BigDecimal interval_rowcount = hist[i].getInterval_rowcount();
						BigDecimal interval_uec = hist[i].getInterval_uec();
						String interval_boundary = hist[i].getInterval_boundary();
						if (interval_number != null) {
							String interval_numberStr = "" + interval_number;
							((DefaultTableModel) tableHist.getModel()).setValueAt(interval_numberStr, i, 0);
							String interval_rowcountStr;
							if(interval_rowcount!=null){
								interval_rowcountStr = "" + interval_rowcount.toBigInteger();								
							}else{
								interval_rowcountStr = "";
							}
							((DefaultTableModel) tableHist.getModel()).setValueAt(interval_rowcountStr, i, 1);
							String interval_uecStr;
							if(interval_uec!=null){
								interval_uecStr = "" + interval_uec.toBigInteger();
							}else{
								interval_uecStr = "";
							}
							((DefaultTableModel) tableHist.getModel()).setValueAt(interval_uecStr, i, 2);

							if(count>4){
								StringTokenizer stk = new StringTokenizer(interval_boundary, "|");
								for(int k = 3; k < count; k++){
									if(stk.hasMoreTokens()){
										((DefaultTableModel) tableHist.getModel()).setValueAt(stk.nextToken(), i, k);
									}
								}	    	
							}else{
								((DefaultTableModel) tableHist.getModel()).setValueAt(interval_boundary, i, 3);	    	
							}
						}
					}
				} else {
					int count = 3;
					if(!columnName.contains("|")){
						count++;
					}else{
						StringTokenizer stok = new StringTokenizer(columnName, "|");
						while(stok.hasMoreTokens()){
							stok.nextToken();
							count++;
						}
					}
					Object obj[][] = new Object[0][count];
					String identifiers[] = new String[count];
					identifiers[0] = "INTERVAL_NUMBER";
					identifiers[1] = "INTERVAL_ROWCOUNT";
					identifiers[2] = "INTERVAL_UEC";
					if(count>4){
						StringTokenizer stk = new StringTokenizer(columnName, "|");
						for(int k = 3; k < count; k++){
							identifiers[k] = "INTERVAL_BOUNDARY_"+stk.nextToken();
						}	    	
					}else{
						identifiers[3] = "INTERVAL_BOUNDARY";	    	
					}
					tableHist.setModel(new javax.swing.table.DefaultTableModel(obj, identifiers));	    	
				}
			} else {
				//Column level
				this.textTotaluec.setText("");
				this.textHighvalue.setText("");
				this.textLowvalue.setText("");
				Object obj[][] = new Object[0][4];
				String identifiers[] = new String[4];
				identifiers[0] = "INTERVAL_NUMBER";
				identifiers[1] = "INTERVAL_ROWCOUNT";
				identifiers[2] = "INTERVAL_UEC";
				identifiers[3] = "INTERVAL_BOUNDARY";
				tableHist.setModel(new javax.swing.table.DefaultTableModel(obj, identifiers));
			} 
			textNumBuckets.setText(histRows + "");
		}catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "Exception Caught: " + e.getMessage(), "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}
	}

	/**
	 * Column-Index Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledColumnLevelMetadata(boolean enable) {
		if(enable){
			UIUtils.enableTextField(textTotaluec ,textLowvalue, textHighvalue , textHistWrite, textNumBuckets );			
		}else{
			UIUtils.disableTextField(textTotaluec ,textLowvalue, textHighvalue , textHistWrite, textNumBuckets );
		}
		this.buttonCreateBuckets.setEnabled(enable);
		this.tableHist.setEnabled(enable);
		this.buttonHistUpload.setEnabled(enable);
		this.buttonUpdateColumn.setEnabled(enable);
		this.buttonResetColValues.setEnabled(enable);
		this.buttonQuantHistShowGraphActionPerformed.setEnabled(enable);			
	}

	/**
	 * Read the current relation stats and validate with appropriate error message.
	 * If validation is success, update values into stats.
	 * @return
	 */
	private boolean validateCurrentRelationStats() {
		boolean valid = false;
		BigInteger card;
		String relationName = this.getCurrentRelation();
		/**
		 * Node : Card (1)
		 * Structural Constraints : >= 0
		 * Consistency Constraints : NIL
		 */
		if(this.textCard.getText() == null || this.textCard.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Cardinality cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			card = new BigInteger(this.textCard.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Cardinality is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		if(card.compareTo(new BigInteger(Long.MAX_VALUE+""))>0){
			valid = false;
			JOptionPane.showMessageDialog(null, "Maximum Relation Cardinality allowed is: " + Long.MAX_VALUE + ". Please enter any positive integer value less than this value.");
			return valid;
		} else if (card.compareTo(BigInteger.ZERO) > 0) {
			valid = true;
		} else if (card.compareTo(BigInteger.ZERO) == 0) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Set the Relation Cardinality to any positive integer value, Otherwise the statistics for the selected relation will remain unchanged.");
			return valid;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Cardinality value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		// valid is true at this point
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */
		NonStopSQLRelationStatistics nsRelStat = (NonStopSQLRelationStatistics) stats.get(relationName).getRelationStatistics();
		nsRelStat.setCardinality(new BigDecimal(card));
		return valid; 
	}

	private void comboBoxRelationsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboBoxRelationsActionPerformed
		if (populatingRels) {
			return;
		}
		String rel = this.getCurrentRelation();
		if (rel!=null && !rel.trim().isEmpty()) {
			this.InitializeRelationLevelMetadata(rel);
			if (this.relUpdated.get(rel) == 0) {
				// Relation statistics are not updated yet.
				// ICARE 12 Sir comment. Only after constructing/completing PK relation one can go to FK relation.
				boolean noDependent = true;
				String depRel = "";
				try {
					String[] FKcols = database.getForeignKeyAttributes(rel);
					if (FKcols != null && FKcols.length > 0) {
						Map<String, String> fkRel = database.getFKColumnRefRelation(rel);
						if (fkRel != null) {
							Iterator<String> iterator = fkRel.keySet().iterator();
							while (iterator.hasNext()) {
								String PKRelation = fkRel.get(iterator.next());
								if(relUpdated.containsKey(PKRelation) && relUpdated.get(PKRelation)==0){
									noDependent = false;
									if (!depRel.contains(PKRelation)) {
										depRel = depRel + PKRelation + "  ";
									}									
								}
							}
						}
					}
				} catch (Exception e) {
					Constants.CPrintErrToConsole(e);
					JOptionPane.showMessageDialog(null, e.getMessage() , "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				}
				if (noDependent) {
					this.setEnabledRelationLevelMetadata(true);
				} else {
					this.comboBoxAttribute.removeAllItems();
					this.setEnabledRelationLevelMetadata(false);
					this.InitializeRelationLevelMetadata(null);
					JOptionPane.showMessageDialog(null, "The relation chosen is a foreign key relation. It can not be constructed untill the PK relation(s) " + depRel + " is/are constructed.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
				}
				this.comboBoxAttribute.setEnabled(false);
			} else {
				// Relation statistics are updated already.
				this.comboBoxAttribute.setEnabled(true);
			}
			this.populateColumnsComboBox(rel);
			this.setEnabledColumnLevelMetadata(false);
			this.InitializeColumnLevelMetadata(null); // clear column level metadata
		} else {
			this.comboBoxAttribute.removeAllItems();
			this.comboBoxAttribute.setEnabled(false);
			this.setEnabledRelationLevelMetadata(false);
			this.setEnabledColumnLevelMetadata(false);
			this.InitializeRelationLevelMetadata(null);
			this.InitializeColumnLevelMetadata(null);
		}
	}//GEN-LAST:event_comboBoxRelationsActionPerformed

	private void buttonUpdateRelationActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonUpdateRelationActionPerformed
		if (validateCurrentRelationStats()) {
			String rel = this.getCurrentRelation();
			this.relUpdated.put(rel, 1);
			this.setEnabledRelationLevelMetadata(false);
			this.comboBoxAttribute.setEnabled(true);
		}
	}//GEN-LAST:event_buttonUpdateRelationActionPerformed

	private void comboBoxAttributeActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboBoxAttributeActionPerformed
		if (populatingCols) {
			return;
		}
		String col = this.getCurrentAttribute();
		if (col==null || col.trim().isEmpty()) {
			this.InitializeColumnLevelMetadata(null);
			this.setEnabledColumnLevelMetadata(false);
			return;
		}

		/**
		 * Put Logic Here:
		 * For a multicolumn histogram if its corresponding columns single column histograms are not created then 
		 * ask first to create all corresponding single column histograms
		 */
		StringTokenizer stok = new StringTokenizer(col, "|");
		if(stok.countTokens() > 1) {
			while(stok.hasMoreTokens()){
				String tokenstr = stok.nextToken("|");
				if(((NonStopSQLColumnStatistics)stats.get(this.getCurrentRelation()).getColumnStatistics(tokenstr)).getTOTAL_UEC().compareTo(BigDecimal.valueOf(-1)) == 0) {
					this.InitializeColumnLevelMetadata(null);
					this.setEnabledColumnLevelMetadata(false);
					JOptionPane.showMessageDialog(null, "First create statistics for corresponding single column histograms", "CODD - Exception",JOptionPane.ERROR_MESSAGE);
					return;
				}
			}
		}
		String relationName = this.getCurrentRelation();
		NonStopSQLColumnStatistics columnStatistics = (NonStopSQLColumnStatistics) this.stats.get(relationName).getColumnStatistics(col);
		if(columnStatistics==null){
			JOptionPane.showMessageDialog(null, "Exception in getting the column statistics. The reason behind this exception may be that you have not intitialized the statistics yet.", "CODD - Exception",JOptionPane.ERROR_MESSAGE);
			return;
		}
		Constraint constraint = columnStatistics.getConstraint();
		try {
			String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, col);
			String PKRelation = temp[0];
			String PKColumn = temp[1];
			// If col is FK and the corresponding PK column is chosen (PK realtion is chosen from SelectFrame window) to construct, but not constructed yet, report to user.
			if (constraint.isFK() && updateableStats.containsKey(PKRelation) && updateableStats.get(PKRelation).get(PKColumn)==false) {
				JOptionPane.showMessageDialog(null, "The column chosen is a foriegn key. It can not be constructed until the PK column " + PKColumn + " of " + PKRelation + " is updated.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
				this.comboBoxAttribute.setSelectedItem(this.emptyString);
				this.setEnabledColumnLevelMetadata(false);
				this.InitializeColumnLevelMetadata(null);
			} else {
				this.setEnabledColumnLevelMetadata(true);
				this.InitializeColumnLevelMetadata(col);
				if(col.contains("|")) { 
					buttonQuantHistShowGraphActionPerformed.setEnabled(false);
					this.buttonCreateBuckets.setEnabled(false);
					this.tableHist.setEnabled(false);
					UIUtils.disableTextField(textNumBuckets );
				}
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_comboBoxAttributeActionPerformed

	private void buttonCreateBucketsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonCreateBucketsActionPerformed

		Object[] data = null ;
		try {
			if(textNumBuckets.getText().trim().isEmpty()){
				JOptionPane.showMessageDialog(null, "CODD Exception: Enter the number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				return;
			}
			int bucketSize;
			try{
				bucketSize = Integer.parseInt(textNumBuckets.getText().trim());
			}catch(Exception e){
				JOptionPane.showMessageDialog(null, "CODD Exception: Enter the integer value in number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				return;
			}
			this.histRows = tableHist.getRowCount();        
			if (bucketSize >= 0 && bucketSize <= NonStopSQLColumnStatistics.MAX_BUCKET_SIZE) {
				if (this.histRows > bucketSize) {
					// Remove the last entries
					int removeCnt = this.histRows - bucketSize;
					for (int i = 0; i < removeCnt; i++) {
						// remove 0th row, removeCnt times.
						((DefaultTableModel) tableHist.getModel()).removeRow(this.histRows - i - 1);
					}
					this.histRows = bucketSize;
				} else {
					int addCnt = bucketSize - this.histRows;
					this.histRows = bucketSize;
					for (int i = 0; i < addCnt; i++) {
						((DefaultTableModel) tableHist.getModel()).addRow(data);
					}
				}
				try{
					if(comboBoxAttribute.getSelectedItem().toString().equalsIgnoreCase("SYSKEY")){
						BigDecimal boundary = new BigDecimal(textLowvalue.getText().trim());
						for (int i = 0; i < this.histRows; i++) {
							tableHist.getModel().setValueAt(i, i, 0);
							tableHist.getModel().setValueAt(1, i, 1);
							tableHist.getModel().setValueAt(1, i, 2);
							tableHist.getModel().setValueAt(boundary, i, 3);
							boundary = boundary.add(BigDecimal.ONE);
						}
					}else{
						for (int i = 0; i < this.histRows; i++) {
							tableHist.getModel().setValueAt((i+1), i, 0);
						}
					}
				}catch(Exception e){
					Constants.CPrintErrToConsole(e);
					JOptionPane.showMessageDialog(null, "CODD Exception: "+ e.getMessage() , "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				}
				tableHist.setVisible(true);
				tableHist.getColumnModel().setColumnSelectionAllowed(true);
			} else {
				JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, " + NonStopSQLColumnStatistics.MAX_BUCKET_SIZE + "].", "CODD - Error", 0);
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: Exception in creating buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_buttonCreateBucketsActionPerformed

	private void buttonHistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonHistUploadActionPerformed
		String filePath;
		try {
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"NonStopSql";
			File path = new File(pathName);
			if(!path.exists()) {
				path.mkdir();
			}
			JFileChooser filechooser = new JFileChooser();
			filechooser.setCurrentDirectory(path);
			int returnvalue = filechooser.showOpenDialog(null);
			if (returnvalue == JFileChooser.APPROVE_OPTION) {
				File selectedFile = filechooser.getSelectedFile();
				filePath = selectedFile.getAbsolutePath();
				textHistWrite.setText(filePath);
				Constants.CPrintToConsole(filePath, Constants.DEBUG_SECOND_LEVEL_Information);
				File myfile = new File(filePath);
				FileReader fr = new FileReader(myfile);
				BufferedReader br = new BufferedReader(fr);
				int columnCount = tableHist.getModel().getColumnCount();
				while(tableHist.getModel().getRowCount()>0){
					((DefaultTableModel)tableHist.getModel()).removeRow(0);
				}
				String line = br.readLine();
				if(line!=null){
					String[] tokens = line.trim().split("\\|\\$\\|");
					if(tokens.length>=2){
						this.textTotaluec.setText(tokens[1]);													
					}
					if(tokens.length>=3){
						this.textLowvalue.setText(tokens[2]);													
					}
					if(tokens.length>=4){
						this.textHighvalue.setText(tokens[3]);		
					}
					line = br.readLine();
				}
				int i = 0;
				while (line != null) {
					((DefaultTableModel)tableHist.getModel()).addRow(new String[columnCount]);
					StringTokenizer st = new StringTokenizer(line, "|");
					int length = st.countTokens();
					for(int k = 0; k < length && k < columnCount; k++) {
						tableHist.getModel().setValueAt(st.nextElement(), i, k);
					}
					Constants.CPrintToConsole(line, Constants.DEBUG_FIRST_LEVEL_Information);
					i++;
					line = br.readLine();
				}
				this.histRows = i;
				this.textNumBuckets.setText(i+"");					
				if(br!=null){
					br.close();											
				}
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: "+ e.getMessage() , "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_buttonHistUploadActionPerformed

	private void buttonResetColValuesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonResetColValuesActionPerformed
		String col = this.getCurrentAttribute();
		this.InitializeColumnLevelMetadata(col);
	}//GEN-LAST:event_buttonResetColValuesActionPerformed

	private void backButtonActionPerformed(java.awt.event.ActionEvent evt) {
		String[] relations = new String[relUpdated.size()];
		int i = 0;
		for(String rel : relUpdated.keySet()){
			relations[i] = rel;
			i++;
		}
		new ConstructMode(DBConstants.NONSTOPSQL , relations, database).setVisible(true);
		this.dispose();				
	}

	private void buttonQuantHistShowGraphActionPerformedActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistShowGraphActionPerformedActionPerformed
		if(!validateCurrentColumnStats()){
			//validateCurrentColumnStats() will display it's own error message
			return;
		}
		boolean db2Hist = true;   // It should be false. Bcoz it is not a db2
		boolean unique = false;
		boolean noDistinct = false;
		int count = this.tableHist.getModel().getRowCount();
		Constants.CPrintToConsole("Count: " + count, Constants.DEBUG_SECOND_LEVEL_Information);
		String relationName = this.getCurrentRelation();
		String colName = this.getCurrentAttribute();
		long[] freq = new long[count];
		long[] dist = new long[count];
		String[] labels = new String[count];
		String type = null;
		try {
			type = database.getType(relationName, colName);
		} catch (Exception e) {
			JOptionPane.showMessageDialog(null, e.getMessage(), "CODD - Exception",JOptionPane.ERROR_MESSAGE);
			return;
		}
		DataType minDT;
		DataType maxDT;
		DataType minWidth;
		if (DataType.isDouble(type)) {
			minDT = new DataType(DataType.DOUBLE, this.textLowvalue.getText().trim());
			maxDT = new DataType(DataType.DOUBLE, this.textHighvalue.getText().trim());
			minWidth = new DataType(DataType.DOUBLE, "1");
		} else if (DataType.isInteger(type)) {
			minDT = new DataType(DataType.INTEGER, this.textLowvalue.getText().trim());
			maxDT = new DataType(DataType.INTEGER, this.textHighvalue.getText().trim());
			minWidth = new DataType(DataType.INTEGER, "1");
		} else if (DataType.isNumeric(type)) {
			minDT = new DataType(DataType.NUMERIC, this.textLowvalue.getText().trim());
			maxDT = new DataType(DataType.NUMERIC, this.textHighvalue.getText().trim());
			minWidth = new DataType(DataType.NUMERIC, "1");
		} else if (DataType.isBigDecimal(type)) {
			minDT = new DataType(DataType.SIGNED_LARGEINT, this.textLowvalue.getText().trim());
			maxDT = new DataType(DataType.SIGNED_LARGEINT, this.textHighvalue.getText().trim());
			minWidth = new DataType(DataType.SIGNED_LARGEINT, "1");
		} else {
			minDT = new DataType(DataType.VARCHAR, "" + this.textLowvalue.getText().trim());
			maxDT = new DataType(DataType.VARCHAR, "" + this.textHighvalue.getText().trim());
			minWidth = null;
		}
		Constants.CPrintToConsole("(minDT , maxDT , minWidth) : : (" + minDT + " , " + maxDT + " , " + minWidth + ")", Constants.DEBUG_SECOND_LEVEL_Information);


		if (count > 0) {
			/**
			 * Read all the information from the table.
			 */
			for(int i=0;i<count;i++){
				String interval_RC = this.tableHist.getModel().getValueAt(i, 1).toString();			//INTERVAL_ROWCOUNT - VALCOUNT
				String interval_UEC = this.tableHist.getModel().getValueAt(i, 2).toString();		//INTERVAL_UEC - DISTCOUNT
				String boundary = this.tableHist.getModel().getValueAt(i, 3).toString();			//INTERVAL_BOUNDARY - COLVALUE
				freq[i] = Long.parseLong(interval_RC);
				dist[i] = Long.parseLong(interval_UEC);
				labels[i] = boundary;
				Constants.CPrintToConsole("Bucket " + i + ": valCount:" + freq[i] + "  Distinct:" + dist[i], Constants.DEBUG_SECOND_LEVEL_Information);
			}
			long totalFreqCount = 0;
			long totalDistCount = 0;

			for (int i = 0; i < labels.length; i++) {
				totalFreqCount = totalFreqCount + freq[i];
				totalDistCount = totalDistCount + dist[i];
			}

			ArrayList<BucketItem> buckets = new ArrayList<BucketItem>();
			double totalFreqPercent = 0;
			double totalDCPercent = 0;
			for (int i = 0; i < labels.length; i++) {
				double freqPercent = (freq[i] * 100.0) / totalFreqCount;
				DecimalFormat decimalFormat = new DecimalFormat("#.##");
				freqPercent = Double.valueOf(decimalFormat.format(freqPercent));
				totalFreqPercent = totalFreqPercent + freqPercent;

				double distinctCountPercent = (dist[i] * 100.0) / totalDistCount;
				decimalFormat = new DecimalFormat("#.##");
				distinctCountPercent = Double.valueOf(decimalFormat.format(distinctCountPercent));
				totalDCPercent = totalDCPercent + distinctCountPercent;

				if (i == labels.length - 1) { // adjust to 100.00
					double remainder = 100.00 - totalFreqPercent;
					freqPercent = freqPercent + remainder;
					freqPercent = Double.valueOf(decimalFormat.format(freqPercent));
					totalFreqPercent = totalFreqPercent + remainder;

					remainder = 100.00 - totalDCPercent;
					distinctCountPercent = distinctCountPercent + remainder;
					distinctCountPercent = Double.valueOf(decimalFormat.format(distinctCountPercent));
					totalDCPercent = totalDCPercent + remainder;

				}

				DataType val, prevVal;
				if (DataType.isDouble(type)) {
					val = new DataType(DataType.DOUBLE, "" + labels[i]);
					if (i == 0) {
						prevVal = minDT;
					} else {
						prevVal = new DataType(DataType.DOUBLE, "" + labels[i - 1]);
					}
				} else if (DataType.isInteger(type)) {
					val = new DataType(DataType.INTEGER, "" + labels[i]);
					if (i == 0) {
						prevVal = minDT;
					} else {
						prevVal = new DataType(DataType.INTEGER, "" + labels[i - 1]);
					}
				} else if (DataType.isNumeric(type)) {
					val = new DataType(DataType.NUMERIC, "" + labels[i]);
					if (i == 0) {
						prevVal = minDT;
					} else {
						prevVal = new DataType(DataType.NUMERIC, "" + labels[i - 1]);
					}
				} else if (DataType.isBigDecimal(type)) {
					val = new DataType(DataType.SIGNED_LARGEINT, "" + labels[i]);
					if (i == 0) {
						prevVal = minDT;
					} else {
						prevVal = new DataType(DataType.SIGNED_LARGEINT, "" + labels[i - 1]);
					}
				} else {
					val = new DataType(DataType.VARCHAR, "" + labels[i]);
					if (i == 0) {
						prevVal = minDT;
					} else {
						prevVal = new DataType(DataType.VARCHAR, "" + labels[i - 1]);
					}
				}
				Constants.CPrintToConsole("(" + prevVal.getString() + ", " + val.getString() + " ):: " + freq[i] + " (" + freqPercent + ")", Constants.DEBUG_SECOND_LEVEL_Information);
				BucketItem bucket = new BucketItem(prevVal, val, freq[i], dist[i], freqPercent, distinctCountPercent);
				buckets.add(bucket);
			}
			double minHeight = 1.0; // 1%
			// 1.0 * (totalFreqCount / 100.0); // 1% of totalRows

			try {
				String[] PKKeys = database.getPrimaryKeyAttributes(relationName);
				int pk = 0;
				while (PKKeys != null && pk < PKKeys.length) {
					String temp = PKKeys[pk];
					Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
					if (temp.equalsIgnoreCase(colName)) {
						unique = true;
					}
					pk++;
				}

			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				return;
			}
			if (DataType.isDouble(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.DOUBLE, db2Hist, unique, noDistinct).setVisible(true);
			} else if (DataType.isInteger(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.INTEGER, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isNumeric(type)){
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.NUMERIC, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isBigDecimal(type)){
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.SIGNED_LARGEINT, db2Hist, unique, noDistinct).setVisible(true);
			} else {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHOUTBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.VARCHAR, db2Hist, unique, noDistinct).setVisible(true);
			}
			//new histogramGUI(this,labels,data,2,max).setVisible(true);
		} else{
			double minHeight = 1.0; 
			try {
				String[] PKKeys = database.getPrimaryKeyAttributes(relationName);
				int pk = 0;
				while (PKKeys != null && pk < PKKeys.length) {
					String temp = PKKeys[pk];
					Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
					if (temp.equalsIgnoreCase(colName)) {
						unique = true;
					}
					pk++;
				}
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				return;
			}
			long totalFreqCount = Long.parseLong(this.textCard.getText().trim());
			long totalDistCount = Long.parseLong(this.textTotaluec.getText().trim());
			ArrayList<BucketItem> buckets = new ArrayList<BucketItem>(); 
			BucketItem bucket = new BucketItem(minDT, maxDT, totalFreqCount, totalDistCount, 100.0, 100.0);
			buckets.add(bucket);
			if (DataType.isDouble(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.DOUBLE, db2Hist, unique, noDistinct).setVisible(true);
			} else if (DataType.isInteger(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.INTEGER, db2Hist, unique, noDistinct).setVisible(true);
			} else if (DataType.isNumeric(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.NUMERIC, db2Hist, unique, noDistinct).setVisible(true);
			} else if (DataType.isBigDecimal(type)) {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.SIGNED_LARGEINT, db2Hist, unique, noDistinct).setVisible(true);
			} else {
				new GraphHistogram((ConstructModeFrame)this, GraphHistogram.FREQUENCY_MODE_WITHOUTBB, totalFreqCount, totalDistCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.VARCHAR, db2Hist, unique, noDistinct).setVisible(true);
			}
		}
	}//GEN-LAST:event_buttonQuantHistShowGraphActionPerformedActionPerformed

	private void buttonUpdateColumnActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonUpdateColumnActionPerformed
		if (validateCurrentColumnStats()) {
			try {
				//Write the histogram information to an external file.
				String name = this.getCurrentRelation() + "_" + this.getCurrentAttribute();
				StringBuffer sb = new StringBuffer();
				String temp = this.textCard.getText().trim() + "|$|" + this.textTotaluec.getText().trim() + "|$|" + this.textLowvalue.getText().trim() + "|$|" + this.textHighvalue.getText().trim() + "\n";
				sb.append(temp);
				int colcount = this.tableHist.getModel().getColumnCount();
				for (int j = 0; j < this.tableHist.getModel().getRowCount(); j++) {
					String interval_number = "";				//INTERVAL_ROWCOUNT - VALCOUNT
					if(this.tableHist.getModel().getValueAt(j, 0)!=null){
						interval_number=this.tableHist.getModel().getValueAt(j, 0).toString().trim();
					}
					String interval_rowcount = "";			//INTERVAL_BOUNDARY - COLVALUE
					if(this.tableHist.getModel().getValueAt(j, 1)!=null){
						interval_rowcount=this.tableHist.getModel().getValueAt(j, 1).toString().trim();
					}
					String interval_uec = "";					//INTERVAL_UEC - DISTCOUNT
					if(this.tableHist.getModel().getValueAt(j, 2)!=null){
						interval_uec=this.tableHist.getModel().getValueAt(j, 2).toString().trim();
					}																	  // For each column
					String interval_boundary = "";
					for(int k = 3; k < colcount; k++){
						String tempStr = "";					//INTERVAL_BOUNDARY - COLVALUE
						if(this.tableHist.getModel().getValueAt(j, k)!=null){
							tempStr=this.tableHist.getModel().getValueAt(j, k).toString().trim();
						}	
						interval_boundary = interval_boundary + "|" + tempStr;
					}
					temp = interval_number + "|" + interval_rowcount + "|" + interval_uec + interval_boundary + "\n";
					sb.append(temp);
				}
				String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"NonStopSql";
				File path = new File(pathName);
				if(!path.exists()) {
					path.mkdir();
				}
				String filePath = pathName + Constants.PathSeparator + name.replace("|", "_") + ".hist" ;
				File file = new File(filePath);
				if(!file.exists()) 
				{ 
					try 
					{ 
						// Try creating the file 
						file.createNewFile(); 
					} 
					catch(IOException ioe) 
					{ 
						Constants.CPrintErrToConsole(ioe); 
					} 
				}
				FileWriter writer = new FileWriter(file, false);  // false for not appending in file
				writer.write(sb.toString());
				writer.flush();
				writer.close();
				textHistWrite.setEnabled(false);
				textHistWrite.setText("");
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
			}
			JOptionPane.showMessageDialog(null, "Column Statistics Updated.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);

			// Set the attribute as updated.
			String relationName = this.getCurrentRelation();
			String columnName = this.getCurrentAttribute();
			this.updateableStats.get(relationName).put(columnName, true);	
		}
	}//GEN-LAST:event_buttonUpdateColumnActionPerformed

	/**
	 * Read the current column stats and validate with appropriate error message.
	 * If validation is success, update values into stats.
	 * @return
	 */
	private boolean validateCurrentColumnStats() {
		boolean valid = false;
		String relationName = this.getCurrentRelation();
		String colName = this.getCurrentAttribute();
		Statistics stat = stats.get(relationName);
		NonStopSQLRelationStatistics nsRelStat = (NonStopSQLRelationStatistics) stat.getRelationStatistics();
		NonStopSQLColumnStatistics nsColStat = (NonStopSQLColumnStatistics) stat.getColumnStatistics(colName);
		Constraint constraint = nsColStat.getConstraint();
		/*
		 * Tokenize the column names, datatype, constraints   -- Added By Deepali
		 */
		StringTokenizer st = new StringTokenizer(colName, "|");
		String cols[] = new String[st.countTokens()]; 
		int k = 0;
		while(st.hasMoreTokens()) {
			cols[k++] = st.nextToken();
		}
		StringTokenizer stc = new StringTokenizer(constraint.toString(), "|");
		Constraint constraints[] = new Constraint[stc.countTokens()]; 
		int j = 0;
		while(stc.hasMoreTokens()) {
			constraints[j++] = new Constraint(stc.nextToken());
		}
		BigDecimal card = nsRelStat.getCardinality();
		BigDecimal col_card = BigDecimal.ZERO;
		BigDecimal total_uec = BigDecimal.ZERO;
		DataType high_Value = null, low_Value = null;
		Statistics PKStat;
		NonStopSQLColumnStatistics nsPKColStat = null;
		String[] colVal = new String[tableHist.getRowCount()];
		int count = 0;
		// Histogram level statistics
		NonStopSQLHistObject[] histogram = null;
		Integer[] interval_number = null;
		String[] interval_boundary = null;
		BigDecimal[] interval_rowcount = null;
		BigDecimal[] interval_uec = null;
		BigDecimal sumValCounts = BigDecimal.ZERO;
		BigDecimal sumtotaluec = BigDecimal.ZERO;

		// For Each Column of Multi Column Attribute, Check all constraints
		for(k = 0; k < cols.length; k++){
			PKStat = null;
			nsPKColStat = null;
			try {
				String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, cols[k]);
				String PKRelation = temp[0];
				String PKColumn = temp[1];
				// If column is FK, retrieve the PK information if chosen to construct
				if (PKRelation != null && PKColumn != null) {
					if (constraints[k].isFK() && stats.containsKey(PKRelation)) {
						PKStat = stats.get(PKRelation);
						nsPKColStat = (NonStopSQLColumnStatistics) PKStat.getColumnStatistics(PKColumn);
					}
				}
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				return false;
			}

			/*
			 * Node :  ROWCOUNT (4)
			 * Structural Constraints: >=0
			 * Consistency Constraints: (2) ColCard <= Card		// Condition for this is not needed since both are coming form same text field.
			 * Attribute Constraints Checks: ColCard = Card, if Unique // Condition for this is not needed since both are coming form same text field.
			 */
			if(textCard.getText() == null || textCard.getText().trim().equals("")){
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Relation Cardinality can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid; 
			}
			try {
				col_card = new BigDecimal(new BigInteger(textCard.getText().trim()));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Relation Cardinality is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (col_card.compareTo(BigDecimal.ZERO) > 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid value in the field Relation Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/*
			 * Node :  TOTAL_UEC (4)
			 * Structural Constraints: >=0
			 * Consistency Constraints: (2) TOTAL_UEC <= ColCard
			 * Attribute Constraints Checks: TOTAL_UEC = ColCard, if Unique
			 * Inter table Consistency Constraints: TOTAL_UEC <= PK.ColCard
			 */
			if(textTotaluec.getText() == null || textTotaluec.getText().trim().equals("")){
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Total UEC cannnot be null.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			try {
				total_uec = new BigDecimal(new BigInteger(textTotaluec.getText().trim()));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Total UEC is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (total_uec.compareTo(BigDecimal.ZERO) > 0) {
				if (total_uec.compareTo(col_card) <= 0){
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Total UEC must be less than or equal to the Cardinality of the column.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				if(cols.length == 1){   //Check only for single column histograms
					if (constraints[k].isUnique()) {
						if (total_uec.compareTo(col_card) == 0) {  
							valid = true;
						} else {
							valid = false;
							JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: Total UEC must be equal to the Cardinality of the column, since the column has a UNIQUE constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
							return valid;
						}
					}

					/*
					 *  TODO Uncomment the following code, once we have the correct query in the method
					 *  getPrimaryKeyRelationAndColumn of CODD_ServerDatabase.java which returns only single
					 *  result for the primary key attribute corresponding to the foreign key attribute
					 *  Done by Ankur Gupta.
					 *  
					 *  Uncommented by Suresh; It should work now !
					 */
					
					if (constraints[k].isFK() && nsPKColStat != null) {
						if (total_uec.compareTo(nsPKColStat.getColCard()) <= 0) {
							valid = true;
						} else {
							valid = false;
							JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: Total UEC must be lesser than or equal to the Column Cardinality of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
							return valid;
						}
					}
					 
				}
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid value in the field Total UEC.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/*
			 * Node : LowValue(8), HighValue(9)
			 * Structural Constraints: Data type should be same as column data type / the value could be empty.
			 * Consistency Constraints: (10) HighValue > LowValue if total_uec >1
			 * Inter table Consistency Constraints: HighValue <= PK.HighValue and LowValue >= PK.LowValue
			 */ 
			String type = nsColStat.getColumnType();
			// type will contain "|" separated values. Take the token corresponding to current column
			String[] stoken = type.split("\\|");

			if(stoken==null || stoken.length<k+1){
				valid = false;
				JOptionPane.showMessageDialog(null, "Could not find the data type of " + (k+1) + "th subattribute." , "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;				
			}else{
				type = stoken[k];				
			}

			if(textLowvalue.getText() == null || textLowvalue.getText().trim().equals("")){
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: LOW_VALUE can not be null.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(textHighvalue.getText() == null || textHighvalue.getText().trim().equals("")){
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: HIGH_VALUE can not be null.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			// Assume that high value and low value in text boxes are pipe separated in case of mch
			// Trim removes both leading and trailing whitespaces.
			// In case of string data types, leading spaces determine whether it
			// is a lowvalue. Hence, we remove only the trailing whitespace
			 
			String str_highValue = null;
			String str_lowValue = null;
			if(DataType.isString(type)){
				str_highValue = textHighvalue.getText().replaceFirst("\\s+$", "");
				str_lowValue = textLowvalue.getText().replaceFirst("\\s+$", "");
			} else{
				str_highValue = textHighvalue.getText().trim();
				str_lowValue = textLowvalue.getText().trim();
			}

			String[] strLowVal = str_lowValue.split("\\|");
			String[] strHighVal = str_highValue.split("\\|");

			/*
			 *  Check if user has entered n "|" separated values in low values and high values,
			 *  where n is equal to number of columns for which statistics are to be created
			 */
			if(strLowVal.length != cols.length) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Not sufficient number of entries in LOW_VALUE.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(strHighVal.length != cols.length) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Not sufficient number of entries in HIGH_VALUE.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			if(DataType.isString(type)){
				str_highValue = strHighVal[k].replaceFirst("\\s+$", "");
				str_lowValue = strLowVal[k].replaceFirst("\\s+$", "");
			} else{
				str_highValue = strHighVal[k].trim();
				str_lowValue = strLowVal[k].trim();
			}			

			/*
			 *  the high value of column in MCH should be same as high value of its individual column;
			 *  Same logic for low value as well
			 */
			if(cols.length > 1){ 
				String lowval = ((NonStopSQLColumnStatistics)stats.get(relationName).getColumnStatistics(cols[k])).getLOW_VALUE();
				if(!str_lowValue.equals(lowval)){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Values present in Low_Value of " + (k+1) + "th subattribute is not equal to Low_Values of their corresponding individual columns .", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				String highval = ((NonStopSQLColumnStatistics)stats.get(relationName).getColumnStatistics(cols[k])).getHIGH_VALUE();
				if(!str_highValue.equals(highval)){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Values present in High_Value of " + (k+1) + "th subattribute is not equal to High_Values of their corresponding individual columns .", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			try{
				low_Value = new DataType(type, str_lowValue);
			} catch (Exception e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Low_Value is not of Column Data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			try{
				high_Value = new DataType(type, str_highValue);
			} catch (Exception e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in High_Value is not of Column Data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (total_uec.compareTo(BigDecimal.ONE) > 0){
				if (high_Value.compare(low_Value) > 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Low_Value must be lesser than the High_Value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				} 
			} else if (total_uec.compareTo(BigDecimal.ONE) == 0){
				if (high_Value.compare(low_Value) == 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Low_Value must be equal to the High_Value, since TOTAL_UEC is one.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				} 
			}
			/*
			 *  TODO Uncomment the following code, once we have the correct query in the method
			 *  getPrimaryKeyRelationAndColumn of CODD_ServerDatabase.java which returns only single
			 *  result for the primary key attribute corresponding to the foreign key attribute
			 *  Done by Ankur Gupta.
			 *  
			 *  Uncommented by Suresh; It should work now !
			 */
			
		if (constraints[k].isFK() && nsPKColStat != null) {
			DataType pk_HighValue = new DataType(type, nsPKColStat.getHIGH_VALUE());
			if (pk_HighValue.getString().length() > 0 && high_Value.compare(pk_HighValue) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: High_Value must be lesser than or equal to the High_Value (" + pk_HighValue.getString() + ") of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			DataType pk_LowValue = new DataType(type, nsPKColStat.getLOW_VALUE());
			if (pk_LowValue.getString().length() > 0 && low_Value.compare(pk_LowValue) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: Low_Value must be greater than or equal to the Low_Value (" + pk_LowValue.getString() + ") of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}
			 
			 // Ashoke - TODO 
			if(cols.length==1 && (DataType.isInteger(type) || DataType.isBigDecimal(type))){
				BigDecimal tempHighValue = new BigDecimal(new BigInteger(str_highValue));
				BigDecimal tempLowValue = new BigDecimal(new BigInteger(str_lowValue));
				if(tempHighValue.subtract(tempLowValue).add(BigDecimal.ONE).compareTo(total_uec) < 0){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - For Integer type column, (MaxValue - MinValue) should be greater than or equal to Total_UEC.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				if(DataType.isBigDecimal(type)){
					if(tempHighValue.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - For Large Integer type column, MaxValue should be less than or equal to " + Long.MAX_VALUE+".", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					if(tempLowValue.compareTo(new BigDecimal(Long.MIN_VALUE)) < 0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - For Large Integer type column, MinValue should be greater than or equal to " + Long.MIN_VALUE+".", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				}
				// No need to test for Integer type since we parse it to Integer object in DataType.java class, but for Long Integer, we don't parse it to Long type object.
			}

			/*
			 * HISTOGRAM VALIDATION:
			 * 
			 * Node : Frequency Value Distribution (9)
			 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
			 * Consistency Constraints: 
			 * (5) Sum of ValCount's must be lesser than or equal to the cardinality.
			 * (6) Number of entries must be lesser than or equal to the Distinct Column Values.
			 * 
			 * Inner Graph Validation.
			 * Structural Constraints: Nodes (1,3,5,.. 39) ColValue - Data type must be same as column datatype
			 *                         Node (2,4,6... 40) -  >=0
			 * Consistency Constraints: NILL
			 */
			int i = 0;
			count = this.tableHist.getModel().getRowCount();
			interval_number = new Integer[count];
			interval_rowcount = new BigDecimal[count];
			interval_uec = new BigDecimal[count];
			interval_boundary = new String[count];
			colVal = new String[count];
			sumValCounts = BigDecimal.ZERO;
			sumtotaluec = BigDecimal.ZERO;
			DataType previousValue = low_Value;
			DataType finalBucketBoundary = null;
			while (count > 0) {
				String s1 = "";
				if(this.tableHist.getModel().getValueAt(i, 0)!=null){
					s1=this.tableHist.getModel().getValueAt(i, 0).toString().trim();
				}
				String s2 = "";
				if(this.tableHist.getModel().getValueAt(i, 1)!=null){
					s2=this.tableHist.getModel().getValueAt(i, 1).toString().trim();
				}
				String s3 = "";
				if(this.tableHist.getModel().getValueAt(i, 2)!=null){
					s3=this.tableHist.getModel().getValueAt(i, 2).toString().trim();
				}
				String s4 = "";
				if(this.tableHist.getModel().getValueAt(i, 3+k)!=null){
					s4=this.tableHist.getModel().getValueAt(i, 3+k).toString().trim();
				}
				if(s1.isEmpty() || s2.isEmpty() || s3.isEmpty() || s4.isEmpty()){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Please fill all the entries in the histogram.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					interval_number[i] = new Integer(s1);
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_NUMBER present in " + (i + 1) + "th row is not of INTEGER data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					interval_rowcount[i] = new BigDecimal(new BigInteger(s2)); 
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_ROWCOUNT Value present in " + (i + 1) + "th row is not of INTEGER type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					interval_uec[i] = new BigDecimal(new BigInteger(s3));
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_UEC Value present in " + (i + 1) + "th row is not of INTEGER type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				if(interval_rowcount[i].compareTo(BigDecimal.ZERO)<0){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_ROWCOUNT Value present in " + (i + 1) + "th row should be greater than or equal to 0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}else{
					sumValCounts = sumValCounts.add(interval_rowcount[i]);
				}
				if(interval_uec[i].compareTo(BigDecimal.ZERO)<0){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_UEC Value present in " + (i + 1) + "th row should be greater than or equal to 0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}else{
					sumtotaluec = sumtotaluec.add(interval_uec[i]);
				}
				if(interval_rowcount[i].compareTo(interval_uec[i]) < 0){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_UEC Value present in " + (i + 1) + "th row is greater than corresponding INTERVAL_ROWCOUNT.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;		
				}
				if((interval_uec[i].compareTo(BigDecimal.ZERO)==0) && (interval_rowcount[i].compareTo(BigDecimal.ZERO)>0))
				{
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_UEC Value present in " + (i + 1) + "th row should be greater than 0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					DataType boundary = new DataType(type, s4); // It will throw an exception if the data type does not match.
					// (13,14 ) There can be no entry greater than highvalue and NO entry lesser than lowvalues.
					if(boundary.compare(high_Value)>0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - There are one or more rows whose INTERVAL_BOUNDARY is greater than High_Value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					if(boundary.compare(low_Value)<0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - There are one or more rows whose INTERVAL_BOUNDARY is lesser than Low_Value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					// For i=0, boundary can also be equal to previousValue which is lowValue. 
					// This validation is already present when we compare with low value. So no need to test when i=0;
					if(i>0 && boundary.compare(previousValue)<=0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - INTERVAL_BOUNDARY value for bucket number " + (i+1) + " should be greater than the value present in previous bucket.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					// Ashoke - TODO
					// If datatype is of integer type, then the number of UEC in a bucket can not be greater than the bucket size.
					if(DataType.isInteger(type) || DataType.isBigDecimal(type)){
						long oldValue;
						long newValue;
						if(DataType.isInteger(type)){
							oldValue = previousValue.getInteger();
							newValue = boundary.getInteger();
						}else if(DataType.isNumeric(type)){
							oldValue = previousValue.getBigDecimal().longValue();
							newValue = boundary.getBigDecimal().longValue();
						}else{
							oldValue = previousValue.getBigInteger().longValue();
							newValue =	boundary.getBigInteger().longValue();							
						}
						// For first bucket
						if(i==0 && ((newValue-oldValue+1) < interval_uec[i].longValue())){
							valid = false;
							JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - INTERVAL_UEC for bucket number " + (i+1) + " should be less than or equal to " + (newValue-oldValue+1) + ".", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
							return valid;
						}
						// For other buckets
						if(i>0 && ((newValue-oldValue) < interval_uec[i].longValue())){
							valid = false;
							JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - INTERVAL_UEC for bucket number " + (i+1) + " should be less than or equal to " + (newValue-oldValue) + ".", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
							return valid;
						}
					}
					previousValue = boundary;
					finalBucketBoundary = boundary;
					interval_boundary[i] = s4;
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Histogram - INTERVAL_BOUNDARY value present in " + (i + 1) + "th row and " + (4+k) + "th column is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				colVal[i] = "";
				for(int l = 0; l <= k; l++){
					colVal[i] = colVal[i] + this.tableHist.getModel().getValueAt(i, 3+l).toString() + "|";
				}
				i++;
				count--;
			}
			// Test that last bucket value should be equal to high value.
			if(finalBucketBoundary!=null && finalBucketBoundary.compare(high_Value)!=0){
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - INTERVAL_BOUNDARY value present in Final Bucket should be equal to the High_Value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			count = i;

		} // End of for loop for each column
		if (count > 0) {
			// Main Graph Consistency Constraints
			// (7) Sum of ValCount's must be lesser than or equal to the cardinality.
			if (graph_flag) {
				if (sumValCounts.compareTo(card) > 0) {
					JOptionPane.showMessageDialog(null, "Consistency Constraint: Histogram - Sum of INTERVAL_ROWCOUNTs is greater than the Cardinality of the relation.\n"
							+ "You need to correct this before Update Column is clicked", "Warning!", JOptionPane.WARNING_MESSAGE);
					valid = true;
				}
			}else {
				if (sumValCounts.compareTo(card) <= 0) {
					valid = true;
				} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - Sum of INTERVAL_ROWCOUNTs is greater than the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
				}
			}
			if (graph_flag) {
				if (!(sumtotaluec.compareTo(total_uec) == 0)){
					JOptionPane.showMessageDialog(null, "Consistency Constraint: Histogram - Sum of INTERVAL_UEC is not equal to TOTAL_UEC of the relation.\n"
							+ "You need to correct this before Update Column is clicked", "Warning!", JOptionPane.WARNING_MESSAGE);
				}
			} else {
				if (!(sumtotaluec.compareTo(total_uec) == 0)){
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - Sum of INTERVAL_UEC is not equal to TOTAL_UEC of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			graph_flag = false;
			// (9) Number of entries must be lesser than or equal to the column cardinality.
			BigDecimal countFreqBI = new BigDecimal(count);
			if (countFreqBI.compareTo(col_card.add(BigDecimal.ONE)) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Histogram - Number of entries in the Histogram is greater than the Column Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			histogram = new NonStopSQLHistObject[count];
			for (int i = 0; i < count; i++) {
				Integer number = interval_number[i];
				BigDecimal rowcount = interval_rowcount[i];
				BigDecimal uec = interval_uec[i];
				String boundary = colVal[i].substring(0, colVal[i].length()-1);
				NonStopSQLHistObject nsHistObject = new NonStopSQLHistObject(number, rowcount, uec, boundary);
				histogram[i] = nsHistObject;
			}
		}
		/*
		 * Read the current relation stats and update into the allStats data structure.
		 */
		nsColStat.setColCard(col_card);
		nsColStat.setTOTAL_UEC(total_uec);
		// Added so that leading whitespaces are not removed.
		String type = nsColStat.getColumnType();
		String LowValue = null;
		String HighValue = null;
		if(DataType.isString(type)){
			HighValue = textHighvalue.getText().replaceFirst("\\s+$", "");
			LowValue = textLowvalue.getText().replaceFirst("\\s+$", "");
		} else{
			LowValue = textLowvalue.getText().trim();
			HighValue = textHighvalue.getText().trim();
		}
		nsColStat.setHIGH_VALUE(HighValue);
		nsColStat.setLOW_VALUE(LowValue);
		if(histogram!=null){
			nsColStat.setNumBuckets(histogram.length);			
		}else{
			nsColStat.setNumBuckets(0);
		}
		nsColStat.setNonStopSQLHistogram(histogram);
		stat.setColumnStatistics(colName, nsColStat);    
		stats.put(relationName, stat);
		return valid;
	}




	private void UpdateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_UpdateActionPerformed
		int status = JOptionPane.showConfirmDialog(null, "If you have chosen to construct without inputting or modifying all relations or columns,\nthen they are not validated for legal and consistency constraint.\n"
				+ "Do you want to continue with the construcion?", "Choose the option.", JOptionPane.YES_NO_OPTION);
		if (status == JOptionPane.YES_OPTION) {
			this.updateStats();
		}
	}//GEN-LAST:event_UpdateActionPerformed

	private void updateStats() {
		Iterator<String> iter = this.stats.keySet().iterator();
		boolean success = true;
		while (iter.hasNext()) {
			String rel = iter.next();
			Statistics stat = this.stats.get(rel);
			Constants.CPrintToConsole("Constructing Statistics for " + rel, Constants.DEBUG_FIRST_LEVEL_Information);
			BigDecimal cardinality = stat.getRelationStatistics().getCardinality();
			if(cardinality!=null && cardinality != BigDecimal.ZERO){
				success = stat.updateStatisticsToDatabase(database);				
			}
			if(!success){
				JOptionPane.showMessageDialog(null, "Statistics Construction for the relation " + rel + " failed.", "CODD - Message", JOptionPane.ERROR_MESSAGE);
				break;
			}
		}
		if(success){
			JOptionPane.showMessageDialog(null, "Statistics Construction is Successful.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
		}
	}


	/**
	 * This method is used by the graph histogram to set the values in histogram table.
	 */
	@Override
	public void setHistogram(ArrayList<BucketItem> buckets, boolean noDistinct) {
		//Set new lowValue and highValue
		String lowValue = buckets.get(0).getLValue().getString();
		String highValue = buckets.get(buckets.size()-1).getValue().getString();
		this.textLowvalue.setText(lowValue);
		this.textHighvalue.setText(highValue);
		//Remove 0th row quantHistRows times
		while(tableHist.getModel().getRowCount()>0){
			((DefaultTableModel) tableHist.getModel()).removeRow(0);
		}
		this.histRows = buckets.size();
		this.textNumBuckets.setText(buckets.size()+"");
		for (int i = 0; i < buckets.size(); i++) {
			BucketItem bucket = buckets.get(i);
			Constants.CPrintToConsole("(" + bucket.getLValue().getString() + "," + bucket.getValue().getString() + ") :: " + bucket.getFreq() + " (" + bucket.getFreqPercent() + ")" + " : " + bucket.getDistinctCount() + " (" + bucket.getDistinctCountPercent() + ")", Constants.DEBUG_SECOND_LEVEL_Information);
			String colValue = bucket.getValue().getString();
			long freq = (long)bucket.getFreq();
			String freqStr = freq + "";
			long distCount = (long)bucket.getDistinctCount();
			String distCountStr = this.emptyString;
			if (!noDistinct) {
				distCountStr = distCount + "";
			}
			String[] data = {(i+1) + "", freqStr, distCountStr, colValue};
			((DefaultTableModel) tableHist.getModel()).addRow(data);
		}
	}

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton backButton;
	private javax.swing.JButton Update;
	private javax.swing.JButton buttonCreateBuckets;
	private javax.swing.JButton buttonHistUpload;
	private javax.swing.JButton buttonQuantHistShowGraphActionPerformed;
	private javax.swing.JButton buttonResetColValues;
	private javax.swing.JButton buttonUpdateColumn;
	private javax.swing.JButton buttonUpdateRelation;
	private javax.swing.JComboBox comboBoxAttribute;
	private javax.swing.JComboBox comboBoxRelations;
	private javax.swing.JFrame jFrame1;
	private javax.swing.JLabel jLabel1;
	private javax.swing.JLabel jLabel10;
	private javax.swing.JLabel jLabel11;
	private javax.swing.JLabel jLabel13;
	private javax.swing.JLabel jLabel3;
	private javax.swing.JLabel jLabel4;
	private javax.swing.JLabel jLabel7;
	private javax.swing.JLabel jLabel8;
	private javax.swing.JLabel jLabel9;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel2;
	private javax.swing.JPanel jPanel3;
	private javax.swing.JPanel jPanel4;
	private javax.swing.JPanel jPanel5;
	private javax.swing.JScrollPane jScrollPane2;
	private javax.swing.JSeparator jSeparator2;
	private javax.swing.JSeparator jSeparator3;
	private javax.swing.JTable tableHist;
	private javax.swing.JTextField textCard;
	private javax.swing.JTextField textHighvalue;
	private javax.swing.JTextField textHistWrite;
	private javax.swing.JTextField textLowvalue;
	private javax.swing.JTextField textNumBuckets;
	private javax.swing.JTextField textTotaluec;
	// End of variables declaration//GEN-END:variables



	/** This method is called from within the constructor to
	 * initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is
	 * always regenerated by the Form Editor.
	 */
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jFrame1 = new javax.swing.JFrame();
		jPanel3 = new javax.swing.JPanel();
		jPanel1 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		jSeparator2 = new javax.swing.JSeparator();
		comboBoxRelations = new JComboBox();
		jLabel1 = new javax.swing.JLabel();
		jSeparator3 = new javax.swing.JSeparator();
		jPanel5 = new javax.swing.JPanel();
		textCard = new javax.swing.JTextField();
		jLabel11 = new javax.swing.JLabel();
		buttonUpdateRelation = new javax.swing.JButton();
		jScrollPane2 = new javax.swing.JScrollPane();
		tableHist = new javax.swing.JTable();
		tableHist.setSurrendersFocusOnKeystroke(true);
		tableHist.setColumnSelectionAllowed(true);
		tableHist.setFillsViewportHeight(true);
		tableHist.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		textHistWrite = new javax.swing.JTextField();
		// Used to add the copy paste functionality in JTable.
		new ExcelAdapter(tableHist);
		buttonQuantHistShowGraphActionPerformed = new javax.swing.JButton();
		jPanel2 = new javax.swing.JPanel();
		jLabel3 = new javax.swing.JLabel();
		textTotaluec = new javax.swing.JTextField();
		jLabel8 = new javax.swing.JLabel();
		textHighvalue = new javax.swing.JTextField();
		jLabel7 = new javax.swing.JLabel();
		textLowvalue = new javax.swing.JTextField();
		buttonHistUpload = new javax.swing.JButton();
		jLabel13 = new javax.swing.JLabel();
		comboBoxAttribute = new JComboBox();
		jLabel4 = new javax.swing.JLabel();
		jLabel9 = new javax.swing.JLabel();
		jPanel4 = new javax.swing.JPanel();
		jLabel10 = new javax.swing.JLabel();
		textNumBuckets = new javax.swing.JTextField();
		buttonCreateBuckets = new javax.swing.JButton();
		buttonUpdateColumn = new javax.swing.JButton();
		buttonResetColValues = new javax.swing.JButton();
		Update = new javax.swing.JButton();
		backButton = new javax.swing.JButton();
		javax.swing.GroupLayout jFrame1Layout = new javax.swing.GroupLayout(jFrame1.getContentPane());
		jFrame1.getContentPane().setLayout(jFrame1Layout);
		jFrame1Layout.setHorizontalGroup(
				jFrame1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 400, Short.MAX_VALUE)
				);
		jFrame1Layout.setVerticalGroup(
				jFrame1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 300, Short.MAX_VALUE)
				);

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3.setLayout(jPanel3Layout);
		jPanel3Layout.setHorizontalGroup(
				jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 100, Short.MAX_VALUE)
				);
		jPanel3Layout.setVerticalGroup(
				jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 100, Short.MAX_VALUE)
				);

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
		setTitle("NON STOP SQL CONSTRUCT MODE");
		setForeground(new java.awt.Color(240, 240, 240));

		jSeparator2.setForeground(new java.awt.Color(0, 0, 0));

		comboBoxRelations.setToolTipText("Selected Relations for Construct Mode");
		comboBoxRelations.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxRelationsActionPerformed(evt);
			}
		});

		jLabel1.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel1.setText("Relation Name :");

		jPanel5.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Please Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel5.setOpaque(false);

		textCard.setFont(new java.awt.Font("Cambria", 0, 14));
		textCard.setMinimumSize(new java.awt.Dimension(6, 20));
		textCard.setPreferredSize(new java.awt.Dimension(6, 20));

		jLabel11.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel11.setText("Cardinality");

		buttonUpdateRelation.setText("UPDATE");
		buttonUpdateRelation.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateRelationActionPerformed(evt);
			}
		});

		JLabel jLabel12 = new JLabel();
		jLabel12.setText("Leave it zero, if you don't want to update the statistic for the selected relation.");
		jLabel12.setFont(new Font("Dialog", Font.PLAIN, 14));

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5Layout.setHorizontalGroup(
				jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel5Layout.createSequentialGroup()
										.addComponent(jLabel11)
										.addGap(26)
										.addComponent(textCard, GroupLayout.DEFAULT_SIZE, 403, Short.MAX_VALUE)
										.addGap(37))
										.addComponent(jLabel12, GroupLayout.PREFERRED_SIZE, 533, GroupLayout.PREFERRED_SIZE))
										.addGap(109)
										.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
										.addContainerGap())
				);
		jPanel5Layout.setVerticalGroup(
				jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel5Layout.createSequentialGroup()
										.addGroup(jPanel5Layout.createParallelGroup(Alignment.BASELINE)
												.addComponent(jLabel11)
												.addComponent(textCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
												.addPreferredGap(ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
												.addComponent(jLabel12, GroupLayout.PREFERRED_SIZE, 19, GroupLayout.PREFERRED_SIZE))
												.addGroup(jPanel5Layout.createSequentialGroup()
														.addContainerGap()
														.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 29, GroupLayout.PREFERRED_SIZE)))
														.addContainerGap())
				);
		jPanel5.setLayout(jPanel5Layout);

		jScrollPane2.setBackground(new java.awt.Color(255, 255, 255));

		tableHist.setFont(new java.awt.Font("Times New Roman", 0, 11));
		Object[][] tempTabRows = new Object[0][4];
		tableHist.setModel(new DefaultTableModel(
				tempTabRows,
				new String[] {
						"INTERVAL_NUMBER", "INTERVAL_ROWCOUNT", "INTERVAL_UEC", "INTERVAL_BOUNDARY"
				}
				));
		tableHist.setToolTipText("<HTML> Structural Constraint: <br> &nbsp; &nbsp; &nbsp; COLVALUE : Value whose type is same as column type <br> Consistency Constraint : <br> &nbsp; &nbsp; COLVALUE must be increasing. </HMTL>");
		tableHist.setGridColor(new java.awt.Color(204, 204, 204));
		jScrollPane2.setViewportView(tableHist);

		buttonQuantHistShowGraphActionPerformed.setText("Show Graph");
		buttonQuantHistShowGraphActionPerformed.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				graph_flag = true;
				buttonQuantHistShowGraphActionPerformedActionPerformed(evt);
			}
		});

		jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Please Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel2.setOpaque(false);

		jLabel3.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel3.setText("Total UEC:");

		jLabel8.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel8.setText("High Value: ");

		jLabel7.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel7.setText("Low Value:");

		JLabel lblUsePipeTo = new JLabel("*Use PIPE Operator (|) to seperate the values in Low Value and High Value for multidimensional histograms.");
		lblUsePipeTo.setForeground(Color.RED);

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
		jPanel2Layout.setHorizontalGroup(
				jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel2Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel2Layout.createSequentialGroup()
										.addComponent(jLabel3, GroupLayout.PREFERRED_SIZE, 75, GroupLayout.PREFERRED_SIZE)
										.addPreferredGap(ComponentPlacement.RELATED)
										.addComponent(textTotaluec, GroupLayout.DEFAULT_SIZE, 198, Short.MAX_VALUE)
										.addPreferredGap(ComponentPlacement.UNRELATED)
										.addComponent(jLabel7, GroupLayout.PREFERRED_SIZE, 75, GroupLayout.PREFERRED_SIZE)
										.addPreferredGap(ComponentPlacement.RELATED)
										.addComponent(textLowvalue, GroupLayout.DEFAULT_SIZE, 136, Short.MAX_VALUE)
										.addPreferredGap(ComponentPlacement.UNRELATED)
										.addComponent(jLabel8)
										.addPreferredGap(ComponentPlacement.RELATED)
										.addComponent(textHighvalue, GroupLayout.DEFAULT_SIZE, 186, Short.MAX_VALUE))
										.addComponent(lblUsePipeTo, Alignment.TRAILING))
										.addContainerGap())
				);
		jPanel2Layout.setVerticalGroup(
				jPanel2Layout.createParallelGroup(Alignment.CENTER)
				.addGroup(jPanel2Layout.createSequentialGroup()
						.addGroup(jPanel2Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(jLabel3, GroupLayout.PREFERRED_SIZE, 17, GroupLayout.PREFERRED_SIZE)
								.addComponent(textTotaluec, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addGroup(jPanel2Layout.createParallelGroup(Alignment.BASELINE)
										.addComponent(textLowvalue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
										.addComponent(jLabel7, GroupLayout.PREFERRED_SIZE, 20, GroupLayout.PREFERRED_SIZE)
										.addComponent(jLabel8))
										.addComponent(textHighvalue, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
										.addPreferredGap(ComponentPlacement.RELATED, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(lblUsePipeTo))
				);
		jPanel2.setLayout(jPanel2Layout);

		buttonHistUpload.setText("UPLOAD");
		buttonHistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonHistUploadActionPerformed(evt);
			}
		});

		jLabel13.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel13.setForeground(new java.awt.Color(255, 0, 51));
		jLabel13.setText("Please enter values in table OR browse file  containing values");

		comboBoxAttribute.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxAttributeActionPerformed(evt);
			}
		});

		jLabel4.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel4.setText("Attribute Name : ");

		jLabel9.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel9.setText("File for Histogram Information:");

		jPanel4.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Bucket Required", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel4.setOpaque(false);

		jLabel10.setFont(new java.awt.Font("Cambria", 0, 14));
		jLabel10.setText("Press 'Create'  button to populate below table with number of buckets/rows given :");

		textNumBuckets.setText(histRows+"");

		buttonCreateBuckets.setText("CREATE");
		buttonCreateBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateBucketsActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4.setLayout(jPanel4Layout);
		jPanel4Layout.setHorizontalGroup(
				jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addContainerGap(35, Short.MAX_VALUE)
						.addComponent(jLabel10, javax.swing.GroupLayout.PREFERRED_SIZE, 533, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addGap(2, 2, 2)
						.addComponent(textNumBuckets, javax.swing.GroupLayout.PREFERRED_SIZE, 48, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addGap(34, 34, 34)
						.addComponent(buttonCreateBuckets, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addContainerGap())
				);
		jPanel4Layout.setVerticalGroup(
				jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
								.addComponent(jLabel10)
								.addComponent(textNumBuckets, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addComponent(buttonCreateBuckets, javax.swing.GroupLayout.PREFERRED_SIZE, 29, javax.swing.GroupLayout.PREFERRED_SIZE))
								.addGap(0, 0, Short.MAX_VALUE))
				);

		buttonUpdateColumn.setText("Update Column");
		buttonUpdateColumn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateColumnActionPerformed(evt);
			}
		});

		buttonResetColValues.setText("Reset");
		buttonResetColValues.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonResetColValuesActionPerformed(evt);
			}
		});

		Update.setText("Construct");
		Update.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				UpdateActionPerformed(evt);
			}
		});
		backButton.setText("<< Back");
		backButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent evt) {
				backButtonActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1Layout.setHorizontalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addGap(6)
									.addComponent(jLabel1, GroupLayout.PREFERRED_SIZE, 105, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, 396, GroupLayout.PREFERRED_SIZE))
								.addComponent(jSeparator3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED, 178, Short.MAX_VALUE)
									.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 103, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(buttonQuantHistShowGraphActionPerformed, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 119, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(buttonUpdateColumn)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(Update, GroupLayout.PREFERRED_SIZE, 118, GroupLayout.PREFERRED_SIZE)
									.addGap(26))
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addGap(12)
									.addComponent(jLabel4)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, 389, GroupLayout.PREFERRED_SIZE))
								.addComponent(jPanel5, GroupLayout.DEFAULT_SIZE, 822, Short.MAX_VALUE))
							.addContainerGap())
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
								.addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, 802, Short.MAX_VALUE)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addPreferredGap(ComponentPlacement.RELATED)
									.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
										.addComponent(jPanel4, GroupLayout.DEFAULT_SIZE, 798, Short.MAX_VALUE)
										.addGroup(Alignment.LEADING, jPanel1Layout.createSequentialGroup()
											.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
												.addGroup(jPanel1Layout.createSequentialGroup()
													.addComponent(jLabel9, GroupLayout.PREFERRED_SIZE, 201, GroupLayout.PREFERRED_SIZE)
													.addPreferredGap(ComponentPlacement.RELATED)
													.addComponent(textHistWrite, GroupLayout.DEFAULT_SIZE, 421, Short.MAX_VALUE))
												.addComponent(jLabel13, GroupLayout.PREFERRED_SIZE, 469, GroupLayout.PREFERRED_SIZE))
											.addGap(18)
											.addComponent(buttonHistUpload, GroupLayout.PREFERRED_SIZE, 117, GroupLayout.PREFERRED_SIZE))
										.addComponent(jScrollPane2, GroupLayout.DEFAULT_SIZE, 798, Short.MAX_VALUE))))
							.addContainerGap())))
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(jLabel1))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel5, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(jLabel4)
						.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(jPanel2, GroupLayout.PREFERRED_SIZE, 66, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel4, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jLabel13, GroupLayout.PREFERRED_SIZE, 19, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.CENTER)
						.addComponent(jLabel9, GroupLayout.PREFERRED_SIZE, 19, GroupLayout.PREFERRED_SIZE)
						.addComponent(textHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(buttonHistUpload))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jScrollPane2, GroupLayout.DEFAULT_SIZE, 234, Short.MAX_VALUE)
					.addGap(18)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE)
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 32, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonQuantHistShowGraphActionPerformed, GroupLayout.PREFERRED_SIZE, 32, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 31, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 31, GroupLayout.PREFERRED_SIZE)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 32, GroupLayout.PREFERRED_SIZE)))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jSeparator3, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE)
					.addGap(0))
		);
		jPanel1Layout.linkSize(SwingConstants.HORIZONTAL, new Component[] {buttonQuantHistShowGraphActionPerformed, buttonUpdateColumn});
		jPanel1.setLayout(jPanel1Layout);

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
		layout.setHorizontalGroup(
			layout.createParallelGroup(Alignment.LEADING)
				.addGroup(layout.createSequentialGroup()
					.addGap(1)
					.addComponent(jPanel1, GroupLayout.DEFAULT_SIZE, 846, Short.MAX_VALUE)
					.addGap(1))
		);
		layout.setVerticalGroup(
			layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(layout.createSequentialGroup()
					.addGap(0)
					.addComponent(jPanel1, GroupLayout.PREFERRED_SIZE, 660, Short.MAX_VALUE))
		);
		getContentPane().setLayout(layout);

		pack();
	}// </editor-fold>//GEN-END:initComponents
}
