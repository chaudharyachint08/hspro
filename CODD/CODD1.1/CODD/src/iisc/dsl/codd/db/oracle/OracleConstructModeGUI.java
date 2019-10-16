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

import iisc.dsl.codd.client.ConstructMode;
import iisc.dsl.codd.client.ConstructModeFrame;
import iisc.dsl.codd.client.gui.ExcelAdapter;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Constraint;
import iisc.dsl.codd.ds.Statistics;
import iisc.dsl.codd.graphhistogram.BucketItem;
import iisc.dsl.codd.ds.DataType;
import iisc.dsl.codd.graphhistogram.GraphHistogram;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JOptionPane;

import java.util.Set;

import javax.swing.JFileChooser;
import javax.swing.table.DefaultTableModel;
import javax.swing.GroupLayout.Alignment;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.GroupLayout;

import java.awt.Dimension;

import javax.swing.JSeparator;

import java.awt.BorderLayout;
import java.awt.Font;

import javax.swing.JLabel;

import java.awt.Color;

import javax.swing.JButton;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.swing.JSlider;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeEvent;

/**
 * Construct Mode Frame for Oracle.
 * @author dsladmin
 */
public class OracleConstructModeGUI extends ConstructModeFrame {

	private static final long serialVersionUID = 765180025052152704L;
	
	static final int FPS_MIN = 0;
    static final int FPS_MAX = 30;
    static final int FPS_INIT = 1;

	// Used in the ComboBox
	String emptyString;

	Database database;
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
	HashMap<String, HashMap<String, Boolean>> updateableStats;

	/**
	 * Keep tracks of update relation statistics.
	 * <Relation, Integer (1/0) updated>
	 */
	HashMap<String, Integer> relUpdated;

	// To avoid handling changing event when rels are being populated
	boolean populatingRels;
	// To avoid handling changing event when cols are being populated
	boolean populatingCols;

	// Number of Frequency Histogram Buckets
	int freqHistRows;
	// Number of Quantile Histogram Buckets
	int quantHistRows;

	/**
	 * Keep tracks of indexed columns and a single column representative
	 * <String representativeColumn, String colNames>
	 * e.g <L_LINENUMBER, L_LINENUMBER+L_ORDERKEY>
	 * Index Statistics will be enabled for L_LINENUMBER
	 * Outer HashMap is for relation of allStats.
	 */
	HashMap<String,HashMap<String, String>> indexColumns;

	/** Creates new form DB2ConstructModeGUI */
	public OracleConstructModeGUI(Database database, Statistics[] stats) {
		super("Oracle Construct Mode");
		quantHistRows = OracleColumnStatistics.DefaultHeightBalancedBucketSize;
		freqHistRows = OracleColumnStatistics.DefaultFrequencyBucketSize;
		initComponents();
		setLocationRelativeTo(null);
		emptyString = "";
		this.database = database;
		this.stats = new HashMap<String, Statistics>();
		// Fill updateableStats.
		updateableStats = new HashMap<String, HashMap<String,Boolean>>();
		relUpdated = new HashMap<String, Integer>();
		indexColumns = new HashMap<String, HashMap<String, String>>();
		for(int i=0;i<stats.length;i++){
			String relationName = stats[i].getRelationName();
			this.stats.put(relationName, stats[i]);
			relUpdated.put(relationName, 0);
			HashMap<String,Boolean> columns = new HashMap<String, Boolean>();
			try{
				String[] cols = database.getAttributes(relationName);
				for (int j = 0; j < cols.length; j++) {
					columns.put(cols[j],false);
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
					indexCols.put(repColumn, key);
				}
				indexColumns.put(relationName, indexCols);
			}catch(Exception e)
			{
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
	 * Returns the current relation, chosen for update
	 * @return
	 */
	private String getCurrentRelation()
	{
		String currRel = (String)this.comboBoxRelations.getSelectedItem();
		return currRel;
	}

	/**
	 * Returns the current attribute, chosen for update
	 * @return
	 */
	private String getCurrentAttribute()
	{
		String column = (String)this.comboBoxAttribute.getSelectedItem();
		return column;
	}
	/**
	 * Populates the set of relations from updateableStats to the relationsCombobox.
	 */
	private void populateRelationsComboBox()
	{
		populatingRels = true;
		this.comboBoxRelations.removeAllItems();
		this.comboBoxRelations.addItem(emptyString);
		Set<String> set = updateableStats.keySet();
		Iterator<String> iter = set.iterator();
		while(iter.hasNext())
		{
			String rel = iter.next();
			this.comboBoxRelations.addItem(rel);
		}
		this.comboBoxRelations.setSelectedItem(emptyString);
		this.comboBoxAttribute.removeAllItems();
		this.comboBoxAttribute.setEnabled(false);
		populatingRels = false;
	}

	/**
	 * Populates the set of columns of the give relation from updateableStats.
	 */
	private void populateColumnsComboBox(String relationName)
	{
		populatingCols = true;
		this.comboBoxAttribute.removeAllItems();
		this.comboBoxAttribute.addItem(emptyString);
		Set<String> set = updateableStats.get(relationName).keySet();
		Iterator<String> iter = set.iterator();
		while (iter.hasNext()) {
			String col = iter.next();
			this.comboBoxAttribute.addItem(col);
		}
		this.comboBoxAttribute.setSelectedItem(emptyString);
		populatingCols = false;
	}

	/**
	 * Relation Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeRelationLevelMetadata(String relationName)
	{
		if(relationName!= null)
		{
			OracleRelationStatistics relationStatistics = (OracleRelationStatistics) this.stats.get(relationName).getRelationStatistics();
			this.textCard.setText(relationStatistics.getCardinality()+"");
			this.textBlocks.setText(relationStatistics.getBlocks()+"");
			this.textAvgRowLen.setText(relationStatistics.getAvgRowLen()+"");
			//this.cardSlider.setValue((int) Math.log10(relationStatistics.getCardinality().doubleValue()));
			this.cardSlider.setValue(1);
		}
		else
		{
			this.textCard.setText("");
			this.textBlocks.setText("");
			this.textAvgRowLen.setText("");
		}
	}

	/**
	 * Relation Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledRelationLevelMetadata(boolean enable)
	{
		this.textCard.setEnabled(enable);
		this.textBlocks.setEnabled(enable);
		this.textAvgRowLen.setEnabled(enable);
		this.cardSlider.setEnabled(enable);

		this.buttonUpdateRelation.setEnabled(enable);
	}

	/**
	 * Column-Index Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeColumnLevelMetadata(String columnName)
	{
		if(columnName != null)
		{
			String relationName = this.getCurrentRelation();
			int flag = 0;
			OracleColumnStatistics columnStatistics = (OracleColumnStatistics) this.stats.get(relationName).getColumnStatistics(columnName);
			
			/*if (DataType.isDouble(columnStatistics.getColumnType()) || DataType.isInteger(columnStatistics.getColumnType()) || DataType.isNumeric(columnStatistics.getColumnType())) {
				flag = 1;
			}*/
			// Column-Level
			this.textDistColValues.setText(columnStatistics.getColCard() + "");
			this.textNullCount.setText(columnStatistics.getNumNulls() + "");
			this.textAvgColLen.setText(columnStatistics.getAvgColLen()+"");
			this.textDensity.setText(columnStatistics.getDensity()+"");

			String histogramType = columnStatistics.getHistogramType();
			this.comboBoxHistogramType.setSelectedItem((Object)histogramType);
			int numBuckets = columnStatistics.getNumBuckets();
			// Column-Histogram-Level
			if(histogramType.equals(OracleColumnStatistics.Frequency)) {
				// Freq Hist
				OracleHistObject[] hist = columnStatistics.getOralceHistogram();
				if (hist != null && numBuckets > 0) {
					//Remove 0th row freqHistRows times
					for (int r = 0; r < freqHistRows; r++) {
						((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
					}
					freqHistRows = numBuckets;
					textNumFreqBuckets.setText(""+freqHistRows);
					checkFreqHistWrite.setSelected(false);
					textFreqHistWrite.setText("");
					String[] data = {"", ""};
					for (int r = 0; r < freqHistRows; r++) {
						((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
					}

					for(int i=0; i< numBuckets; i++) {
						String colValue = hist[i].getEndPointActualValue();
						BigInteger valCount =  new BigInteger(hist[i].getEndPointNumber());
						if (colValue != null) {
							((DefaultTableModel) tableFrqHist.getModel()).setValueAt(colValue, i - 1, 0);
							String valCountStr = "" + valCount;
							((DefaultTableModel) tableFrqHist.getModel()).setValueAt(valCountStr, i - 1, 1);
						}
					}
				} else {
					//Remove 0th row freqHistRows times
					for (int r = 0; r < freqHistRows; r++) {
						((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
					}
					freqHistRows = OracleColumnStatistics.DefaultFrequencyBucketSize;
					textNumFreqBuckets.setText(""+freqHistRows);
					checkFreqHistWrite.setSelected(false);
					textFreqHistWrite.setText("");
					String[] data = {"", ""};
					for (int r = 0; r < freqHistRows; r++) {
						((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
					}
				}
			}
			// Quant Hist
			else if(histogramType.equals(OracleColumnStatistics.HeightBalanced)) {
				OracleHistObject[] hist = columnStatistics.getOralceHistogram();
				if (hist != null && numBuckets > 0) {
					//Remove 0th row quantHistRows times
					for (int r = 0; r < quantHistRows; r++) {
						((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(0);
					}
					quantHistRows = numBuckets;
					textNumQuantBuckets.setText(""+quantHistRows);
					checkQuantHistWrite.setSelected(false);
					textQuantHistWrite.setText("");
					String[] data = {""};
					for (int r = 0; r < quantHistRows; r++) {
						((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data);
					}

					for(int i=0; i< numBuckets; i++) {
						String colValue = hist[i].getEndPointActualValue();
						//if(colValue == null && flag == 1) colValue = hist[i].getEndPointValue();
						if (colValue != null) {
							((DefaultTableModel) tableHeightBalancedHist.getModel()).setValueAt(colValue, i, 0);
						}
					}
				} else {
					//Remove 0th row freqHistRows times
					for (int r = 0; r < quantHistRows; r++) {
						((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(0);
					}
					quantHistRows = OracleColumnStatistics.DefaultHeightBalancedBucketSize;
					textNumQuantBuckets.setText(""+quantHistRows);
					checkQuantHistWrite.setSelected(false);
					textQuantHistWrite.setText("");
					String[] data = {""};
					for (int r = 0; r < quantHistRows; r++) {
						((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data);
					}
				}
			}
			else {
				// Column-Histogram-Level
				this.comboBoxHistogramType.setSelectedIndex(0);
				// Freq Hist
				//Remove 0th row freqHistRows times
				for (int r = 0; r < freqHistRows; r++) {
					((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
				}
				freqHistRows = OracleColumnStatistics.DefaultFrequencyBucketSize;
				textNumFreqBuckets.setText(""+freqHistRows);
				checkFreqHistWrite.setSelected(false);
				textFreqHistWrite.setText("");

				String[] data = {"", ""};
				for (int r = 0; r < freqHistRows; r++) {
					((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
				}
				// Quant Hist
				//Remove 0th row quantHistRows times
				for (int r = 0; r < quantHistRows; r++) {
					((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(0);
				}
				quantHistRows = OracleColumnStatistics.DefaultHeightBalancedBucketSize;
				textNumQuantBuckets.setText(""+quantHistRows);
				checkQuantHistWrite.setSelected(false);
				textQuantHistWrite.setText("");

				String[] data1 = {""};
				for (int r = 0; r < quantHistRows; r++) {
					((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data1);
				}
			}

			// Index-Level
			HashMap<String, String> indexCols = indexColumns.get(relationName);

			String colNames = indexCols.get(columnName);
			if(colNames != null)
			{
				OracleIndexStatistics indexStatistics = (OracleIndexStatistics) this.stats.get(relationName).getIndexStatistics(colNames);
				this.checkIndexStats.setSelected(false);
				this.textIndNumRows.setText(indexStatistics.getNumRows()+"");
				this.textLeafBlocks.setText(indexStatistics.getLeafBlocks()+"");
				this.textDistKeys.setText(indexStatistics.getDistinctKeys()+"");
				this.textAvgLeafBloclsPerKey.setText(indexStatistics.getAvgLeafBlocksPerKey()+"");
				this.textAvgDataBloclsPerKey.setText(indexStatistics.getAvgDataBlocksPerKey()+"");
				this.textClusterFactor.setText(indexStatistics.getClusteringFactor()+"");
				this.textIndexLevel.setText(indexStatistics.getIndLevel()+"");

				this.checkIndexStats.setEnabled(true);
				this.textIndNumRows.setEnabled(false);
				this.textLeafBlocks.setEnabled(false);
				this.textDistKeys.setEnabled(false);
				this.textAvgLeafBloclsPerKey.setEnabled(false);
				this.textAvgDataBloclsPerKey.setEnabled(false);
				this.textClusterFactor.setEnabled(false);
				this.textIndexLevel.setEnabled(false);
			}
			else
			{
				this.checkIndexStats.setSelected(false);
				this.textIndNumRows.setText("");
				this.textLeafBlocks.setText("");
				this.textDistKeys.setText("");
				this.textAvgLeafBloclsPerKey.setText("");
				this.textAvgDataBloclsPerKey.setText("");
				this.textClusterFactor.setText("");
				this.textIndexLevel.setText("");

				this.checkIndexStats.setEnabled(false);
				this.textIndNumRows.setEnabled(false);
				this.textLeafBlocks.setEnabled(false);
				this.textDistKeys.setEnabled(false);
				this.textAvgLeafBloclsPerKey.setEnabled(false);
				this.textAvgDataBloclsPerKey.setEnabled(false);
				this.textClusterFactor.setEnabled(false);
				this.textIndexLevel.setEnabled(false);
			}
		}
		else
		{
			// Column-Level
			this.textDistColValues.setText("");
			this.textNullCount.setText("");
			this.textAvgColLen.setText("");
			this.textDensity.setText("");

			// Column-Histogram-Level
			this.comboBoxHistogramType.setSelectedIndex(0);
			// Freq Hist
			//Remove 0th row freqHistRows times
			for (int r = 0; r < freqHistRows; r++) {
				((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
			}
			freqHistRows = OracleColumnStatistics.DefaultFrequencyBucketSize;
			textNumFreqBuckets.setText(""+freqHistRows);
			
			checkFreqHistWrite.setEnabled(false);
			checkFreqHistWrite.setSelected(false);
			textFreqHistWrite.setText("");
			textFreqHistWrite.setEnabled(false);

			String[] data = {"", ""};
			for (int r = 0; r < freqHistRows; r++) {
				((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
			}
			// Quant Hist
			//Remove 0th row quantHistRows times
			for (int r = 0; r < quantHistRows; r++) {
				((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(0);
			}
			quantHistRows = OracleColumnStatistics.DefaultHeightBalancedBucketSize;
			textNumQuantBuckets.setText(""+quantHistRows);
			checkQuantHistWrite.setEnabled(false);
			checkQuantHistWrite.setSelected(false);
			textQuantHistWrite.setText("");
			textQuantHistWrite.setEnabled(false);

			String[] data1 = {""};
			for (int r = 0; r < quantHistRows; r++) {
				((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data1);
			}
			// Index-Level
			this.checkIndexStats.setSelected(false);
			this.textIndNumRows.setText("");
			this.textLeafBlocks.setText("");
			this.textDistKeys.setText("");
			this.textAvgLeafBloclsPerKey.setText("");
			this.textAvgDataBloclsPerKey.setText("");
			this.textClusterFactor.setText("");
			this.textIndexLevel.setText("");
		}
	}

	/**
	 * Column-Index Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledColumnLevelMetadata(boolean enable)
	{
		// Column-Level
		this.textDistColValues.setEnabled(enable);
		this.textNullCount.setEnabled(enable);
		this.textAvgColLen.setEnabled(enable);
		this.textDensity.setEnabled(enable);

		// Column-Histogram-Level
		this.comboBoxHistogramType.setEnabled(enable);
		this.setEnableHistogram(enable);
		// Index-Level
		this.checkIndexStats.setEnabled(enable);
		this.textIndNumRows.setEnabled(enable);
		this.textLeafBlocks.setEnabled(enable);
		this.textDistKeys.setEnabled(enable);
		this.textAvgLeafBloclsPerKey.setEnabled(enable);
		this.textAvgDataBloclsPerKey.setEnabled(enable);
		this.textClusterFactor.setEnabled(enable);
		this.textIndexLevel.setEnabled(enable);

		this.buttonUpdateColumn.setEnabled(enable);
		this.buttonResetColValues.setEnabled(enable);
	}

	private void clearTableEntries() {
		// Frequency
		for (int i = 0; i < this.freqHistRows; i++) {
			((DefaultTableModel) tableFrqHist.getModel()).setValueAt("", i, 0);
			((DefaultTableModel) tableFrqHist.getModel()).setValueAt("", i, 1);
		}
		// Height Balanced
		for (int i = 0; i < this.quantHistRows; i++) {
			((DefaultTableModel) tableHeightBalancedHist.getModel()).setValueAt("", i, 0);
		}
	}

	private void setEnableHistogram(boolean enable)
	{
		boolean freqenable = false;
		boolean hbenable = false;
		// clear both table entries first
		clearTableEntries();
		if (enable && this.comboBoxHistogramType.getSelectedIndex() == 1) {
			freqenable = true;
			jTabbedPane1.setSelectedIndex(0);
		} else if (enable && this.comboBoxHistogramType.getSelectedIndex() == 2) {
			hbenable = true;
			jTabbedPane1.setSelectedIndex(1);
		}

		// Freq Hist
		this.textNumFreqBuckets.setEnabled(freqenable);
		this.buttonCreateFreqBuckets.setEnabled(freqenable);
		this.tableFrqHist.setEnabled(freqenable);
		this.checkFreqHistWrite.setEnabled(freqenable);
		this.buttonFreqhistUpload.setEnabled(freqenable);
		// Quant Hist
		this.textNumQuantBuckets.setEnabled(hbenable);
		this.buttonCreateQuantBuckets.setEnabled(hbenable);
		this.tableHeightBalancedHist.setEnabled(hbenable);
		this.checkQuantHistWrite.setEnabled(hbenable);
		this.buttonQuantHistUpload.setEnabled(hbenable);
		this.buttonQuantHistShowGraph.setEnabled(hbenable);
	}

	/**
	 * Updates the stats to the database.
	 * This is called at the end after validating all stats.
	 */
	private void updateStats()
	{
		Iterator<String> iter = this.stats.keySet().iterator();
		boolean success = true;
		while(iter.hasNext())
		{
			String rel = iter.next();
			Statistics stat = this.stats.get(rel);
			Constants.CPrintToConsole("Constructing Statistics for "+rel, Constants.DEBUG_SECOND_LEVEL_Information);
			Long cardinality = stat.getRelationStatistics().getCardinality().longValue();
			if(cardinality>0){
				success = stat.updateStatisticsToDatabase(database);				
			}
			if(!success){
				JOptionPane.showMessageDialog(null, "Statistics Construction for the relation " + rel + " failed.", "CODD - Message", JOptionPane.ERROR_MESSAGE);
				break;
			}
		}
		if (success) {
			JOptionPane.showMessageDialog(null, "Statistics Construction is Successful.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
		}
	}

	/**
	 * Read the current relation stats and validate with appropriate error message.
	 * If validation is success, update values into stats.
	 * @return
	 */
	private boolean validateCurrentRelationStats()
	{
		boolean valid = false;
		BigInteger card, blocks, avgrowlen;
		String relationName = this.getCurrentRelation();
		/**
		 * Node : Card (1)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		if(this.textCard.getText() == null || this.textCard.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Cardinality cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			card = new BigInteger(this.textCard.getText());
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
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Set the Relation Cardinality to some positive integer value, Otherwise the statistics for the selected relation will remain unchanged.");
			return valid;
		}

		/**
		 * Node : Blocks (2)
		 * Structural Constraints: >=0
		 * Consistency Constraints: (1) Blocks <= Card
		 */
		if(this.textBlocks.getText() == null || this.textBlocks.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Blocks cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			blocks = new BigInteger(this.textBlocks.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Blocks is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (blocks.compareTo(BigInteger.ZERO) >= 0) {
			if (blocks.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Blocks is greater than CARD.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Blocks value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Avg Row Len (3)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		if(this.textBlocks.getText() == null || this.textBlocks.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Avg Row Len cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			avgrowlen = new BigInteger(this.textAvgRowLen.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Avg Row Len is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (avgrowlen.compareTo(BigInteger.ZERO) >= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Avg Row Len value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		// valid must be true
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		Statistics stat = stats.get(relationName);
		OracleRelationStatistics oracleRelStat = (OracleRelationStatistics)stat.getRelationStatistics();
		oracleRelStat.setCardinality(new BigDecimal(card));
		oracleRelStat.setBlocks(new BigDecimal(blocks));
		oracleRelStat.setAvgRowLen(new BigDecimal(avgrowlen));
		stat.setRelationStatistics(oracleRelStat);
		stats.put(relationName, stat);

		return valid;
	}

	/**
	 * Read the current column stats and validate with appropriate error message.
	 * If validation is success, update values into stats.
	 * @return
	 */
	private boolean validateCurrentColumnStats()
	{
		boolean valid = false;

		String relationName = this.getCurrentRelation();
		String colName = this.getCurrentAttribute();
		Statistics stat = stats.get(relationName);
		OracleRelationStatistics oracleRelStat = (OracleRelationStatistics)stat.getRelationStatistics();
		OracleColumnStatistics oracleColStat = (OracleColumnStatistics)stat.getColumnStatistics(colName);
		Constraint constraint = oracleColStat.getConstraint();
		OracleIndexStatistics oracleIndexStat = null;
		int avg_col_len_char;
		BigDecimal card, col_card, num_nulls;
		double density;
		card = oracleRelStat.getCardinality();

		Statistics PKStat;
		OracleColumnStatistics oracle2PKColStat = null;
		try {
			String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, colName);
			String PKRelation = temp[0];
			String PKColumn = temp[1];
			// If col is FK, retrieve the PK information if chosen to construct
			if (constraint.isFK() && stats.containsKey(PKRelation)) {
				PKStat = stats.get(PKRelation);
				oracle2PKColStat = (OracleColumnStatistics)PKStat.getColumnStatistics(PKColumn);
			}
		} catch(Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}

		/**
		 * Node :  Distinct Column Values (4)
		 * Structural Constraints: >=0
		 * Consistency Constraints: (2) ColCard <= Card
		 * Attribute Constraints Checks: ColCard = Card, if Unique
		 * Inter table Consistency Constraints: ColCard <= PK.ColCard
		 */
		if(textDistColValues.getText() == null || textDistColValues.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Distinct Column Values can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			col_card = new BigDecimal(new BigInteger(textDistColValues.getText()));
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Distinct Column Values is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (col_card.compareTo(BigDecimal.ZERO) >= 0) {
			if (col_card.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Distinct Column Values must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(constraint.isUnique()) {
				if (col_card.equals(card)) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: Distinct Column Values must be equal to the Cardinality of the relation, since the column has a UNIQUE constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			if(constraint.isFK() && oracle2PKColStat != null) {
				if (col_card.compareTo(oracle2PKColStat.getColCard()) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: Distinct Column Values must be lesser than or equal to the Distinct Column Values of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid value in the field Distinct Column Values.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : NumNulls (5)
		 * Structural Constraints: >=0
		 * Consistency Constraints: (3) NumNulls <= Card
		 * Attribute Constraints Checks: ColCard = 0, if NotNULL
		 */
		if(textNullCount.getText() == null || textNullCount.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nNull Count can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			num_nulls = new BigDecimal(new BigInteger(textNullCount.getText().trim()));
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Null Count is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (num_nulls.compareTo(BigDecimal.ZERO) >= 0) {
			if (num_nulls.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Null Count must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(constraint.isNotNULL() ) {
				if (num_nulls.equals(BigDecimal.ZERO)) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: Null Count must be equal to 0, since the column has a NOTNULL constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Null Count value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Consistency Constraint: (4) col_card + num_nulls <= card
		 */
		BigDecimal sum = col_card.add(num_nulls);
		if (sum.compareTo(card) <= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Sum of Column Cardinality and Null Count must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Avg Col Len Char (6)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		if(textAvgColLen.getText() == null || textAvgColLen.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nAverage Column Length can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			avg_col_len_char = Integer.parseInt(this.textAvgColLen.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Avg Column Len Char is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (avg_col_len_char >= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Avg Column Len Char value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Density (7)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		try {
			density = Double.parseDouble(this.textDensity.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Density is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (density >= 0 && density <= 1.0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Density value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		String type = oracleColStat.getColumnType();
		OracleHistObject[] histogram = null;
		// Get histogram type
		String histType = (String)this.comboBoxHistogramType.getSelectedItem();
		int countQuant = 0;
		
		/**
		 * Node : Frequency Value Distribution (9)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: (5) Sum of ValCount's must be lesser than or equal to the cardinality.
		 *                          (6) Number of entries must be lesser than or equal to the Distinct Column Values.
		 */
		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,3,5,.. 39) ColValue - Data type must be same as column datatype
		 *                         Node (2,4,6... 40) -  >=0
		 * Consistency Constraints: NILL
		 */
		if(histType.equals(OracleColumnStatistics.Frequency)) {
			int countFreq = this.tableFrqHist.getModel().getRowCount();
			DataType[] colValuesFreq = new DataType[countFreq];
			Long[] valCountsFreq = new Long[countFreq];
			for(int i=0; i<countFreq; i++){
				String s1 = (String) this.tableFrqHist.getModel().getValueAt(i, 0);
				String s2 = (String) this.tableFrqHist.getModel().getValueAt(i, 1);
				if(s1 == null || s1.trim().isEmpty()){
					valid = false;
					JOptionPane.showMessageDialog(null, "Frequency Histogram - COLVALUE Value present in " + (i + 1) + "th row is empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				} else if(s2 == null || s2.trim().isEmpty()){
					valid = false;
					JOptionPane.showMessageDialog(null, "Frequency Histogram - VALCOUNT Value present in " + (i + 1) + "th row is empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;					
				}else{
					try {
						colValuesFreq[i] = new DataType(type, s1);
					} catch (Exception e) {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - COLVALUE present in " + (i + 1) + "th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					try {
						valCountsFreq[i] = Long.parseLong(s2);
						if(valCountsFreq[i]<=0){
							valid = false;
							JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - VALCOUNT present in " + (i + 1) + "th row should be greater than Zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
							return valid;							
						}
					} catch (NumberFormatException e) {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - VALCOUNT Value present in " + (i + 1) + "th row is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					i++;
				}
			}

			if(countFreq > 0){
				Long sumValCounts = 0L;
				for(int i=0;i<countFreq;i++)
				{
					sumValCounts = sumValCounts + valCountsFreq[i];
				}
				// Main Graph Consistency Constraints
				// (5) Sum of ValCount's must be lesser than or equal to the cardinality.
				BigDecimal sumValCountsBI = new BigDecimal(sumValCounts);
				if (sumValCountsBI.compareTo(card) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram - Sum of VALCOUNTs is greater than the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				// (6) Number of entries must be lesser than or equal to the column cardinality.
				BigDecimal countFreqBI = new BigDecimal(countFreq+"");
				if (countFreqBI.compareTo(col_card) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram - Number of entries in the Frequency Histogram is greater than the Column Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}

				// Everything is fine. Create the Map Frequency Histogram
				histogram = new OracleHistObject[countFreq];
				for (int i = 0; i < countFreq; i++) {
					String col = colValuesFreq[i].getString();
					String value = valCountsFreq[i]+"";
					OracleHistObject oracleHistObject = new OracleHistObject(col, value, "");
					histogram[i] = oracleHistObject;
				}
			}
		} 
		/**
		 * Node : Height Balanced Histogram (8)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: NILL             *
		 */
		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,2,3,.. 80) ColValue - Data type must be same as column datatype.
		 * Consistency Constraints: ColValue nodes must be increasing.
		 */		
		else if(histType.equalsIgnoreCase(OracleColumnStatistics.HeightBalanced)) {
			countQuant = this.tableHeightBalancedHist.getModel().getRowCount();
			DataType[] colValuesQuant = new DataType[countQuant];
			for(int i=0; i<countQuant; i++){
				String s1 = (String) tableHeightBalancedHist.getModel().getValueAt(i, 0);
				if (s1 != null && !s1.equals(this.emptyString)) {
					try {
						colValuesQuant[i] = new DataType(type, s1);
					} catch (Exception e) {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Height Balanced Histogram - COLVALUE present in " + (i + 1) + "th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					//i++;
				}else{
					valid = false;
					JOptionPane.showMessageDialog(null, "Height Balanced Histogram - COLVALUE present in " + (i + 1) + "th row is empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;					
				}
			}
			if (countQuant > 0) {
				DataType prevColValue = colValuesQuant[0];
				for (int i = 0; i < countQuant; i++) // Inner graph Structural Constraint
				{
					if (colValuesQuant[i].compare(prevColValue) >= 0) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Height Balanced Histogram - COLVALUE present in " + (i + 1) + "th row is lesser than its previous row value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					prevColValue = colValuesQuant[i];
				}

				// Everything is fine. Create the Map Quantile Histogram
				histogram = new OracleHistObject[countQuant];
				for (int i = 0; i < countQuant; i++) {
					String col = colValuesQuant[i].getString();
					OracleHistObject oracleHistObject = new OracleHistObject(col, "", "");
					histogram[i] = oracleHistObject;
				}
			}
		}



		String Str_IndexNumRows = this.textIndNumRows.getText();
		String Str_LeafBlocks = this.textLeafBlocks.getText();
		String Str_DistinctKeys = this.textDistKeys.getText();
		String Str_AvgLeafBlocksPerKey = this.textAvgLeafBloclsPerKey.getText();
		String Str_AvgDataBlocksPerKey = this.textAvgDataBloclsPerKey.getText();
		String Str_ClusterFactor = this.textClusterFactor.getText();
		String Str_IndexLevels = this.textIndexLevel.getText();
		//int  leaf_blocks = 0, index_level = 0, avgLeafBlocksPerKey = 0, avgDataBlocksPerKey = 0;
		BigDecimal leaf_blocks = new BigDecimal(BigDecimal.ZERO+"");
		BigDecimal index_level = new BigDecimal(BigDecimal.ZERO+"");
		BigDecimal avgLeafBlocksPerKey = new BigDecimal(BigDecimal.ZERO+"");
		BigDecimal avgDataBlocksPerKey = new BigDecimal(BigDecimal.ZERO+"");
		
		BigDecimal ind_num_rows = new BigDecimal(BigDecimal.ZERO+""), dist_keys = new BigDecimal(BigDecimal.ZERO+"");

		double clusterFactor = 0;
		if(this.checkIndexStats.isSelected())
		{
			/**
			 * Node : IndexNumRows (10)
			 * Structural Constraints: >=0
			 * Consistency Constraints: (7) IndNumRows = Card
			 */
			try {
				ind_num_rows = new BigDecimal(new BigInteger(Str_IndexNumRows));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Num Rows is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (ind_num_rows.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Num Blocks must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (ind_num_rows.equals(card)) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Num Rows is not same as Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : LeafBlocks (11)
			 * Structural Constraints: >=0
			 * Consistency Constraints: NILL
			 */
			try {
				leaf_blocks = new BigDecimal(new BigInteger(Str_LeafBlocks));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Leaf Blocks is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (leaf_blocks.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Leaf Blocks must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : DistinctKeys (12)
			 * Structural Constraints: >=0
			 * Consistency Constraints: NILL
			 */
			try {
				dist_keys = new BigDecimal(new BigInteger(Str_DistinctKeys));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Distinct Keys is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (dist_keys.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Distinct Keys must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : AvgLeafBlocksPerKey (13)
			 * Structural Constraints: >=0
			 * Consistency Constraints: NILL
			 */
			try {
				avgLeafBlocksPerKey = new BigDecimal(new BigInteger(Str_AvgLeafBlocksPerKey));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Avg Leaf Blocks Per Key is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (avgLeafBlocksPerKey.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Avg Leaf Blocks Per Key must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : AvgDataBlocksPerKey (14)
			 * Structural Constraints: >=0
			 * Consistency Constraints: NILL
			 */
			try {
				avgDataBlocksPerKey = new BigDecimal(new BigInteger(Str_AvgDataBlocksPerKey));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Avg Data Blocks Per Key is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (avgDataBlocksPerKey.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Avg Data Blocks Per Key must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : ClusteringFactor (15)
			 * Structural Constraints: 0 to 1
			 * Consistency Constraints: NILL
			 */
			try {
				clusterFactor = Double.parseDouble(Str_ClusterFactor);
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Clustering Factor value is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (clusterFactor >= 0 && clusterFactor <= 1) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Clustering Factor value must be in between 0 and 1.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : IndexLevels (16)
			 * Structural Constraints: >=0
			 * Consistency Constraints: NILL
			 */
			try {
				index_level = new BigDecimal(new BigInteger(Str_IndexLevels));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Levels is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (index_level.compareTo(BigDecimal.ZERO) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Levels must be greater than zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}
		// valid must be true
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		oracleColStat.setColCard(col_card);
		oracleColStat.setNumNulls(num_nulls);
		oracleColStat.setAvgColLen(avg_col_len_char);
		oracleColStat.setDensity(density);
		oracleColStat.setHistogramType(histType);
		oracleColStat.setOralceHistogram(histogram);
		oracleColStat.setNumBuckets(countQuant);

		stat.setColumnStatistics(colName, oracleColStat);

		if(this.checkIndexStats.isSelected())
		{
			String colNames = this.indexColumns.get(relationName).get(colName);
			if(colNames != null) {
				oracleIndexStat = (OracleIndexStatistics) stat.getIndexStatistics(colNames);
				oracleIndexStat.setNumRows(ind_num_rows);
				oracleIndexStat.setLeafBlocks(leaf_blocks);
				oracleIndexStat.setDistinctKeys(dist_keys);
				oracleIndexStat.setClusteringFactor(clusterFactor);
				oracleIndexStat.setAvgDataBlocksPerKey(avgDataBlocksPerKey);
				oracleIndexStat.setAvgLeafBlocksPerKey(avgLeafBlocksPerKey);
				oracleIndexStat.setIndLevel(index_level);
				stat.setIndexStatistics(colNames, oracleIndexStat);
			}
		}
		stats.put(relationName, stat);
		return valid;
	}
	
    /**
     * Listen to the text field.  This method detects when the
     * value of the text field (not necessarily the same
     * number as you'd get from getText) changes.
     */
    public void textCardPropertyChange(PropertyChangeEvent e) {
        if ("value".equals(e.getPropertyName())) {
        	BigInteger value = (BigInteger)e.getNewValue();
        	//BigDecimal value = (BigDecimal)
            if (cardSlider != null && value != null) {
            	//scaleFactor.setValue(value.intValue());
            	cardSlider.setValue((int) Math.log10(value.doubleValue()));
            }
        }
    }

	/** This method is called from within the constructor to
	 * initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is
	 * always regenerated by the Form Editor.
	 */
	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jPanel1 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		labelIndexStatsTip = new javax.swing.JLabel();
		labelIndexStats = new javax.swing.JLabel();
		labelHistograms = new javax.swing.JLabel();
		buttonQuantHistShowGraph = new javax.swing.JButton();
		comboBoxHistogramType = new javax.swing.JComboBox();
		jPanel2 = new javax.swing.JPanel();
		labelCard = new javax.swing.JLabel();
		textCard = new javax.swing.JTextField();
		textCard.addPropertyChangeListener(new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				textCardPropertyChange(evt);
			}
		});
		labelBlocks = new javax.swing.JLabel();
		textBlocks = new javax.swing.JTextField();
		labelAvgRowLen = new javax.swing.JLabel();
		textAvgRowLen = new javax.swing.JTextField();
		buttonUpdateRelation = new javax.swing.JButton();
		jPanel3 = new javax.swing.JPanel();
		labelDistColumnValues = new javax.swing.JLabel();
		textDistColValues = new javax.swing.JTextField();
		lableNullCount = new javax.swing.JLabel();
		textNullCount = new javax.swing.JTextField();
		labelAvgColLen = new javax.swing.JLabel();
		textAvgColLen = new javax.swing.JTextField();
		lableDensity = new javax.swing.JLabel();
		textDensity = new javax.swing.JTextField();
		jPanel4 = new javax.swing.JPanel();
		labelIndNumRows = new javax.swing.JLabel();
		textIndNumRows = new javax.swing.JTextField();
		labelLeafBlocks = new javax.swing.JLabel();
		textLeafBlocks = new javax.swing.JTextField();
		labelDistKeys = new javax.swing.JLabel();
		textDistKeys = new javax.swing.JTextField();
		labelAvgLeafBloclsPerKey = new javax.swing.JLabel();
		textAvgLeafBloclsPerKey = new javax.swing.JTextField();
		labelAvgDataBloclsPerKey = new javax.swing.JLabel();
		textAvgDataBloclsPerKey = new javax.swing.JTextField();
		labelClusterFactor = new javax.swing.JLabel();
		textClusterFactor = new javax.swing.JTextField();
		labelIndexLevel = new javax.swing.JLabel();
		textIndexLevel = new javax.swing.JTextField();
		checkIndexStats = new javax.swing.JCheckBox();
		Update = new javax.swing.JButton();
		buttonResetColValues = new javax.swing.JButton();
		jTabbedPane1 = new javax.swing.JTabbedPane();
		jPanel5 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net.png");
		jPanel7 = new javax.swing.JPanel();
		labelNumFreqBuckets = new javax.swing.JLabel();
		textNumFreqBuckets = new javax.swing.JTextField();
		buttonCreateFreqBuckets = new javax.swing.JButton();
		jSeparator4 = new javax.swing.JSeparator();
		buttonFreqhistUpload = new javax.swing.JButton();
		textFreqHistWrite = new javax.swing.JTextField();
		checkFreqHistWrite = new javax.swing.JCheckBox();
		jScrollPane2 = new javax.swing.JScrollPane();
		tableFrqHist = new javax.swing.JTable();
		jPanel6 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net.png");
		jPanel8 = new javax.swing.JPanel();
		labelNumQuantBuckets = new javax.swing.JLabel();
		textNumQuantBuckets = new javax.swing.JTextField();
		buttonCreateQuantBuckets = new javax.swing.JButton();
		jSeparator6 = new javax.swing.JSeparator();
		checkQuantHistWrite = new javax.swing.JCheckBox();
		textQuantHistWrite = new javax.swing.JTextField();
		buttonQuantHistUpload = new javax.swing.JButton();
		jScrollPane3 = new javax.swing.JScrollPane();
		tableHeightBalancedHist = new javax.swing.JTable();
		comboBoxRelations = new javax.swing.JComboBox();
		labelRelationName = new javax.swing.JLabel();
		comboBoxAttribute = new javax.swing.JComboBox();
		labelAttribute = new javax.swing.JLabel();
		jSeparator1 = new javax.swing.JSeparator();

		// Used to add the copy paste functionality in JTable.
		new ExcelAdapter(tableFrqHist);
		new ExcelAdapter(tableHeightBalancedHist);

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
		setTitle("Construct Mode - Oracle");

		jPanel1.setPreferredSize(new Dimension(1050, 680));

		labelIndexStatsTip.setFont(new java.awt.Font("Cambria", 1, 14));
		labelIndexStatsTip.setText("[There is a system generated index on this attribute.]");

		labelIndexStats.setFont(new java.awt.Font("Cambria", 1, 14));
		labelIndexStats.setText("Index Statistics:");

		labelHistograms.setFont(new java.awt.Font("Cambria", 0, 14));
		labelHistograms.setText("Histograms Type:");

		buttonQuantHistShowGraph.setFont(new Font("Dialog", Font.BOLD, 12));
		buttonQuantHistShowGraph.setText("Show Graph");
		buttonQuantHistShowGraph.setToolTipText("Graphically edit the histogram.");
		buttonQuantHistShowGraph.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistShowGraphActionPerformed(evt);
			}
		});

		comboBoxHistogramType.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "None", "Frequency Value", "Height Balanced"}));
		comboBoxHistogramType.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxHistogramTypeActionPerformed(evt);
			}
		});

		jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel2.setOpaque(false);

		labelCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelCard.setText("Cardinality:");
		labelCard.setToolTipText("Number of rows in the table.");

		textCard.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le CARD &le 9223372036854775807 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelBlocks.setFont(new java.awt.Font("Cambria", 0, 14));
		labelBlocks.setText("Blocks:");
		labelBlocks.setToolTipText("Number of used data blocks in the table.");

		textBlocks.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le BLOCKS &le 9223372036854775807 <br>  Consistency Constraint: <br> &nbsp; &nbsp; BLOCKS &le CARD </HTML>");

		labelAvgRowLen.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgRowLen.setText("Avg Row Len:");
		labelAvgRowLen.setToolTipText("Average length of a row in the table (in bytes).");

		textAvgRowLen.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Avg Col Len &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		buttonUpdateRelation.setText("Update");
		buttonUpdateRelation.setToolTipText("Updates Relation Level Metadata");
		buttonUpdateRelation.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateRelationActionPerformed(evt);
			}
		});
		
		cardSlider = new JSlider(JSlider.HORIZONTAL,
                FPS_MIN, 10, FPS_INIT);
		cardSlider.setPaintTicks(true);
		cardSlider.setPaintLabels(true);
		cardSlider.setMinorTickSpacing(1);
		cardSlider.setMajorTickSpacing(10);
				cardSlider.setEnabled(false);
				cardSlider.addChangeListener(new javax.swing.event.ChangeListener() {
				public void stateChanged(javax.swing.event.ChangeEvent evt) {
					cardSliderStateChanged(evt);
					}
				});

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
		jPanel2Layout.setHorizontalGroup(
			jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.TRAILING)
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addComponent(labelCard)
							.addGap(2)
							.addComponent(textCard, GroupLayout.DEFAULT_SIZE, 147, Short.MAX_VALUE))
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addGap(39)
							.addComponent(cardSlider, GroupLayout.PREFERRED_SIZE, 182, GroupLayout.PREFERRED_SIZE)
							.addGap(0, 0, Short.MAX_VALUE)))
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.TRAILING)
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(labelBlocks)
							.addGap(3)
							.addComponent(textBlocks, GroupLayout.DEFAULT_SIZE, 98, Short.MAX_VALUE)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(labelAvgRowLen)
							.addGap(3)
							.addComponent(textAvgRowLen, GroupLayout.PREFERRED_SIZE, 51, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addGap(147)
							.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 142, GroupLayout.PREFERRED_SIZE)))
					.addContainerGap())
		);
		jPanel2Layout.setVerticalGroup(
			jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelCard)
						.addComponent(textCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(textAvgRowLen, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(textBlocks, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelAvgRowLen)
						.addComponent(labelBlocks))
					.addGap(9)
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.TRAILING, false)
						.addComponent(buttonUpdateRelation)
						.addComponent(cardSlider, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap())
		);
		jPanel2.setLayout(jPanel2Layout);

		jPanel3.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel3.setOpaque(false);

		labelDistColumnValues.setFont(new java.awt.Font("Cambria", 0, 14));
		labelDistColumnValues.setText("Distinct Column Values: ");
		labelDistColumnValues.setToolTipText("Number of distinct values in the column.");

		textDistColValues.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Column Cardinality &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Column Cardinality &le Cardinality <br> &nbsp; &nbsp; Column Cardinality = Cardinality, for Unique column <br> &nbsp; &nbsp; Column Cardinality &le Primary Key Column Cardinality, for Foreign Key Column.  </HTML> ");

		lableNullCount.setFont(new java.awt.Font("Cambria", 0, 14));
		lableNullCount.setText("Null Count: ");
		lableNullCount.setToolTipText("Number of distinct values in the column.");

		textNullCount.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Null Count &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Null Count &le Cardinality <br> &nbsp; &nbsp; Null Count = 0, for NOTNULL column <br> &nbsp; &nbsp; Null Count + Column Cardinality &le Cardinality </HTML> ");

		labelAvgColLen.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgColLen.setText("Avg Col Len:");
		labelAvgColLen.setToolTipText("Average length of the column (in bytes).");

		textAvgColLen.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Avg Col Len Char &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		lableDensity.setFont(new java.awt.Font("Cambria", 0, 14));
		lableDensity.setText("Density:");
		lableDensity.setToolTipText("Density of the column");

		textDensity.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le Density &le 1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3Layout.setHorizontalGroup(
				jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(labelDistColumnValues)
								.addComponent(lableNullCount))
								.addPreferredGap(ComponentPlacement.RELATED)
								.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
										.addComponent(textNullCount, GroupLayout.DEFAULT_SIZE, 110, Short.MAX_VALUE)
										.addComponent(textDistColValues, GroupLayout.DEFAULT_SIZE, 110, Short.MAX_VALUE))
										.addPreferredGap(ComponentPlacement.RELATED)
										.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
												.addComponent(labelAvgColLen)
												.addComponent(lableDensity))
												.addPreferredGap(ComponentPlacement.RELATED)
												.addGroup(jPanel3Layout.createParallelGroup(Alignment.TRAILING, false)
														.addComponent(textDensity, GroupLayout.PREFERRED_SIZE, 61, GroupLayout.PREFERRED_SIZE)
														.addComponent(textAvgColLen, GroupLayout.PREFERRED_SIZE, 61, GroupLayout.PREFERRED_SIZE))
														.addContainerGap())
				);
		jPanel3Layout.setVerticalGroup(
				jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelDistColumnValues)
								.addComponent(textAvgColLen, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelAvgColLen)
								.addComponent(textDistColValues, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addPreferredGap(ComponentPlacement.RELATED)
								.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
										.addComponent(textDensity, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
										.addComponent(lableNullCount)
										.addComponent(textNullCount, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
										.addComponent(lableDensity))
										.addContainerGap(26, Short.MAX_VALUE))
				);
		jPanel3.setLayout(jPanel3Layout);

		jPanel4.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel4.setOpaque(false);

		labelIndNumRows.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndNumRows.setText("Num Rows:");
		labelIndNumRows.setToolTipText("Number of rows in the index");

		textIndNumRows.setToolTipText("<HTML> Datatype: NUMBER<br> Structural Constraint: <br> &nbsp; &nbsp; Index Num Rows &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Index Num Rows = Cardinality </HTML>");

		labelLeafBlocks.setFont(new java.awt.Font("Cambria", 0, 14));
		labelLeafBlocks.setText("Leaf Blocks:");
		labelLeafBlocks.setToolTipText("Number of leaf blocks in the index");

		textLeafBlocks.setToolTipText("<HTML> Datatype: Number<br> Structural Constraint: <br> &nbsp; &nbsp; Leaf Blocks &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelDistKeys.setFont(new java.awt.Font("Cambria", 0, 14));
		labelDistKeys.setText("Distinct Keys:");
		labelDistKeys.setToolTipText("Number of distinct keys in the index");

		textDistKeys.setToolTipText("<HTML> Datatype: Number<br> Structural Constraint: <br> &nbsp; &nbsp; Distinct Keys &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelAvgLeafBloclsPerKey.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgLeafBloclsPerKey.setText("Avg Leaf Blocks Per Key:");
		labelAvgLeafBloclsPerKey.setToolTipText("Average number of leaf blocks per key");

		textAvgLeafBloclsPerKey.setToolTipText("<HTML> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Avg Leaf Blocks per Key &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelAvgDataBloclsPerKey.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgDataBloclsPerKey.setText("Avg Data Blocks Per Key:");
		labelAvgDataBloclsPerKey.setToolTipText("Average number of data blocks per key");

		textAvgDataBloclsPerKey.setToolTipText("<HTML> Datatype: NUMBER<br> Structural Constraint: <br> &nbsp; &nbsp; Avg Data Blocks per Key &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelClusterFactor.setFont(new java.awt.Font("Cambria", 0, 14));
		labelClusterFactor.setText("ClusterFactor: ");
		labelClusterFactor.setToolTipText("<html> Indicates the amount of order <br>of the rows in the table <br> based on the values of the index.</html>");

		textClusterFactor.setToolTipText("<HTML> Datatype: NUMBER<br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le Clustering Factor &le 1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelIndexLevel.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndexLevel.setText("Index Level: ");
		labelIndexLevel.setToolTipText("Index Level");

		textIndexLevel.setToolTipText("<HTML> Datatype: NUMBER<br> Structural Constraint: <br> &nbsp; &nbsp; Index Level &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4Layout.setHorizontalGroup(
				jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel4Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel4Layout.createSequentialGroup()
										.addComponent(labelIndNumRows)
										.addPreferredGap(ComponentPlacement.UNRELATED)
										.addComponent(textIndNumRows, GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
										.addPreferredGap(ComponentPlacement.UNRELATED)
										.addComponent(labelLeafBlocks))
										.addGroup(jPanel4Layout.createSequentialGroup()
												.addComponent(labelAvgDataBloclsPerKey)
												.addPreferredGap(ComponentPlacement.UNRELATED)
												.addComponent(textAvgDataBloclsPerKey, GroupLayout.DEFAULT_SIZE, 110, Short.MAX_VALUE)))
												.addPreferredGap(ComponentPlacement.UNRELATED)
												.addGroup(jPanel4Layout.createParallelGroup(Alignment.LEADING)
														.addGroup(jPanel4Layout.createSequentialGroup()
																.addComponent(textLeafBlocks, GroupLayout.DEFAULT_SIZE, 109, Short.MAX_VALUE)
																.addPreferredGap(ComponentPlacement.UNRELATED)
																.addComponent(labelDistKeys)
																.addPreferredGap(ComponentPlacement.UNRELATED)
																.addComponent(textDistKeys, GroupLayout.DEFAULT_SIZE, 111, Short.MAX_VALUE)
																.addGap(30)
																.addComponent(labelAvgLeafBloclsPerKey)
																.addPreferredGap(ComponentPlacement.UNRELATED)
																.addComponent(textAvgLeafBloclsPerKey, GroupLayout.DEFAULT_SIZE, 107, Short.MAX_VALUE))
																.addGroup(jPanel4Layout.createSequentialGroup()
																		.addComponent(labelClusterFactor)
																		.addPreferredGap(ComponentPlacement.UNRELATED)
																		.addComponent(textClusterFactor, GroupLayout.DEFAULT_SIZE, 124, Short.MAX_VALUE)
																		.addGap(36)
																		.addComponent(labelIndexLevel)
																		.addPreferredGap(ComponentPlacement.UNRELATED)
																		.addComponent(textIndexLevel, GroupLayout.DEFAULT_SIZE, 131, Short.MAX_VALUE)
																		.addGap(147)))
																		.addGap(16))
				);
		jPanel4Layout.setVerticalGroup(
				jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addGroup(jPanel4Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelIndNumRows)
								.addComponent(textIndNumRows, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(textAvgLeafBloclsPerKey, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelAvgLeafBloclsPerKey)
								.addComponent(labelDistKeys)
								.addComponent(textDistKeys, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelLeafBlocks)
								.addComponent(textLeafBlocks, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addPreferredGap(ComponentPlacement.UNRELATED)
								.addGroup(jPanel4Layout.createParallelGroup(Alignment.BASELINE)
										.addComponent(labelAvgDataBloclsPerKey)
										.addComponent(textAvgDataBloclsPerKey, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
										.addComponent(labelClusterFactor)
										.addComponent(textClusterFactor, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
										.addComponent(labelIndexLevel)
										.addComponent(textIndexLevel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
										.addContainerGap())
				);
		jPanel4.setLayout(jPanel4Layout);

		checkIndexStats.setBackground(new java.awt.Color(247, 246, 235));
		checkIndexStats.setFont(new java.awt.Font("Tahoma", 0, 12));
		checkIndexStats.setText("Update index statistics (if it exists)");
		checkIndexStats.setOpaque(false);
		checkIndexStats.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkIndexStatsActionPerformed(evt);
			}
		});

		Update.setText("Construct");
		Update.setToolTipText("Input is over. Construct database.");
		Update.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				UpdateActionPerformed(evt);
			}
		});

		buttonResetColValues.setText("Reset Values");
		buttonResetColValues.setToolTipText("Resets the Column level metadata");
		buttonResetColValues.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonResetColValuesActionPerformed(evt);
			}
		});

		jPanel7.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel7.setOpaque(false);

		labelNumFreqBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		labelNumFreqBuckets.setText("Set the number of Buckets:");
		labelNumFreqBuckets.setEnabled(false);

		textNumFreqBuckets.setText(""+OracleColumnStatistics.DefaultFrequencyBucketSize);
		textNumFreqBuckets.setEnabled(false);

		buttonCreateFreqBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonCreateFreqBuckets.setText("Create");
		buttonCreateFreqBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateFreqBucketsActionPerformed(evt);
			}
		});

		buttonFreqhistUpload.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonFreqhistUpload.setText("Upload");
		buttonFreqhistUpload.setToolTipText("Upload Frequency Distribution from file.");
		buttonFreqhistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonFreqhistUploadActionPerformed(evt);
			}
		});

		textFreqHistWrite.setEnabled(false);

		checkFreqHistWrite.setBackground(new java.awt.Color(246, 247, 235));
		checkFreqHistWrite.setFont(new java.awt.Font("Tahoma", 0, 12));
		checkFreqHistWrite.setText("Write to File");
		checkFreqHistWrite.setToolTipText("Write Frequency Distribution to file.");
		checkFreqHistWrite.setOpaque(false);
		checkFreqHistWrite.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkFreqHistWriteActionPerformed(evt);
			}
		});

		JSeparator jSeparator2 = new JSeparator();

		javax.swing.GroupLayout jPanel7Layout = new javax.swing.GroupLayout(jPanel7);
		jPanel7Layout.setHorizontalGroup(
			jPanel7Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
					.addGroup(jPanel7Layout.createParallelGroup(Alignment.TRAILING)
						.addGroup(jPanel7Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(jSeparator2, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE))
						.addGroup(jPanel7Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(jSeparator4, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)
								.addGroup(jPanel7Layout.createSequentialGroup()
									.addComponent(labelNumFreqBuckets)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(textNumFreqBuckets, GroupLayout.DEFAULT_SIZE, 43, Short.MAX_VALUE))))
						.addGap(42)
						.addGroup(jPanel7Layout.createSequentialGroup()
							.addContainerGap(125, Short.MAX_VALUE)
							.addComponent(buttonCreateFreqBuckets, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
						.addGroup(Alignment.LEADING, jPanel7Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(checkFreqHistWrite, GroupLayout.PREFERRED_SIZE, 123, GroupLayout.PREFERRED_SIZE)
								.addComponent(textFreqHistWrite, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)))
						.addGroup(Alignment.LEADING, jPanel7Layout.createSequentialGroup()
							.addGap(41)
							.addComponent(buttonFreqhistUpload, GroupLayout.PREFERRED_SIZE, 163, GroupLayout.PREFERRED_SIZE)))
					.addContainerGap())
		);
		jPanel7Layout.setVerticalGroup(
			jPanel7Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel7Layout.createSequentialGroup()
					.addGroup(jPanel7Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelNumFreqBuckets)
						.addComponent(textNumFreqBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(buttonCreateFreqBuckets, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jSeparator4, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(buttonFreqhistUpload, GroupLayout.DEFAULT_SIZE, 56, Short.MAX_VALUE)
					.addGap(14)
					.addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(checkFreqHistWrite)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(textFreqHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		jPanel7.setLayout(jPanel7Layout);

		tableFrqHist.setFont(new java.awt.Font("Tahoma", 0, 14));

		tableFrqHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"COLVAUE", "Frequency"
				}
				));
		for(int i=0;i<OracleColumnStatistics.DefaultFrequencyBucketSize;i++){
			String[] data = {"",""};
			((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
		}
		tableFrqHist.setToolTipText("<HTML> Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : Value whose type is same as column type <br>  &nbsp; &nbsp; VALCOUNT : NUMBER : VALCOUNT >= 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Sum (VALCOUNT's) <= Cardinality <br> &nbsp; &nbsp; Number of buckets <= Column Cardinality </HTML> ");
		jScrollPane2.setViewportView(tableFrqHist);

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5Layout.setHorizontalGroup(
				jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel7, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addGap(18)
						.addComponent(jScrollPane2, GroupLayout.DEFAULT_SIZE, 680, Short.MAX_VALUE)
						.addContainerGap())
				);
		jPanel5Layout.setVerticalGroup(
				jPanel5Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(Alignment.LEADING, jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel5Layout.createSequentialGroup()
										.addComponent(jPanel7, GroupLayout.PREFERRED_SIZE, 244, GroupLayout.PREFERRED_SIZE)
										.addContainerGap())
										.addGroup(jPanel5Layout.createSequentialGroup()
												.addComponent(jScrollPane2, GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE)
												.addGap(19))))
				);
		jPanel5.setLayout(jPanel5Layout);

		jTabbedPane1.addTab("Frequency Value", jPanel5);

		jPanel8.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel8.setOpaque(false);

		labelNumQuantBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		labelNumQuantBuckets.setText("Set the number of Buckets:");
		labelNumQuantBuckets.setEnabled(false);

		textNumQuantBuckets.setText(""+OracleColumnStatistics.DefaultHeightBalancedBucketSize);
		textNumQuantBuckets.setEnabled(false);

		buttonCreateQuantBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonCreateQuantBuckets.setText("Create ");
		buttonCreateQuantBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateQuantBucketsActionPerformed(evt);
			}
		});

		checkQuantHistWrite.setBackground(new java.awt.Color(247, 246, 235));
		checkQuantHistWrite.setFont(new java.awt.Font("Tahoma", 0, 12));
		checkQuantHistWrite.setText("Write to File");
		checkQuantHistWrite.setToolTipText("Write Height Balanced Distribution to file.");
		checkQuantHistWrite.setOpaque(false);
		checkQuantHistWrite.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkQuantHistWriteActionPerformed(evt);
			}
		});

		textQuantHistWrite.setEnabled(false);

		buttonQuantHistUpload.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonQuantHistUpload.setText("Upload");
		buttonQuantHistUpload.setToolTipText("Upload Height Balanced Distribution from file.");
		buttonQuantHistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistUploadActionPerformed(evt);
			}
		});

		JSeparator jSeparator3 = new JSeparator();

		javax.swing.GroupLayout jPanel8Layout = new javax.swing.GroupLayout(jPanel8);
		jPanel8Layout.setHorizontalGroup(
			jPanel8Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel8Layout.createSequentialGroup()
					.addGroup(jPanel8Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel8Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel8Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(buttonCreateQuantBuckets, Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
								.addGroup(jPanel8Layout.createSequentialGroup()
									.addComponent(labelNumQuantBuckets)
									.addGap(18)
									.addComponent(textNumQuantBuckets, GroupLayout.PREFERRED_SIZE, 43, GroupLayout.PREFERRED_SIZE))
								.addComponent(jSeparator6, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)))
						.addGroup(jPanel8Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel8Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(checkQuantHistWrite)
								.addComponent(textQuantHistWrite, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)))
						.addGroup(jPanel8Layout.createSequentialGroup()
							.addGap(37)
							.addComponent(buttonQuantHistUpload, GroupLayout.PREFERRED_SIZE, 165, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel8Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(jSeparator3, GroupLayout.DEFAULT_SIZE, 233, Short.MAX_VALUE)))
					.addContainerGap())
		);
		jPanel8Layout.setVerticalGroup(
			jPanel8Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel8Layout.createSequentialGroup()
					.addGroup(jPanel8Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(textNumQuantBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelNumQuantBuckets))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(buttonCreateQuantBuckets, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jSeparator6, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(buttonQuantHistUpload, GroupLayout.DEFAULT_SIZE, 57, Short.MAX_VALUE)
					.addGap(12)
					.addComponent(jSeparator3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(10)
					.addComponent(checkQuantHistWrite)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(textQuantHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		jPanel8.setLayout(jPanel8Layout);

		tableHeightBalancedHist.setFont(new java.awt.Font("Tahoma", 0, 14));
		tableHeightBalancedHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"End Point Value"
				}
				));
		for(int i=0;i<OracleColumnStatistics.DefaultHeightBalancedBucketSize;i++){
			String[] data = {""};
			((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data);
		}
		tableHeightBalancedHist.setToolTipText("<HTML> Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : Value whose type is same as column type <br>  Consistency Constraint: <br> &nbsp; &nbsp; COLVALUE must be increasing. </HTML>");
		jScrollPane3.setViewportView(tableHeightBalancedHist);

		javax.swing.GroupLayout jPanel6Layout = new javax.swing.GroupLayout(jPanel6);
		jPanel6Layout.setHorizontalGroup(
				jPanel6Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel6Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel8, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addGap(18)
						.addComponent(jScrollPane3, GroupLayout.DEFAULT_SIZE, 680, Short.MAX_VALUE)
						.addContainerGap())
				);
		jPanel6Layout.setVerticalGroup(
				jPanel6Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(Alignment.LEADING, jPanel6Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel6Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel6Layout.createSequentialGroup()
										.addComponent(jPanel8, GroupLayout.PREFERRED_SIZE, 245, GroupLayout.PREFERRED_SIZE)
										.addContainerGap())
										.addGroup(jPanel6Layout.createSequentialGroup()
												.addComponent(jScrollPane3, GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE)
												.addGap(19))))
				);
		jPanel6.setLayout(jPanel6Layout);

		jTabbedPane1.addTab("Height Balanced", jPanel6);

		comboBoxRelations.setToolTipText("Selected Relations for Construt");
		comboBoxRelations.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxRelationsActionPerformed(evt);
			}
		});

		labelRelationName.setFont(new java.awt.Font("Cambria", 0, 14));
		labelRelationName.setText("Relation Name:");

		comboBoxAttribute.setToolTipText("Attributes of selected relation");
		comboBoxAttribute.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxAttributeActionPerformed(evt);
			}
		});

		labelAttribute.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAttribute.setText("Attribute Name:");
		buttonUpdateColumn = new javax.swing.JButton();

		buttonUpdateColumn.setText("Update Column");
		buttonUpdateColumn.setToolTipText("Updates the column level metadata");
		buttonUpdateColumn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateColumnActionPerformed(evt);
			}
		});

		JLabel lblyouCanConstruct = new JLabel("<html>- You can construct only one type of histogram.<br>\r\n- Choose the type of histogram, which you want to create, from the drop box.</html>");
		lblyouCanConstruct.setForeground(Color.RED);
		
		backButton = new JButton();
		backButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				backButtonActionPerformed(e);
			}
		});
		backButton.setToolTipText("");
		backButton.setText("<< Back");

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1Layout.setHorizontalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
							.addGroup(jPanel1Layout.createSequentialGroup()
								.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
									.addGroup(jPanel1Layout.createSequentialGroup()
										.addComponent(checkIndexStats)
										.addPreferredGap(ComponentPlacement.RELATED)
										.addComponent(labelIndexStats)
										.addPreferredGap(ComponentPlacement.RELATED)
										.addComponent(labelIndexStatsTip, GroupLayout.PREFERRED_SIZE, 454, GroupLayout.PREFERRED_SIZE))
									.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 1028, Short.MAX_VALUE)
									.addComponent(jPanel4, GroupLayout.DEFAULT_SIZE, 1028, Short.MAX_VALUE)
									.addGroup(jPanel1Layout.createSequentialGroup()
										.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
											.addGroup(jPanel1Layout.createSequentialGroup()
												.addComponent(labelRelationName)
												.addGap(18)
												.addComponent(comboBoxRelations, 0, 407, Short.MAX_VALUE))
											.addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, 530, Short.MAX_VALUE))
										.addPreferredGap(ComponentPlacement.RELATED)
										.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
											.addGroup(jPanel1Layout.createSequentialGroup()
												.addComponent(labelAttribute)
												.addPreferredGap(ComponentPlacement.UNRELATED)
												.addComponent(comboBoxAttribute, 0, 360, Short.MAX_VALUE))
											.addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, 486, Short.MAX_VALUE)))
									.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 1028, Short.MAX_VALUE))
								.addContainerGap())
							.addGroup(Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
								.addComponent(labelHistograms)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(comboBoxHistogramType, GroupLayout.PREFERRED_SIZE, 289, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.UNRELATED)
								.addComponent(lblyouCanConstruct, GroupLayout.PREFERRED_SIZE, 557, GroupLayout.PREFERRED_SIZE)
								.addContainerGap(43, Short.MAX_VALUE)))
						.addGroup(Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
							.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 100, GroupLayout.PREFERRED_SIZE)
							.addGap(12)
							.addComponent(buttonQuantHistShowGraph, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(buttonResetColValues)
							.addGap(12)
							.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 146, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
							.addContainerGap())))
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING, false)
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelRelationName)
								.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(jPanel2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelAttribute)
								.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 6, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(comboBoxHistogramType, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(labelHistograms))
						.addComponent(lblyouCanConstruct))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 333, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE, false)
						.addComponent(checkIndexStats)
						.addComponent(labelIndexStats)
						.addComponent(labelIndexStatsTip))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel4, GroupLayout.PREFERRED_SIZE, 72, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
							.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonQuantHistShowGraph, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)))
					.addGap(6))
		);
		jPanel1.setLayout(jPanel1Layout);

		getContentPane().add(jPanel1, BorderLayout.CENTER);

		pack();
	}// </editor-fold>//GEN-END:initComponents
	
	
	private void cardSliderStateChanged(javax.swing.event.ChangeEvent evt) {
        JSlider source = (JSlider)evt.getSource();
        int sf = (int)source.getValue();
    	BigDecimal val = new BigDecimal("0");
    	double cardValue = 0.0;
    	if(textCard.getText().equals(""))
    		cardValue = 1.0;
    	else
    		cardValue = Double.parseDouble(textCard.getText());
        if (!source.getValueIsAdjusting()) { //done adjusting
        	//val = new BigDecimal(Math.pow(10, sf));
       // 	if(textCard.getText().equals(""))
        		val = new BigDecimal(cardValue * Math.pow(10, sf));
        	 	textCard.setText(val.toPlainString());
       // 	else
        //		val = new BigDecimal(Double.parseDouble(textCard.getText()) * sf);
        } else { //value is adjusting; just set the text
        //	if(textCard.getText().equals(""))
        		val = new BigDecimal(cardValue * Math.pow(10, sf));
      //  	else
        //		val = new BigDecimal(Double.parseDouble(textCard.getText()) * sf);
       // 	textCard.setText(val.toPlainString());
        }
	}

	private void checkIndexStatsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkIndexStatsActionPerformed
		if (checkIndexStats.isSelected()) {
			this.textIndNumRows.setEnabled(true);
			this.textLeafBlocks.setEnabled(true);
			this.textDistKeys.setEnabled(true);
			this.textAvgLeafBloclsPerKey.setEnabled(true);
			this.textAvgDataBloclsPerKey.setEnabled(true);
			this.textClusterFactor.setEnabled(true);
			this.textIndexLevel.setEnabled(true);
		} else {
			this.textIndNumRows.setEnabled(false);
			this.textLeafBlocks.setEnabled(false);
			this.textDistKeys.setEnabled(false);
			this.textAvgLeafBloclsPerKey.setEnabled(false);
			this.textAvgDataBloclsPerKey.setEnabled(false);
			this.textClusterFactor.setEnabled(false);
			this.textIndexLevel.setEnabled(false);
		}
	}//GEN-LAST:event_checkIndexStatsActionPerformed

	private void backButtonActionPerformed(java.awt.event.ActionEvent evt) {
		String[] relations = new String[relUpdated.size()];
		int i = 0;
		for(String rel : relUpdated.keySet()){
			relations[i] = rel;
			i++;
		}
		new ConstructMode(DBConstants.ORACLE , relations, database).setVisible(true);
		this.dispose();				
	}

	private void buttonQuantHistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistUploadActionPerformed
		String filePath;
		try {
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Oracle";
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
				Constants.CPrintToConsole(filePath, Constants.DEBUG_SECOND_LEVEL_Information);
				File myfile = new File(filePath);
				FileReader fr = new FileReader(myfile);
				BufferedReader br = new BufferedReader(fr);

				while(tableHeightBalancedHist.getModel().getRowCount()>0){
					((DefaultTableModel)tableHeightBalancedHist.getModel()).removeRow(0);
				}
				String line = br.readLine();
				int i = 0;
				while (line != null) {
					((DefaultTableModel)tableHeightBalancedHist.getModel()).addRow(new String[1]);
					String[] vals = line.split("[|]");
					tableHeightBalancedHist.getModel().setValueAt(vals[0], i, 0);
					Constants.CPrintToConsole(vals[0], Constants.DEBUG_SECOND_LEVEL_Information);
					i++;
					line = br.readLine();
				}
				this.quantHistRows = i;
				this.textNumQuantBuckets.setText(i+"");					
				if(br!=null){
					br.close();											
				}
			}
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
		}

	}//GEN-LAST:event_buttonQuantHistUploadActionPerformed

	private void buttonCreateFreqBucketsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonCreateFreqBucketsActionPerformed
		if(textNumFreqBuckets.getText().trim().isEmpty()){
			JOptionPane.showMessageDialog(null, "CODD Exception: Enter the number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			return;
		}
		int bucketSize;
		try{
			bucketSize = Integer.parseInt(textNumFreqBuckets.getText().trim());
		}catch(Exception e){
			JOptionPane.showMessageDialog(null, "CODD Exception: Enter the integer value in number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			return;
		}
		if (bucketSize >=0 && bucketSize <= OracleColumnStatistics.MaxFrequencyBucketSize) {
			if(this.freqHistRows > bucketSize) {
				// Remove the last entries
				int removeCnt = this.freqHistRows - bucketSize;
				for (int i = 0; i < removeCnt; i++) {
					// remove 0th row, removeCnt times.
					((DefaultTableModel) tableFrqHist.getModel()).removeRow(this.freqHistRows - i - 1);
				}
				this.freqHistRows = bucketSize;
			} else {
				int addCnt = bucketSize - this.freqHistRows;
				this.freqHistRows = bucketSize;
				String[] data;
				for (int i = 0; i < addCnt; i++) {
					data = new String[]{"",""};
					((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
				}
			}
			tableFrqHist.getColumnModel().setColumnSelectionAllowed(true);
		} else {
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+OracleColumnStatistics.MaxFrequencyBucketSize+"]." , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonCreateFreqBucketsActionPerformed

	private void buttonQuantHistShowGraphActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistShowGraphActionPerformed
		/**
		 * Read all the information from the table and show it in the graph.
		 */
		boolean db2Hist = false;
		boolean unique = false;
		boolean noDistinct = true;
		int count = this.tableHeightBalancedHist.getModel().getRowCount();
		Constants.CPrintToConsole("Count: " + count, Constants.DEBUG_FIRST_LEVEL_Information);
		int i = 0;
		String relationName = this.getCurrentRelation();
		String colName = this.getCurrentAttribute();
		String type = null;
		try{
			type = database.getType(relationName, colName);
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
		}
		String[] labels = new String[count];
		double[] f = new double[count];
		double[] d = new double[count];
		int realCnt = 0;

		while(count > 0)
		{
			String s1 = (String)tableHeightBalancedHist.getModel().getValueAt(i, 0);
			if( s1 != null && !s1.equals(this.emptyString))
			{
				labels[i] = s1;
				realCnt++;
				i++;
			}
			count--;
		}
		if(realCnt > 0 && validateCurrentColumnStats())
		{
			double prevValCount = 0;
			double prevDistCount = 0;
			for (i = 0; i < labels.length; i++) {
				double freq = 10 - prevValCount; // some constants to show up the buckets in the graph
				f[i] = freq;
				double dist = 10 - prevDistCount;
				d[i] = dist;
				Constants.CPrintToConsole("Bucket "+i+": valCount:" + freq+"  Distinct:"+dist, Constants.DEBUG_FIRST_LEVEL_Information);
			}
			double totalFreqCount = 0;
			double totalDistCount = 0;

			for (i = 0; i < labels.length; i++) {
				totalFreqCount = totalFreqCount + f[i];
				totalDistCount = totalDistCount + d[i];
			}
			ArrayList<BucketItem> buckets = new ArrayList<BucketItem>();
			double totalFreqPercent = 0;
			double totalDCPercent = 0;
			for (i = 0; i < labels.length; i++) {
				double freqPercent = (f[i] * 100.0) / totalFreqCount;
				DecimalFormat decimalFormat = new DecimalFormat("#.##");
				freqPercent = Double.valueOf(decimalFormat.format(freqPercent));
				totalFreqPercent = totalFreqPercent + freqPercent;

				double distinctCountPercent = 0;
				if(!noDistinct)
				{
					distinctCountPercent = (d[i] * 100.0) / totalDistCount;
				}
				decimalFormat = new DecimalFormat("#.##");
				distinctCountPercent = Double.valueOf(decimalFormat.format(distinctCountPercent));
				totalDCPercent = totalDCPercent + distinctCountPercent;

				if (i == labels.length - 1) { // adjust to 100.00
					double remainder = 100.00 - totalFreqPercent;
					freqPercent = freqPercent + remainder;
					totalFreqPercent = totalFreqPercent + remainder;
					freqPercent = Double.valueOf(decimalFormat.format(freqPercent));

					remainder = 100.00 - totalDCPercent;
					distinctCountPercent = distinctCountPercent + remainder;
					totalDCPercent = totalDCPercent + remainder;
					distinctCountPercent = Double.valueOf(decimalFormat.format(distinctCountPercent));

				}

				DataType val, prevVal;
				if(DataType.isDouble(type)) {
					val = new DataType(DataType.DOUBLE, "" + labels[i]);
					if (i == 0) {
						double min = Double.parseDouble(labels[i]) - d[i];
						//double min = -3.72;
						prevVal = new DataType(DataType.DOUBLE, "" + min);
					} else {
						prevVal = new DataType(DataType.DOUBLE, "" + labels[i - 1]);
					}
				}  else if(DataType.isInteger(type)) {
					val = new DataType(DataType.INTEGER, "" + labels[i]);
					if (i == 0) {
						int min = Integer.parseInt(labels[i]) - (int)d[i];
						prevVal = new DataType(DataType.INTEGER, "" + min);
					} else {
						prevVal = new DataType(DataType.INTEGER, "" + labels[i - 1]);
					}
				}  else if(DataType.isNumeric(type)) {
					val = new DataType(DataType.NUMERIC, "" + labels[i]);
					if (i == 0) {
						BigDecimal min = new BigDecimal(0.0);
			        	try
			        	{
			        		DecimalFormat bigDecimalFormat = new DecimalFormat();
			        		bigDecimalFormat.setParseBigDecimal(true);
			        		BigDecimal bd = (BigDecimal) bigDecimalFormat.parse(labels[i]);
			        		min = bd.subtract(BigDecimal.valueOf(d[i]));
			        		
			        	} catch (java.text.ParseException e)
			        	{
			        		Constants.CPrintErrToConsole(e);
			        	}
						prevVal = new DataType(DataType.NUMERIC, "" + min);
					} else {
						prevVal = new DataType(DataType.NUMERIC, "" + labels[i - 1]);
					}
				}  else {
					val = new DataType(DataType.VARCHAR, "" + labels[i]);
					if (i == 0) {
						String minStr =  " ";
						prevVal = new DataType(DataType.VARCHAR, "" + minStr);
					} else {
						prevVal = new DataType(DataType.VARCHAR, "" + labels[i - 1]);
					}
				}
				Constants.CPrintToConsole("(" + prevVal.getString() + ", " + val.getString() + " ):: " + f[i] + " (" + freqPercent + ")", Constants.DEBUG_FIRST_LEVEL_Information);
				BucketItem bucket = new BucketItem(prevVal, val, f[i], d[i], freqPercent, distinctCountPercent);
				buckets.add(bucket);
			}
			DataType minDT;
			DataType maxDT;
			DataType minWidth;
			if(DataType.isDouble(type)) {
				double min = Double.parseDouble(labels[0]) - d[0];
				//double min = -3.72;
				double max = Double.parseDouble(labels[labels.length-1]);
				minDT = new DataType(DataType.DOUBLE, "" + min);
				maxDT = new DataType(DataType.DOUBLE, "" + max);
				minWidth = new DataType(DataType.DOUBLE, "1");
			} else if(DataType.isInteger(type)) {
				int min = Integer.parseInt(labels[0]) - (int)d[0];
				int max = Integer.parseInt(labels[labels.length-1]);
				minDT = new DataType(DataType.INTEGER, "" + min);
				maxDT = new DataType(DataType.INTEGER, "" + max);
				minWidth = new DataType(DataType.INTEGER, "1");
			} else if(DataType.isNumeric(type)) {
				BigDecimal min = new BigDecimal(0.0);
				BigDecimal max = new BigDecimal(0.0);
				try
	        	{
	        		DecimalFormat bigDecimalFormat = new DecimalFormat();
	        		bigDecimalFormat.setParseBigDecimal(true);
	        		BigDecimal bd = (BigDecimal) bigDecimalFormat.parse(labels[0]);
	        		min = bd.subtract(BigDecimal.valueOf(d[0]));
	        		max = (BigDecimal) bigDecimalFormat.parse(labels[labels.length-1]);
	        		
	        	} catch (java.text.ParseException e)
	        	{
	        		Constants.CPrintErrToConsole(e);
	        	}
				minDT = new DataType(DataType.NUMERIC, "" + min);
				maxDT = new DataType(DataType.NUMERIC, "" + max);
				minWidth = new DataType(DataType.NUMERIC, "1");
			} else {
				String minStr =  " ";
				String maxStr = labels[labels.length-1];
				minDT = new DataType(DataType.VARCHAR, "" + minStr);
				maxDT = new DataType(DataType.VARCHAR, "" + maxStr);
				minWidth = null;
			}
			double minHeight = 1.0; // 1%
			// 1.0 * (totalFreqCount / 100.0); // 1% of totalRows

			//DataType minWidth = new DataType(DataType.INTEGER,"1");
			Integer IntegerTotalFreqCount = new Integer((int) totalFreqCount);
			Integer IntegerTotalDistinctCount = new Integer((int) totalDistCount);
			try
			{
				String[] PKKeys = database.getPrimaryKeyAttributes(relationName);
				int pk = 0;
				while (PKKeys != null && pk < PKKeys.length) {
					String temp = PKKeys[pk];
					Constants.CPrintToConsole(temp, Constants.DEBUG_FIRST_LEVEL_Information);
					if(temp.equalsIgnoreCase(colName))
					{
						unique = true;
					}
					pk++;
				}

			}catch(Exception e)
			{
				Constants.CPrintErrToConsole(e);
			}

			if(DataType.isDouble(type)) {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_ONLYBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.DOUBLE, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isInteger(type)) {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_ONLYBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.INTEGER, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isNumeric(type)) {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_ONLYBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.NUMERIC, db2Hist, unique, noDistinct).setVisible(true);
			} else {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_ONLYBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.VARCHAR, db2Hist, unique, noDistinct).setVisible(true);
			}
		}
		else if(realCnt <= 0) {
			JOptionPane.showMessageDialog(null, "No Values in the Table.\n" , "CODD - Error", 0);
		}

	}//GEN-LAST:event_buttonQuantHistShowGraphActionPerformed

	private void checkQuantHistWriteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkQuantHistWriteActionPerformed
		if (checkQuantHistWrite.isSelected()) {
			textQuantHistWrite.setEnabled(true);
		}
		else {
			textQuantHistWrite.setEnabled(false);
		}
	}//GEN-LAST:event_checkQuantHistWriteActionPerformed

	private void buttonCreateQuantBucketsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonCreateQuantBucketsActionPerformed
		if(textNumQuantBuckets.getText().trim().isEmpty()){
			JOptionPane.showMessageDialog(null, "CODD Exception: Enter the number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			return;
		}
		int bucketSize;
		try{
			bucketSize = Integer.parseInt(textNumQuantBuckets.getText().trim());
		}catch(Exception e){
			JOptionPane.showMessageDialog(null, "CODD Exception: Enter the integer value in number of buckets.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			return;
		}
		if (bucketSize >= 0 && bucketSize <= OracleColumnStatistics.MaxQuantileBucketSize) {
			if(this.quantHistRows > bucketSize) {
				// Remove the last entries
				int removeCnt = this.quantHistRows - bucketSize;
				for (int i = 0; i < removeCnt; i++) {
					// remove 0th row, removeCnt times.
					((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(this.quantHistRows - i - 1);
				}
				this.quantHistRows = bucketSize;
			} else {
				int addCnt = bucketSize - this.quantHistRows;
				this.quantHistRows = bucketSize;
				String[] data;
				for (int i = 0; i < addCnt; i++) {
					data = new String[]{""};
					((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data);
				}
			}
			tableHeightBalancedHist.getColumnModel().setColumnSelectionAllowed(true);
		} else {
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+OracleColumnStatistics.MaxQuantileBucketSize+"]." , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonCreateQuantBucketsActionPerformed

	private void buttonUpdateColumnActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonUpdateColumnActionPerformed
		if(validateCurrentColumnStats())
		{
			if (this.checkFreqHistWrite.isSelected()) {
				try {
					StringBuffer sb = new StringBuffer();
					String temp = "";
					for (int j = 0; j < this.freqHistRows; j++) {
						String colValue = (String) this.tableFrqHist.getModel().getValueAt(j, 0);
						String valCount = (String) this.tableFrqHist.getModel().getValueAt(j, 1);
						if (colValue != null && valCount != null) {
							temp += colValue + "|" + valCount + "\n";
							sb.append(temp);
							temp = "";
						}
					}
					String fileName = textFreqHistWrite.getText();
					if(fileName==null || fileName.trim().isEmpty()){
						fileName = "Frequency_" + this.getCurrentRelation() + "_" + this.getCurrentAttribute();
					}else{
						fileName = fileName.trim();
					}
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Oracle";
					File path = new File(pathName);
					if(!path.exists()) {
						path.mkdir();
					}
					String filePath = pathName + Constants.PathSeparator + fileName + ".hist" ;
					File file = new File(filePath);
					FileWriter writer = new FileWriter(file, false);
					writer.write(sb.toString());
					writer.flush();
					writer.close();
				} catch (Exception e) {
					Constants.CPrintErrToConsole(e);
				}
			}
			if (checkQuantHistWrite.isSelected()) {
				try {
					StringBuffer sb = new StringBuffer();
					String temp = "";
					for (int j = 0; j < this.quantHistRows; j++) {
						String colValue = (String) this.tableHeightBalancedHist.getModel().getValueAt(j, 0);
						if (colValue != null) {
							temp += colValue ;
							sb.append(temp);
							temp = "";
						}
					}
					String fileName = textQuantHistWrite.getText();
					if(fileName==null || fileName.trim().isEmpty()){
						fileName = "Quantile_" + this.getCurrentRelation() + "_" + this.getCurrentAttribute();
					}else{
						fileName = fileName.trim();
					}
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Oracle";
					File path = new File(pathName);
					if(!path.exists()) {
						path.mkdir();
					}
					String filePath = pathName + Constants.PathSeparator + fileName + ".hist" ;
					File file = new File(filePath);
					FileWriter writer = new FileWriter(file, false);
					writer.write(sb.toString());
					writer.flush();
					writer.close();
				} catch (Exception e) {
					Constants.CPrintErrToConsole(e);
				}
			}
			String relationName = this.getCurrentRelation();
			String columnName = this.getCurrentAttribute();
			this.updateableStats.get(relationName).put(columnName, true);	
		}
	}//GEN-LAST:event_buttonUpdateColumnActionPerformed

	private void buttonResetColValuesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonResetColValuesActionPerformed
		String col = this.getCurrentAttribute();
		this.InitializeColumnLevelMetadata(col);
	}//GEN-LAST:event_buttonResetColValuesActionPerformed

	private void checkFreqHistWriteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkFreqHistWriteActionPerformed
		if (checkFreqHistWrite.isSelected()) {
			textFreqHistWrite.setEnabled(true);
		} else {
			textFreqHistWrite.setEnabled(false);
		}
	}//GEN-LAST:event_checkFreqHistWriteActionPerformed

	private void buttonFreqhistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonFreqhistUploadActionPerformed
		String filePath;
		try{
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Oracle";
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
				Constants.CPrintToConsole(filePath, Constants.DEBUG_SECOND_LEVEL_Information);
				File myfile = new File(filePath);
				FileReader fr = new FileReader(myfile);
				BufferedReader br = new BufferedReader(fr);
				while(tableFrqHist.getModel().getRowCount()>0){
					((DefaultTableModel)tableFrqHist.getModel()).removeRow(0);
				}
				String line = br.readLine();
				int i = 0;
				while (line != null) {
					((DefaultTableModel)tableFrqHist.getModel()).addRow(new String[2]);
					String[] vals = line.split("[|]");
					tableFrqHist.getModel().setValueAt(vals[0], i, 0);
					tableFrqHist.getModel().setValueAt(vals[1], i, 1);
					Constants.CPrintToConsole(line, Constants.DEBUG_FIRST_LEVEL_Information);
					i++;
					line = br.readLine();
				}
				this.freqHistRows = i;
				this.textNumFreqBuckets.setText(i+"");					
				if(br!=null){
					br.close();											
				}
			}
		}catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: "+ e.getMessage() , "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_buttonFreqhistUploadActionPerformed

	private void comboBoxRelationsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboBoxRelationsActionPerformed
		if(populatingRels){
			return;
		}
		String rel = this.getCurrentRelation();
		if (rel!=null && !rel.trim().isEmpty()) {
			this.InitializeRelationLevelMetadata(rel);
			if(this.relUpdated.get(rel) == 0)
			{
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
			}
			else
			{
				// Relation statistics are updated already.
				this.comboBoxAttribute.setEnabled(true);
			}
			this.populateColumnsComboBox(rel);
			this.setEnabledColumnLevelMetadata(false);
			this.InitializeColumnLevelMetadata(null); // clear column level metadata
		}
		else
		{
			this.comboBoxAttribute.removeAllItems();
			this.comboBoxAttribute.setEnabled(false);
			this.setEnabledRelationLevelMetadata(false);
			this.setEnabledColumnLevelMetadata(false);
			this.InitializeRelationLevelMetadata(null);
			this.InitializeColumnLevelMetadata(null);
		}

	}//GEN-LAST:event_comboBoxRelationsActionPerformed

	private void buttonUpdateRelationActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonUpdateRelationActionPerformed
		if(validateCurrentRelationStats())
		{
			String rel = this.getCurrentRelation();
			this.relUpdated.put(rel, 1);
			this.setEnabledRelationLevelMetadata(false);
			this.comboBoxAttribute.setEnabled(true);
		}
	}//GEN-LAST:event_buttonUpdateRelationActionPerformed

	private void comboBoxAttributeActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboBoxAttributeActionPerformed
		if(populatingCols)
			return;
		String col = this.getCurrentAttribute();
		if(col != null && !col.trim().equals(this.emptyString))
		{
			String relationName = this.getCurrentRelation();
			OracleColumnStatistics columnStatistics = (OracleColumnStatistics) this.stats.get(relationName).getColumnStatistics(col);
			Constraint constraint = columnStatistics.getConstraint();
			try {
				String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, col);
				String PKRelation = temp[0];
				String PKColumn = temp[1];
				// If col is FK and the corresponding PK column is chosen (PK realtion is chosen from SelectFrame window) to construct, but not constructed yet, report to user.
				if (constraint.isFK() && updateableStats.containsKey(PKRelation) && updateableStats.get(PKRelation).get(PKColumn)==false) {
					JOptionPane.showMessageDialog(null, "The column chosen is a foriegn key. It can not be constructed untill the PK column" + PKColumn + " of " + PKRelation + ".", "CODD - Error", JOptionPane.ERROR_MESSAGE);
					this.comboBoxAttribute.setSelectedItem(this.emptyString);
					this.setEnabledColumnLevelMetadata(false);
					this.InitializeColumnLevelMetadata(null);
				} else {
					this.setEnabledColumnLevelMetadata(true);
					this.InitializeColumnLevelMetadata(col);
				}
			} catch(Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
		}
		else
		{
			this.setEnabledColumnLevelMetadata(false);
			this.InitializeColumnLevelMetadata(null);
		}
	}//GEN-LAST:event_comboBoxAttributeActionPerformed

	private void UpdateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_UpdateActionPerformed
		int status = JOptionPane.showConfirmDialog(null, "If you have chosen to construct without inputting or modifying all relations or columns,\nthen they are not validated for legal and consistency constraint.\n"
				+ "Do you want to continue with the construcion?", "Choose the option.", JOptionPane.YES_NO_OPTION);
		if (status == JOptionPane.YES_OPTION) {
			this.updateStats();
		}
	}//GEN-LAST:event_UpdateActionPerformed

	private void comboBoxHistogramTypeActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboBoxHistogramTypeActionPerformed
		setEnableHistogram(true);
	}//GEN-LAST:event_comboBoxHistogramTypeActionPerformed

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton Update;
	private javax.swing.JButton buttonCreateFreqBuckets;
	private javax.swing.JButton buttonCreateQuantBuckets;
	private javax.swing.JButton buttonFreqhistUpload;
	private javax.swing.JButton buttonQuantHistShowGraph;
	private javax.swing.JButton buttonQuantHistUpload;
	private javax.swing.JButton buttonResetColValues;
	private javax.swing.JButton buttonUpdateColumn;
	private javax.swing.JButton buttonUpdateRelation;
	private javax.swing.JCheckBox checkFreqHistWrite;
	private javax.swing.JCheckBox checkIndexStats;
	private javax.swing.JCheckBox checkQuantHistWrite;
	private javax.swing.JComboBox comboBoxAttribute;
	private javax.swing.JComboBox comboBoxHistogramType;
	private javax.swing.JComboBox comboBoxRelations;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel2;
	private javax.swing.JPanel jPanel3;
	private javax.swing.JPanel jPanel4;
	private javax.swing.JPanel jPanel5;
	private javax.swing.JPanel jPanel6;
	private javax.swing.JPanel jPanel7;
	private javax.swing.JPanel jPanel8;
	private javax.swing.JScrollPane jScrollPane2;
	private javax.swing.JScrollPane jScrollPane3;
	private javax.swing.JSeparator jSeparator1;
	private javax.swing.JSeparator jSeparator4;
	private javax.swing.JSeparator jSeparator6;
	private javax.swing.JTabbedPane jTabbedPane1;
	private javax.swing.JLabel labelAttribute;
	private javax.swing.JLabel labelAvgColLen;
	private javax.swing.JLabel labelAvgDataBloclsPerKey;
	private javax.swing.JLabel labelAvgLeafBloclsPerKey;
	private javax.swing.JLabel labelAvgRowLen;
	private javax.swing.JLabel labelBlocks;
	private javax.swing.JLabel labelCard;
	private javax.swing.JLabel labelClusterFactor;
	private javax.swing.JLabel labelDistColumnValues;
	private javax.swing.JLabel labelDistKeys;
	private javax.swing.JLabel labelHistograms;
	private javax.swing.JLabel labelIndNumRows;
	private javax.swing.JLabel labelIndexLevel;
	private javax.swing.JLabel labelIndexStats;
	private javax.swing.JLabel labelIndexStatsTip;
	private javax.swing.JLabel labelLeafBlocks;
	private javax.swing.JLabel labelNumFreqBuckets;
	private javax.swing.JLabel labelNumQuantBuckets;
	private javax.swing.JLabel labelRelationName;
	private javax.swing.JLabel lableDensity;
	private javax.swing.JLabel lableNullCount;
	private javax.swing.JTable tableFrqHist;
	private javax.swing.JTable tableHeightBalancedHist;
	private javax.swing.JTextField textAvgColLen;
	private javax.swing.JTextField textAvgDataBloclsPerKey;
	private javax.swing.JTextField textAvgLeafBloclsPerKey;
	private javax.swing.JTextField textAvgRowLen;
	private javax.swing.JTextField textBlocks;
	private javax.swing.JTextField textCard;
	private javax.swing.JTextField textClusterFactor;
	private javax.swing.JTextField textDensity;
	private javax.swing.JTextField textDistColValues;
	private javax.swing.JTextField textDistKeys;
	private javax.swing.JTextField textFreqHistWrite;
	private javax.swing.JTextField textIndNumRows;
	private javax.swing.JTextField textIndexLevel;
	private javax.swing.JTextField textLeafBlocks;
	private javax.swing.JTextField textNumFreqBuckets;
	private javax.swing.JTextField textNumQuantBuckets;
	private javax.swing.JTextField textNullCount;
	private javax.swing.JTextField textQuantHistWrite;
	JButton backButton;
	private javax.swing.JSlider cardSlider;
	// End of variables declaration//GEN-END:variables

	@Override
	public void setHistogram(ArrayList<BucketItem> buckets, boolean noDistinct) {

		//Remove 0th row quantHistRows times
		for (int r = 0; r < quantHistRows; r++) {
			((DefaultTableModel) tableHeightBalancedHist.getModel()).removeRow(0);
		}
		this.quantHistRows = buckets.size();
		this.textNumQuantBuckets.setText(buckets.size()+"");
		for (int i = 0; i < buckets.size(); i++) {
			BucketItem bucket = buckets.get(i);
			Constants.CPrintToConsole("(" + bucket.getLValue().getString() + "," + bucket.getValue().getString() + ") :: " + bucket.getFreq() + " (" + bucket.getFreqPercent() + ")" + " : " + bucket.getDistinctCount() + " (" + bucket.getDistinctCountPercent() + ")", Constants.DEBUG_FIRST_LEVEL_Information);
			String colValue = bucket.getValue().getString();
			String[] data = {colValue};
			((DefaultTableModel) tableHeightBalancedHist.getModel()).addRow(data);
		}
	}
}
