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

import iisc.dsl.codd.client.ConstructMode;
import iisc.dsl.codd.client.ConstructModeFrame;
import iisc.dsl.codd.client.gui.ExcelAdapter;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.HistogramObject;
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
import java.util.Map.Entry;

import javax.swing.JOptionPane;
import java.util.Set;
import java.util.TreeMap;
import javax.swing.JFileChooser;
import javax.swing.table.DefaultTableModel;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout;
import javax.swing.LayoutStyle.ComponentPlacement;
import java.awt.BorderLayout;
import javax.swing.ListSelectionModel;
import javax.swing.JSeparator;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

/**
 * Construct Mode Frame for DB2.
 * @author dsladmin
 */
public class DB2ConstructModeGUI extends ConstructModeFrame {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 8633149563444500362L;

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
	HashMap<String, HashMap<String,Boolean>> updateableStats;

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
	public DB2ConstructModeGUI(Database database, Statistics[] stats) {
		super("DB2 Construct Mode");
		quantHistRows = DB2ColumnStatistics.DefaultQuantileBucketSize;
		freqHistRows = DB2ColumnStatistics.DefaultFrequencyBucketSize;
		initComponents();
		setLocationRelativeTo(null);
		emptyString = "";
		this.database = database;
		this.stats = new HashMap<String, Statistics>();
		// Fill updateableStats.
		updateableStats = new HashMap<String, HashMap<String,Boolean>>();
		relUpdated = new HashMap<String, Integer>();
		indexColumns = new HashMap<String, HashMap<String, String>>();
		for(int i=0;i<stats.length;i++)
		{
			String relationName = stats[i].getRelationName();
			this.stats.put(relationName, stats[i]);
			relUpdated.put(relationName, 0);
			HashMap<String,Boolean> columns = new HashMap<String, Boolean>();
			try
			{
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
	private void populateRelationsComboBox() {
		populatingRels = true;
		this.comboBoxRelations.removeAllItems();
		this.comboBoxRelations.addItem(emptyString);
		Set<String> set = updateableStats.keySet();
		Iterator<String> iter = set.iterator();
		while(iter.hasNext()) {
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
			DB2RelationStatistics relationStatistics = (DB2RelationStatistics) this.stats.get(relationName).getRelationStatistics();
			this.textCard.setText(relationStatistics.getCardinality()+"");
			this.textNPages.setText(relationStatistics.getNPages()+"");
			this.textFPages.setText(relationStatistics.getFPages()+"");
			this.textOverflow.setText(relationStatistics.getOverflow()+"");
		}
		else
		{
			this.textCard.setText("");
			this.textNPages.setText("");
			this.textFPages.setText("");
			this.textOverflow.setText("");
		}
	}

	/**
	 * Relation Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledRelationLevelMetadata(boolean enable)
	{
		this.textCard.setEnabled(enable);
		this.textNPages.setEnabled(enable);
		this.textFPages.setEnabled(enable);
		this.textOverflow.setEnabled(enable);

		this.buttonUpdateRelation.setEnabled(enable);
	}

	/**
	 * Column-Index Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeColumnLevelMetadata(String columnName) {
		if(columnName != null)
		{
			String relationName = this.getCurrentRelation();

			DB2ColumnStatistics columnStatistics = (DB2ColumnStatistics) this.stats.get(relationName).getColumnStatistics(columnName);
			// Column-Level
			this.textColCard.setText(columnStatistics.getColCard() + "");
			this.textNullCount.setText(columnStatistics.getNumNulls() + "");
			this.textHigh2Key.setText(columnStatistics.getHigh2key() + "");
			this.textLow2Key.setText(columnStatistics.getLow2key() + "");
			this.textAvgColLenChar.setText(columnStatistics.getAvgColLen() + "");

			// Column-Histogram-Level
			// Freq Hist
			TreeMap<Integer, DB2FreqHistObject> map = (TreeMap<Integer, DB2FreqHistObject>)columnStatistics.getFrequencyHistogram();

			if (map != null) {
				Set<Entry<Integer, DB2FreqHistObject>> set = map.entrySet();
				//Remove 0th row freqHistRows times
				for (int r = 0; r < freqHistRows; r++) {
					((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
				}
				freqHistRows = set.size();
				this.textNumFreqBuckets.setText(freqHistRows+"");
				String[] data;
				for (int r = 0; r < freqHistRows; r++) {
					data = new String[]{"",""};
					((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
				}

				Iterator<Entry<Integer, DB2FreqHistObject>> i = set.iterator();
				while (i.hasNext()) {
					Map.Entry<Integer, DB2FreqHistObject> me = i.next();

					// <SEQNO, FreqHistObject(VALCOUNT,COLVALUE)> // Ordered by SEQNO
					Integer seqno = (Integer) me.getKey();
					DB2FreqHistObject freqHistObject = (DB2FreqHistObject) me.getValue();
					String colValue = freqHistObject.getColValue();
					BigDecimal valCount = freqHistObject.getValCount();
					if(colValue != null) {
						((DefaultTableModel) tableFrqHist.getModel()).setValueAt(colValue, seqno-1, 0);
						String valCountStr = ""+valCount;
						((DefaultTableModel) tableFrqHist.getModel()).setValueAt(valCountStr, seqno-1, 1);
					}
				}
			}
			else
			{
				//Remove 0th row freqHistRows times
				for (int r = 0; r < freqHistRows; r++) {
					((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
				}
				freqHistRows = DB2ColumnStatistics.DefaultFrequencyBucketSize;
				this.textNumFreqBuckets.setText(freqHistRows+"");
				String[] data;
				for (int r = 0; r < freqHistRows; r++) {
					data = new String[]{"", ""};
					((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
				}
			}
			this.textFreqHistWrite.setText("Frequency_" + this.getCurrentRelation() + "_" + this.getCurrentAttribute());
			this.checkFreqHistWrite.setEnabled(true);
			this.checkFreqHistWrite.setSelected(false);
			this.textFreqHistWrite.setEnabled(false);

			// Quant Hist
			TreeMap<Integer, HistogramObject> mapQHist = (TreeMap<Integer, HistogramObject>) columnStatistics.getQuantileHistogram();
			if (mapQHist != null) {
				Long prevValueCount = 0L;
				Long prevDistCount = 0L;

				Set<Entry<Integer, HistogramObject>> set = mapQHist.entrySet();
				//Remove 0th row quantHistRows times
				for (int r = 0; r < quantHistRows; r++) {
					((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
				}
				quantHistRows = set.size();
				this.textNumQuantBuckets.setText(quantHistRows+"");
				String[] data;
				for (int r = 0; r < quantHistRows; r++) {
					data = new String[]{"","",""};
					((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
				}
				Iterator<Entry<Integer, HistogramObject>> i = set.iterator();
				while (i.hasNext()) {
					Map.Entry<Integer, HistogramObject> me = i.next();
					Integer seqno = (Integer) me.getKey();
					HistogramObject histogramObject = (HistogramObject) me.getValue();
					String colValue = histogramObject.getColValue();
					Long valCount = histogramObject.getValCount().longValue() + prevValueCount;
					Long distCount = null;
					if(histogramObject.getDistCount() != null) {
						distCount = histogramObject.getDistCount().longValue();
					}
					if(colValue != null) {
						((DefaultTableModel) tableQuantHist.getModel()).setValueAt(colValue, seqno-1, 0);
						String valCountStr = ""+valCount;
						((DefaultTableModel) tableQuantHist.getModel()).setValueAt(valCountStr, seqno-1, 1);
						if (distCount != null && prevDistCount != null) //If DISTCOUNT is present
						{
							distCount = distCount + prevDistCount;
							String distCountStr = ""+distCount;
							((DefaultTableModel) tableQuantHist.getModel()).setValueAt(distCountStr, seqno-1, 2);
						}
						prevValueCount = valCount;
						prevDistCount = distCount;
					}
				}
			}
			else
			{
				//Remove 0th row quantHistRows times
				for (int r = 0; r < quantHistRows; r++) {
					((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
				}
				quantHistRows = DB2ColumnStatistics.DefaultQuantileBucketSize;
				this.textNumQuantBuckets.setText(quantHistRows+"");
				String[] data;
				for (int r = 0; r < quantHistRows; r++) {
					data = new String[]{"", ""};
					((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
				}

			}
			this.textQuantHistWrite.setText("Quantile_" + this.getCurrentRelation() + "_" + this.getCurrentAttribute());
			this.checkQuantHistWrite.setEnabled(true);
			this.checkQuantHistWrite.setSelected(false);
			this.textQuantHistWrite.setEnabled(false);

			// Index-Level
			HashMap<String, String> indexCols = indexColumns.get(relationName);

			String colNames = indexCols.get(columnName);
			if(colNames != null)
			{
				DB2IndexStatistics indexStatistics = (DB2IndexStatistics) this.stats.get(relationName).getIndexStatistics(colNames);
				this.checkIndexStats.setSelected(false);
				this.textIndCard.setText(indexStatistics.getIndCard()+"");
				this.textNLeaf.setText(indexStatistics.getnLeaf()+"");
				this.textNLevels.setText(indexStatistics.getnLevels()+"");
				this.textDensity.setText(indexStatistics.getDensity()+"");
				this.textNumRID.setText(indexStatistics.getNumRIDs()+"");
				this.textNumEmptyLeafs.setText(indexStatistics.getNumEmptyLeafs()+"");

				this.checkIndexStats.setEnabled(true);
				this.textIndCard.setEnabled(false);
				this.textNLeaf.setEnabled(false);
				this.textNLevels.setEnabled(false);
				this.textDensity.setEnabled(false);
				this.textNumRID.setEnabled(false);
				this.textNumEmptyLeafs.setEnabled(false);
			}
			else
			{
				this.checkIndexStats.setSelected(false);
				this.textIndCard.setText("");
				this.textNLeaf.setText("");
				this.textNLevels.setText("");
				this.textDensity.setText("");
				this.textNumRID.setText("");
				this.textNumEmptyLeafs.setText("");

				this.checkIndexStats.setEnabled(false);
				this.textIndCard.setEnabled(false);
				this.textNLeaf.setEnabled(false);
				this.textNLevels.setEnabled(false);
				this.textDensity.setEnabled(false);
				this.textNumRID.setEnabled(false);
				this.textNumEmptyLeafs.setEnabled(false);
			}
		}
		else
		{
			// Column-Level
			this.textColCard.setText("");
			this.textNullCount.setText("");
			this.textHigh2Key.setText("");
			this.textLow2Key.setText("");
			this.textAvgColLenChar.setText("");

			// Column-Histogram-Level
			// Freq Hist
			//Remove 0th row freqHistRows times
			for (int r = 0; r < freqHistRows; r++) {
				((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
			}
			freqHistRows = DB2ColumnStatistics.DefaultFrequencyBucketSize;
			this.textNumFreqBuckets.setText(freqHistRows+"");
			String[] data;
			for (int r = 0; r < freqHistRows; r++) {
				data = new String[]{"", ""};
				((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
			}
			this.textFreqHistWrite.setText("");
			this.checkFreqHistWrite.setEnabled(false);
			this.checkFreqHistWrite.setSelected(false);
			this.textFreqHistWrite.setText("");
			this.textFreqHistWrite.setEnabled(false);

			// Quant Hist
			//Remove 0th row quantHistRows times
			for (int r = 0; r < quantHistRows; r++) {
				((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
			}
			quantHistRows = DB2ColumnStatistics.DefaultQuantileBucketSize;
			this.textNumQuantBuckets.setText(quantHistRows+"");
			for (int r = 0; r < quantHistRows; r++) {
				data = new String[]{"", ""};
				((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
			}
			this.textQuantHistWrite.setText("");
			this.checkQuantHistWrite.setEnabled(false);
			this.checkQuantHistWrite.setSelected(false);
			this.textQuantHistWrite.setText("");
			this.textQuantHistWrite.setEnabled(false);

			// Index-Level
			this.checkIndexStats.setSelected(false);
			this.textIndCard.setText("");
			this.textNLeaf.setText("");
			this.textNLevels.setText("");
			this.textDensity.setText("");
			this.textNumRID.setText("");
			this.textNumEmptyLeafs.setText("");
		}
	}

	/**
	 * Column-Index Level Metadata
	 * enables/disables the components.
	 */
	public void setEnabledColumnLevelMetadata(boolean enable)
	{
		// Column-Level
		this.textColCard.setEnabled(enable);
		this.textNullCount.setEnabled(enable);
		this.textHigh2Key.setEnabled(enable);
		this.textLow2Key.setEnabled(enable);
		this.textAvgColLenChar.setEnabled(enable);
		this.buttonCreateFreqBuckets.setEnabled(enable);
		this.tableFrqHist.setEnabled(enable);
		this.checkFreqHistWrite.setEnabled(enable);
		this.buttonFreqhistUpload.setEnabled(enable);
		this.textNumFreqBuckets.setEnabled(enable);
		this.buttonCreateQuantBuckets.setEnabled(enable);
		this.tableQuantHist.setEnabled(enable);
		this.checkQuantHistWrite.setEnabled(enable);
		this.buttonQuantHistUpload.setEnabled(enable);
		this.buttonQuantHistShowGraph.setEnabled(enable);
		this.textNumQuantBuckets.setEnabled(enable);

		// Index-Level
		this.checkIndexStats.setEnabled(enable);
		this.textIndCard.setEnabled(enable);
		this.textNLeaf.setEnabled(enable);
		this.textNLevels.setEnabled(enable);
		this.textDensity.setEnabled(enable);
		this.textNumRID.setEnabled(enable);
		this.textNumEmptyLeafs.setEnabled(enable);

		this.buttonUpdateColumn.setEnabled(enable);
		this.buttonResetColValues.setEnabled(enable);
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
		BigInteger card, fpages, npages, overflow;;
		String relationName = this.getCurrentRelation();
		/**
		 * Node : Card (1)
		 * Datatype: BIGINT
		 * Structural Constraints: -1 or >=0 <= 9223372036854775807
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
		 * Node : NPages (2)
		 * Datatype: BIGINT
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: (1) NPages <= Card
		 */
		if(this.textNPages.getText() == null || this.textNPages.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in NPages cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			npages = new BigInteger(this.textNPages.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in NPages is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (npages.compareTo(new BigInteger("-1")) >= 0) {
			if (npages.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: NPAGES is greater than CARD.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid NPages value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : FPages (3)
		 * Datatype: BIGINT
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: (2) FPages >= NPages
		 */
		if(this.textFPages.getText() == null || this.textFPages.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in FPages cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			fpages = new BigInteger(this.textFPages.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in FPages is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (fpages.compareTo(new BigInteger("-1")) >= 0) {
			if (npages.compareTo(fpages) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: FPAGES is lesser than NPAGES.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid FPages value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Overflow (4)
		 * Datatype: BIGINT
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: NILL
		 */
		if(this.textFPages.getText() == null || this.textFPages.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Overflow cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			overflow = new BigInteger(this.textOverflow.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Overflow is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (overflow.compareTo(new BigInteger("-1")) >= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Overflow value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		// valid must be true
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		Statistics stat = stats.get(relationName);
		DB2RelationStatistics db2RelStat = (DB2RelationStatistics)stat.getRelationStatistics();
		db2RelStat.setCardinality(new BigDecimal(card));
		db2RelStat.setNPages(new BigDecimal(npages));
		db2RelStat.setFPages(new BigDecimal(fpages));
		db2RelStat.setOverflow(new BigDecimal(overflow));
		stat.setRelationStatistics(db2RelStat);
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
		DB2RelationStatistics db2RelStat = (DB2RelationStatistics)stat.getRelationStatistics();
		DB2ColumnStatistics db2ColStat = (DB2ColumnStatistics)stat.getColumnStatistics(colName);
		Constraint constraint = db2ColStat.getConstraint();
		DB2IndexStatistics db2IndexStat = null;
		int avg_col_len;
		BigDecimal card, col_card, num_nulls;
		card = db2RelStat.getCardinality();

		Statistics PKStat;
		DB2ColumnStatistics db2PKColStat = null;
		try {
			String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, colName);
			String PKRelation = temp[0];
			String PKColumn = temp[1];
			// If col is FK, retrieve the PK information if chosen to construct
			if (constraint.isFK() && stats.containsKey(PKRelation)) {
				PKStat = stats.get(PKRelation);
				db2PKColStat = (DB2ColumnStatistics)PKStat.getColumnStatistics(PKColumn);
			}
		} catch(Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}

		/**
		 * Node : ColCard (5)
		 * Datatype: BIGINT
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: (4) ColCard <= Card
		 * Attribute Constraints Checks: ColCard = Card, if Unique
		 * Inter table Consistency Constraints: ColCard <= PK.ColCard
		 */
		if(textColCard.getText() == null || textColCard.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Column Cardinality can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			col_card = new BigDecimal(new BigInteger(textColCard.getText().trim()));
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nValue present in Column Cardinality is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (col_card.compareTo(new BigDecimal("-1")) >= 0) {
			if (col_card.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: \nColumn Cardinality must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(constraint.isUnique() ) {
				if (col_card.equals(card)) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: \nColumn Cardinality must be equal to the Cardinality of the relation, since the column has a UNIQUE constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			if(constraint.isFK() && db2PKColStat != null ) {
				if (col_card.compareTo(db2PKColStat.getColCard()) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: \nColumn Cardinality must be lesser than or equal to the Column Cardinality of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nColumn Cardinality value should be either -1 or >=0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : NumNulls (6)
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: (5) NumNulls <= Card
		 * Attribute Constraints Checks: NumNulls = 0, if NotNULL
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
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nValue present in Null Count is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (num_nulls.compareTo(new BigDecimal("-1")) >= 0) {
			if (num_nulls.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: \nNull Count must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(constraint.isNotNULL()) {
				if (num_nulls.equals(BigDecimal.ZERO)) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: \nNull Count must be equal to 0, since the column has a NOTNULL constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nNull Count value should be either -1 or >=0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Consistency Constraint: (3) col_card + num_nulls <= card
		 */
		BigDecimal sum = col_card.add(num_nulls);
		if (sum.compareTo(card) <= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: \nSum of Column Cardinality and Null Count must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Avg Col Len (7)
		 * Structural Constraints: -1 or >=0
		 * Consistency Constraints: NILL
		 */
		if(textAvgColLenChar.getText() == null || textAvgColLenChar.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nAverage Column Length can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			avg_col_len = Integer.parseInt(textAvgColLenChar.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nValue present in Avg Column Length is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (avg_col_len >= -1) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nAverage Column Length value shuold be either -1 or >=0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		DataType high2key, low2key;
		/**
		 * Node : Low2Key(8), High2Key(9)
		 * Structural Constraints: Data type should be same as column data type / the value could be empty.
		 * Consistency Constraints: (10) High2Key > Low2Key if ColCard >3.
		 * Inter table Consistency Constraints: High2Key <= PK.High2Key and Low2Key >= PK.Low2Key
		 */
		String str_high2key = textHigh2Key.getText();
		String str_low2key = textLow2Key.getText();
		if(str_high2key==null){
			str_high2key = "";
		}else{
			str_high2key = str_high2key.trim();
		}
		if(str_low2key==null){
			str_low2key = "";
		}else{
			str_low2key = str_low2key.trim();
		}
		// Either both field should have values, or none of them.
		if((str_high2key.length() > 0 && str_low2key.length() > 0 ) || (str_high2key.length() == 0 && str_low2key.length() == 0 )) {
			valid=true;
			if((str_high2key.length() > 0 && str_low2key.length() > 0 )) {
				try {
					high2key = new DataType(db2ColStat.getColumnType(), str_high2key);
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nValue present in High2Key is not of Column Data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					low2key = new DataType(db2ColStat.getColumnType(), str_low2key);
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nValue present in Low2Key is not of Column Data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				BigDecimal three = new BigDecimal("3");
				if (col_card.compareTo(three) > 0) {
					if (high2key.compare(low2key) > 0) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: \nLow2Key must be lesser than the High2Key.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				} else {
					if (high2key.compare(low2key) >= 0) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: \nLow2Key must be lesser than equal to the High2Key, since Column Cardinality is lesser than 3.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				}
				if (constraint.isFK() && db2PKColStat != null) {
					// If PK Col Stats high2key has a valid value.
					DataType pk_High2Key = new DataType(db2ColStat.getColumnType(), db2PKColStat.getHigh2key().trim());
					if (pk_High2Key.getString().length() > 0 && high2key.compare(pk_High2Key) <= 0) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: \nHigh2Key must be lesser than or equal to the High2Key (" + pk_High2Key.getString() + ") of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					// If PK Col Stats low2key has a valid value.
					DataType pk_Low2Key = new DataType(db2ColStat.getColumnType(), db2PKColStat.getLow2key());
					if (pk_Low2Key.getString().length() > 0 && low2key.compare(pk_Low2Key) >= 0) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: \nLow2Key must be greater than or equal to the Low2Key (" + pk_Low2Key.getString() + ") of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				}
			} // End if, if high2key, low2key has some value in their respective fields
			else {
				// Both fields are empty;
				high2key = null; //new DataType(db2ColStat.getColumnType(), str_high2key);
				low2key = null; //new DataType(db2ColStat.getColumnType(), str_low2key);
			}
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nEither both high2key, low2key must be filled or both should be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Frequency Value Distribution (10)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: (7) Sum of ValCount's must be lesser than or equal to the cardinality.
		 *                          (9) Number of entries must be lesser than or equal to the column cardinality.
		 *                          (13,14 ) There can be one entry greater than high2key and one entry lesser than low2key.
		 */

		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,3,5,.. 19) ColValue - Data type must be same as column datatype
		 *                         Node (2,4,6... 20) -  -1 or >=0
		 * Consistency Constraints: ValCount nodes must be decreasing.
		 */

		int countFreq = this.tableFrqHist.getModel().getRowCount();
		String type = db2ColStat.getColumnType();
		DataType[] colValuesFreq = new DataType[countFreq];
		BigDecimal[] valCountsFreq = new BigDecimal[countFreq];
		for(int i=0;i<countFreq;i++){
			String s1 = (String) this.tableFrqHist.getModel().getValueAt(i, 0);
			String s2 = (String) this.tableFrqHist.getModel().getValueAt(i, 1);
			if (s1 != null && !s1.trim().equals(this.emptyString) && s2 != null && !s2.trim().equals(this.emptyString)) {
				s1=s1.trim();
				s2=s2.trim();
				try {
					colValuesFreq[i] = new DataType(type, s1);
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nFrequency Histogram COLVALUE present in "+(i+1)+"th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					valCountsFreq[i] = new BigDecimal(new BigInteger(s2));
					if(valCountsFreq[i].compareTo(BigDecimal.ZERO)<=0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nFrequency Histogram VALCOUNT Value present in "+(i+1)+"th row should be a positive value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nFrequency Histogram VALCOUNT Value present in "+(i+1)+"th row is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} else{
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Frequency Histogram - Enter all the values in "+(i+1)+"th row.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}
		TreeMap<Integer,DB2FreqHistObject> freqHist = null;

		if(countFreq > 0)
		{
			BigDecimal sumValCounts = BigDecimal.ZERO;
			int greaterThanHigh2Key = 0;
			int lesserThanLow2Key = 0;
			BigDecimal prevValue = valCountsFreq[0];
			for(int i=0;i<countFreq;i++)
			{
				sumValCounts = sumValCounts.add(valCountsFreq[i]);
				if(high2key != null && high2key.compare(colValuesFreq[i]) < 0)
				{
					greaterThanHigh2Key++;
				}
				if(low2key != null && low2key.compare(colValuesFreq[i]) > 0)
				{
					lesserThanLow2Key++;
				}

				// Inner graph Structural Constraint
				// VALCOUNT values must be in non-increasing order, which means that COLVALUE with highest frequency should be in first row of table.
				if (valCountsFreq[i].compareTo(prevValue) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram: \nVALCOUNT present in "+(i+1)+"th row is greater than its previous row value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				prevValue = valCountsFreq[i];
			}

			// Main Graph Consistency Constraints
			// (7) Sum of ValCount's must be lesser than or equal to the cardinality.
			if (sumValCounts.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram: \nSum of VALCOUNTs is greater than the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			// (9) Number of entries must be lesser than or equal to the column cardinality.
			BigDecimal countFreqBI = new BigDecimal(countFreq+"");
			if (countFreqBI.compareTo(col_card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram: \nNumber of entries in the Frequency Histogram is greater than the Column Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			// (13,14 ) There can be one entry greater than high2key and one entry lesser than low2key.
			if (low2key != null && lesserThanLow2Key <= 1) {
				valid = true;
			} else if (low2key != null) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram: \nThere are more than one row whose column value is lesser than low2key.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (high2key != null && greaterThanHigh2Key <= 1) {
				valid = true;
			} else if(high2key != null) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram: \nThere are more than one row whose column value is greater than high2key.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			// Everything is fine. Create the Map Frequency Histogram
			freqHist = new TreeMap<Integer, DB2FreqHistObject>();
			int seqno = 1;
			for(int i=0;i<countFreq;i++)
			{
				String col = colValuesFreq[i].getString();
				BigDecimal valCount = new BigDecimal(valCountsFreq[i]+"");
				DB2FreqHistObject freqHistObject = new DB2FreqHistObject(col,valCount);
				freqHist.put(seqno, freqHistObject);
				seqno++;
			}
		}

		/**
		 * Node : Quantile Value Distribution (11)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: (6) The largest COLVALUE's VALCOUNT must be equal to CARD.
		 *                          (8) The largest COLVALUE's DISTCOUNT must be equal to COLCARD.
		 *                          (11,12 ) There can be one unique entry greater than high2key and one unique entry lesser than low2key. (Optional)
		 *                          (15) The VALCOUNT of a Quantile Histogram bin b must be greater than the sum of VALCOUNTs in the Frequency Histogram whose COLVALUE is less than the b's COLVALUE.
		 */

		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,4,7,.. 28) ColValue - Data type must be same as column datatype.
		 *                         Node (2,5,8... 29) -  -1 or >=0
		 *                         Node (3,6,9... 30) -  -1 or >=0
		 * Consistency Constraints: ColValue, ValCount, DistCount nodes must be increasing. ith row distcount must be lesser than ith row valcount.
		 */
		BigDecimal minusOneBI = new BigDecimal("-1");
		int countQuant = this.tableQuantHist.getModel().getRowCount();
		DataType[] colValuesQuant = new DataType[countQuant];
		BigDecimal[] valCountsQuant = new BigDecimal[countQuant];
		BigDecimal[] distCountsQuant = new BigDecimal[countQuant];
		for(int i=0;i<countQuant;i++) {
			String s1 = (String) tableQuantHist.getModel().getValueAt(i, 0);
			String s2 = (String) tableQuantHist.getModel().getValueAt(i, 1);
			String s3 = (String) tableQuantHist.getModel().getValueAt(i, 2);

			if (s1 != null && !s1.trim().equals(this.emptyString) && s2 != null && !s2.trim().equals(this.emptyString) && s3 != null && !s3.trim().equals(this.emptyString)){
				s1 = s1.trim();
				s2 = s2.trim();
				s3 = s3.trim();
				try {
					colValuesQuant[i] = new DataType(type, s1);
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Quantile Histogram: \nCOLVALUE present in "+(i+1)+"th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					valCountsQuant[i] = new BigDecimal(new BigInteger(s2));
					if(valCountsQuant[i].compareTo(BigDecimal.ZERO)<0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nQuantile Histogram VALCOUNT Value present in "+(i+1)+"th row should be greater than or equal to zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Quantile Histogram: \nVALCOUNT Value present in "+(i+1)+"th row is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					distCountsQuant[i] = new BigDecimal(new BigInteger(s3.trim()));
					if(distCountsQuant[i].compareTo(BigDecimal.ZERO)<0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nQuantile Histogram DISTCOUNT Value present in "+(i+1)+"th row should be greater than or equal to zero.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					if(valCountsQuant[i].compareTo(distCountsQuant[i])<0){
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nQuantile Histogram VALCOUNT Value present in "+(i+1)+"th row should be greater than or equal to DISTCOUNT.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
					if((distCountsQuant[i].compareTo(BigDecimal.ZERO)==0) && (valCountsQuant[i].compareTo(BigDecimal.ZERO)>0))
					{
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nQuantile Histogram - DISTCOUNT Value present in " + (i + 1) + "th row should be greater than 0.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Quantile Histogram: \nDISTCOUNT Value present in "+(i+1)+"th row is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				if(constraint.isUnique() ) {
					if (valCountsQuant[i].equals(distCountsQuant[i])) {
						valid = true;
					} else {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Quantile Histogram: \nVALCOUNT must be equal to the DISTCOUNT in "+(i+1)+"th row, since the column has a UNIQUE constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				}
			} else{
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Quantile Histogram - Enter all the values in "+(i+1)+"th row.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}

		TreeMap<Integer, HistogramObject> quantHist = null;

		if(countQuant > 0)
		{

			int greaterThanHigh2Key = 0;
			int lesserThanLow2Key = 0;
			DataType lessThanLow2Key = null;
			DataType greatThanHigh2Key = null;
			DataType prevColValue = colValuesQuant[0];
			BigDecimal prevValue = new BigDecimal(BigDecimal.ZERO+"");
			BigDecimal prevDistCount = new BigDecimal(BigDecimal.ZERO+"");
			for(int i=0;i<countQuant;i++){

				if(high2key != null && high2key.compare(colValuesQuant[i]) < 0)
				{
					if(greatThanHigh2Key != null && greatThanHigh2Key.equals(colValuesQuant[i]))
					{
						// Do not increment
					}
					else
					{
						greaterThanHigh2Key++;
						if(greatThanHigh2Key == null)
						{
							greatThanHigh2Key = colValuesQuant[i];
						}
					}
				}
				if(low2key != null && low2key.compare(colValuesQuant[i]) > 0)
				{
					if(lessThanLow2Key != null && lessThanLow2Key.equals(colValuesQuant[i]))
					{
						// Do not increment
					}
					else
					{
						lesserThanLow2Key++;
						if(lessThanLow2Key == null)
						{
							lessThanLow2Key = colValuesQuant[i];
						}
					}
				}

				// Inner graph Structural Constraint
				if (colValuesQuant[i].compare(prevColValue) >= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nCOLVALUE present in "+(i+1)+"th row is lesser than its previous row value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				prevColValue = colValuesQuant[i];

				if (valCountsQuant[i].compareTo(prevValue) >= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nVALCOUNT present in "+(i+1)+"th row is lesser than its previous row value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				prevValue = valCountsQuant[i];

				// -1 represents, no distcount in the histogram
				if (distCountsQuant[i].equals(minusOneBI) || distCountsQuant[i].compareTo(prevDistCount) >= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nDISTCOUNT present in "+(i+1)+"th row is lesser than its previous row value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				prevDistCount = distCountsQuant[i];

				// -1 represents, no distcount in the histogram
				if (distCountsQuant[i].equals(minusOneBI) || distCountsQuant[i].compareTo(valCountsQuant[i]) <= 0){
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nDISTCOUNT present in "+(i+1)+"th row is greater than VALCOUNT.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}

			// Main Graph Consistency Constraints
			// (6) The largest COLVALUE's VALCOUNT must be equal to CARD.
			if (valCountsQuant[countQuant-1].equals(card)) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nThe largest COLVALUE's VALCOUNT is not equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			// (8) The largest COLVALUE's DISTCOUNT must be equal to COLCARD.
			if( !distCountsQuant[countQuant-1].equals(minusOneBI)) // -1 reprsents no distcount in the histogram
			{
				if (distCountsQuant[countQuant - 1].equals(col_card)) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nThe largest COLVALUE's DISTCOUNT is not equal to the Column Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			// (11, 12) There can be one unique entry greater than high2key and one unique entry lesser than low2key.
			if (low2key != null && lesserThanLow2Key <= 1) {
				valid = true;
			} else if(low2key != null) {
				/*
				int retValue = JOptionPane.showConfirmDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram - There are more than one row whose column value is lesser than low2key. Would you like to change it?", "CODD - Validation Warning", JOptionPane.YES_NO_OPTION);
				if(retValue == JOptionPane.YES_OPTION) {
					valid =false;
					return valid;
				}
				 */
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nThere are more than one row whose column value is lesser than low2key.", "CODD - Validation Warning", JOptionPane.INFORMATION_MESSAGE);
				return valid;
			}
			if (high2key != null && greaterThanHigh2Key <= 1) {
				valid = true;
			} else if(high2key != null) {
				/*
				int retValue = JOptionPane.showConfirmDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram - There are more than one row whose column value is greater than high2key. Would you like to change it?", "CODD - Validation Warning", JOptionPane.YES_NO_OPTION);
				if(retValue == JOptionPane.YES_OPTION) {
					valid =false;
					return valid;
				}
				 */
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \nThere are more than one row whose column value is greater than high2key.", "CODD - Validation Warning", JOptionPane.INFORMATION_MESSAGE);
				return valid;
			}

			// (15) The VALCOUNT of a Quantile Histogram bin b must be greater than the sum of VALCOUNTs in the Frequency Histogram whose COLVALUE is less than the b's COLVALUE.
			for(int i=0;i<countQuant;i++)
			{
				DataType colValueQuant = colValuesQuant[i];
				BigDecimal valCountQuant = valCountsQuant[i];
				BigDecimal sumValCountFreq = BigDecimal.ZERO;
				for(int j=0;j<countFreq;j++)
				{
					DataType colValueFreq = colValuesFreq[j];
					BigDecimal valCountFreq = valCountsFreq[j];
					if(colValueFreq.compare(colValueQuant) <= 0)
					{
						sumValCountFreq = sumValCountFreq.add(valCountFreq);
					}
				}
				if (sumValCountFreq.compareTo(valCountQuant) <= 0) {
					valid = true;
				} else {
					/*
					int retValue = JOptionPane.showConfirmDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram "+(i+1)+"th row ValCount is lesser than the sum of ValCounts of Freq Hist whose COLVALUE is lesser than Q-Hist "+(i+1)+"th row COLVALUE. Would you like to change it?", "CODD - Validation Warning", JOptionPane.YES_NO_OPTION);
					if (retValue == JOptionPane.YES_OPTION) {
						valid = false;
						return valid;
					}
					 */
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Quantile Histogram: \n"+(i+1)+"th row ValCount is lesser than the sum of ValCounts of Freq Hist whose COLVALUE is lesser than Q-Hist "+(i+1)+"th row COLVALUE.", "CODD - Validation Warning", JOptionPane.INFORMATION_MESSAGE);
					return valid;
				}

			}

			// Everything is fine. Create the Map Quantile Histogram
			quantHist = new TreeMap<Integer,HistogramObject>();
			int seqno = 1;
			BigDecimal prevValCount = BigDecimal.ZERO;
			BigDecimal prevDistCount1 = BigDecimal.ZERO;
			for(int i=0;i<countQuant;i++)
			{
				String col = colValuesQuant[i].getString();
				BigDecimal temp = new BigDecimal(valCountsQuant[i]+"");
				BigDecimal valCount = temp.subtract(prevValCount);
				prevValCount = new BigDecimal(valCountsQuant[i]+"");
				BigDecimal distCount = null;
				if(!distCountsQuant[i].equals(new BigDecimal("-1")))
				{
					BigDecimal temp1 = new BigDecimal(distCountsQuant[i]+"");
					distCount = temp1.subtract(prevDistCount1);
					prevDistCount1 = new BigDecimal(distCountsQuant[i]+"");
				}
				// Use Double Value, for DB2 only, the valcount is integer.
				HistogramObject histogramObject;
				if(distCount != null) {
					histogramObject = new HistogramObject(col,valCount.doubleValue(),distCount.doubleValue());
				} else {
					histogramObject = new HistogramObject(col,valCount.doubleValue(),null);
				}
				quantHist.put(seqno, histogramObject);
				seqno++;
			}
		}

		String Str_IndexCard = this.textIndCard.getText().trim();
		String Str_NLeaf = this.textNLeaf.getText().trim();
		String Str_NLevels = this.textNLevels.getText().trim();
		String Str_NumEmptyLeafs = this.textNumEmptyLeafs.getText().trim();
		String Str_NumRIDs = this.textNumRID.getText().trim();
		String Str_Density = this.textDensity.getText().trim();

		BigDecimal ind_card = new BigDecimal(minusOneBI+"");
		BigDecimal NLeaf = new BigDecimal(minusOneBI+"");
		BigDecimal NLevels = new BigDecimal(minusOneBI+"");
		//int NLeaf = -1, NLevels = -1;
		BigDecimal NumRID = new BigDecimal(minusOneBI+"");
		BigDecimal NumEmptyLeafs = new BigDecimal(minusOneBI+"");
		double density = -1, clusterFactor = -1;
		

		if(this.checkIndexStats.isSelected()){
			/**
			 * Node : IndCard (12)
			 * Structural Constraints: -1 or >=0
			 * Consistency Constraints: (18) IndCard = Card
			 */
			try {
				ind_card = new BigDecimal(new BigInteger(Str_IndexCard));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Cardinality is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (ind_card.equals(card)) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Cardinality is not same as Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : ClusterFactor (13)
			 * Structural Constraints: -1 or 0 to 1
			 * Consistency Constraints: NILL
			 */
			// Removed it, because it creates issues with PAGE_FETCH_PAIRS. Now we store default value -1.
			/*
			try {
				clusterFactor = Double.parseDouble(Str_ClusterFactor);
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Cluster Factor value is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (clusterFactor == -1 || ( clusterFactor >= 0 && clusterFactor <= 1)) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Cluster Factor value must be in between 0 and 1.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}*/

			/**
			 * Node : NumRIDs (15)
			 * Structural Constraints: -1 or >=0
			 * Consistency Constraints: (19) NumRIDs >= IndCard
			 */
			try {
				NumRID = new BigDecimal(new BigInteger(Str_NumRIDs));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NumRIDs is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (NumRID.compareTo(ind_card) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NumRIDs is lesser than Index Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : NLeaf (16)
			 * Structural Constraints: -1 or >=0
			 * Consistency Constraints: NILL
			 */
			try {
				NLeaf = new BigDecimal(new BigInteger(Str_NLeaf));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NLeaf is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (NLeaf.compareTo(minusOneBI) >= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NLeaf value is not a valid value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : NLevels (17)
			 * Structural Constraints: -1 or >=0
			 * Consistency Constraints: (22) NLevels <= NLeaf
			 */
			try {
				NLevels = new BigDecimal(new BigInteger(Str_NLevels));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NLevels is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (NLevels.compareTo(minusOneBI) >= 0 && NLevels.compareTo(NLeaf) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NLevel value must be lesser than or equal to the NLeaf.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : NumEmptyLeafs (18)
			 * Structural Constraints: -1 or >=0
			 * Consistency Constraints: (21) NumEmptyLeafs <= NLeaf
			 */
			try {
				NumEmptyLeafs = new BigDecimal(new BigInteger(Str_NumEmptyLeafs));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NumEmptyLeafs is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			BigDecimal NLeafBI = new BigDecimal(NLeaf+"");
			if (NumEmptyLeafs.compareTo(NLeafBI) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: NumEmptyLeafs value must be lesser than or equal to the NLeaf.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			/**
			 * Node : Density (19)
			 * Structural Constraints: -1 or 0 to 100
			 * Consistency Constraints: NILL
			 */
			try {
				density = Double.parseDouble(Str_Density);
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Density value is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (density == -1 || ( density >= 0 && density <= 100)) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Density value must be in between 0 and 100.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

		}
		// valid must be true
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		db2ColStat.setColCard(col_card);
		db2ColStat.setNumNulls(num_nulls);
		db2ColStat.setAvgColLen(avg_col_len);
		if(low2key != null) {
			db2ColStat.setLow2key(low2key.getString());
		} else {
			db2ColStat.setLow2key(new String(""));
		}
		if(high2key != null) {
			db2ColStat.setHigh2key(high2key.getString());
		} else {
			db2ColStat.setHigh2key(new String(""));
		}
		db2ColStat.setFrequencyHistogram(freqHist);
		db2ColStat.setHistogram(quantHist);
		stat.setColumnStatistics(colName, db2ColStat);

		if(this.checkIndexStats.isSelected())
		{
			String colNames = this.indexColumns.get(relationName).get(colName);
			if(colNames != null) {
				db2IndexStat = (DB2IndexStatistics) stat.getIndexStatistics(colNames);
				db2IndexStat.setIndCard(ind_card);
				db2IndexStat.setClusterFactor(clusterFactor);
				db2IndexStat.setNumRIDs(NumRID);
				db2IndexStat.setnLeaf(NLeaf);
				db2IndexStat.setnLevels(NLevels);
				db2IndexStat.setNumEmptyLeafs(NumEmptyLeafs);
				db2IndexStat.setDensity(density);
				stat.setIndexStatistics(colNames, db2IndexStat);
			}
		}
		stats.put(relationName, stat);
		return valid;
	}

	/** This method is called from within the constructor to
	 * initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is
	 * always regenerated by the Form Editor.
	 */
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jPanel1 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		checkIndexStats = new javax.swing.JCheckBox();
		labelIndexStatsTip = new javax.swing.JLabel();
		labelIndexStats = new javax.swing.JLabel();
		buttonQuantHistShowGraph = new javax.swing.JButton();
		jPanel2 = new javax.swing.JPanel();
		buttonUpdateRelation = new javax.swing.JButton();
		labelCard = new javax.swing.JLabel();
		textCard = new javax.swing.JTextField();
		labelOverflow = new javax.swing.JLabel();
		textOverflow = new javax.swing.JTextField();
		labelFPages = new javax.swing.JLabel();
		textFPages = new javax.swing.JTextField();
		labelNPages = new javax.swing.JLabel();
		textNPages = new javax.swing.JTextField();
		jPanel3 = new javax.swing.JPanel();
		labelColCard = new javax.swing.JLabel();
		textColCard = new javax.swing.JTextField();
		lableNullCount = new javax.swing.JLabel();
		textNullCount = new javax.swing.JTextField();
		labelHigh2Key = new javax.swing.JLabel();
		textHigh2Key = new javax.swing.JTextField();
		labelLow2Key = new javax.swing.JLabel();
		textLow2Key = new javax.swing.JTextField();
		labelAvgColLenChar = new javax.swing.JLabel();
		textAvgColLenChar = new javax.swing.JTextField();
		jTabbedPane1 = new javax.swing.JTabbedPane();
		jPanel4 = new javax.swing.JPanel();
		jScrollPane4 = new javax.swing.JScrollPane();
		tableFrqHist = new javax.swing.JTable();
		tableFrqHist.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		tableFrqHist.setFillsViewportHeight(true);
		tableFrqHist.setColumnSelectionAllowed(true);
		jPanel7 = new javax.swing.JPanel();
		labelNoFreqBuckets = new javax.swing.JLabel();
		textNumFreqBuckets = new javax.swing.JTextField();
		textNumFreqBuckets.setEnabled(false);
		textNumFreqBuckets.setText(DB2ColumnStatistics.DefaultFrequencyBucketSize + "");
		buttonCreateFreqBuckets = new javax.swing.JButton();
		checkFreqHistWrite = new javax.swing.JCheckBox();
		jSeparator6 = new javax.swing.JSeparator();
		textFreqHistWrite = new javax.swing.JTextField();
		buttonFreqhistUpload = new javax.swing.JButton();
		jPanel5 = new javax.swing.JPanel();
		jScrollPane3 = new javax.swing.JScrollPane();
		tableQuantHist = new javax.swing.JTable();
		tableQuantHist.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		tableQuantHist.setFillsViewportHeight(true);
		tableQuantHist.setColumnSelectionAllowed(true);
		jPanel8 = new javax.swing.JPanel();
		labelNumQuantBuckets = new javax.swing.JLabel();
		textNumQuantBuckets = new javax.swing.JTextField();
		buttonCreateQuantBuckets = new javax.swing.JButton();
		jSeparator7 = new javax.swing.JSeparator();
		checkQuantHistWrite = new javax.swing.JCheckBox();
		textQuantHistWrite = new javax.swing.JTextField();
		buttonQuantHistUpload = new javax.swing.JButton();
		jPanel6 = new javax.swing.JPanel();
		labelIndCard = new javax.swing.JLabel();
		textIndCard = new javax.swing.JTextField();
		labelNLeaf = new javax.swing.JLabel();
		textNLeaf = new javax.swing.JTextField();
		labelNLevels = new javax.swing.JLabel();
		textNLevels = new javax.swing.JTextField();
		labelDensity = new javax.swing.JLabel();
		textDensity = new javax.swing.JTextField();
		labelNumRID = new javax.swing.JLabel();
		textNumRID = new javax.swing.JTextField();
		labelNumEmptyLeafs = new javax.swing.JLabel();
		textNumEmptyLeafs = new javax.swing.JTextField();
		Update = new javax.swing.JButton();
		buttonResetColValues = new javax.swing.JButton();
		buttonUpdateColumn = new javax.swing.JButton();
		comboBoxRelations = new javax.swing.JComboBox();
		labelRelationName = new javax.swing.JLabel();
		labelAttribute = new javax.swing.JLabel();
		comboBoxAttribute = new javax.swing.JComboBox();
		// Used to add the copy paste functionality in JTable.
		new ExcelAdapter(tableFrqHist);
		new ExcelAdapter(tableQuantHist);

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

		jPanel1.setOpaque(false);

		checkIndexStats.setBackground(new java.awt.Color(247, 246, 235));
		checkIndexStats.setFont(new java.awt.Font("Tahoma", 0, 12));
		checkIndexStats.setText("Update index statistics (if it exists)");
		checkIndexStats.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkIndexStatsActionPerformed(evt);
			}
		});

		labelIndexStatsTip.setFont(new java.awt.Font("Cambria", 3, 14));
		labelIndexStatsTip.setText("[There is a system generated index on this attribute.]");

		labelIndexStats.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndexStats.setText("Index Statistics:");

		buttonQuantHistShowGraph.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonQuantHistShowGraph.setText("Show Quantile Value Graph");
		buttonQuantHistShowGraph.setToolTipText("Graphically edit the histogram.");
		buttonQuantHistShowGraph.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistShowGraphActionPerformed(evt);
			}
		});

		jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Please Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel2.setOpaque(false);

		buttonUpdateRelation.setText("Update");
		buttonUpdateRelation.setToolTipText("Updates Relation Level Metadata");
		buttonUpdateRelation.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateRelationActionPerformed(evt);
			}
		});

		labelCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelCard.setText("Cardinality:");
		labelCard.setToolTipText("Cardinality of the relation ");

		textCard.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or 0 &le CARD &le 9223372036854775807 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML>");

		labelOverflow.setFont(new java.awt.Font("Cambria", 0, 14));
		labelOverflow.setText("Overflow:");
		labelOverflow.setToolTipText("<html>\nAvg. Row Size (integer):\n<br />\nAverage length (in bytes) of both compressed and uncompressed rows in this table\n</html>");

		textOverflow.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or Overflow &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; None </HTML>");

		labelFPages.setFont(new java.awt.Font("Cambria", 0, 14));
		labelFPages.setText("FPages:");
		labelFPages.setToolTipText("<html>\nFPages (integer):\n<br />\nTotal number of pages.\n</html>");

		textFPages.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or FPAGES &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; FPAGES &ge NPAGES  </HTML>");

		labelNPages.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNPages.setText("NPages:");
		labelNPages.setToolTipText("<html>NPages (integer):\n<br>\nTotal number of pages on which the rows exist.\n\n</html>");

		textNPages.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or NPAGES &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NPAGES &le CARD </HTML>");

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
		jPanel2Layout.setHorizontalGroup(
				jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(labelCard)
						.addPreferredGap(ComponentPlacement.RELATED)
						.addComponent(textCard, GroupLayout.DEFAULT_SIZE, 153, Short.MAX_VALUE)
						.addGap(26)
						.addComponent(labelNPages)
						.addPreferredGap(ComponentPlacement.RELATED)
						.addComponent(textNPages, GroupLayout.DEFAULT_SIZE, 153, Short.MAX_VALUE)
						.addPreferredGap(ComponentPlacement.UNRELATED)
						.addComponent(labelOverflow)
						.addPreferredGap(ComponentPlacement.RELATED)
						.addComponent(textOverflow, GroupLayout.DEFAULT_SIZE, 153, Short.MAX_VALUE)
						.addPreferredGap(ComponentPlacement.UNRELATED)
						.addComponent(labelFPages)
						.addPreferredGap(ComponentPlacement.RELATED)
						.addComponent(textFPages, GroupLayout.DEFAULT_SIZE, 153, Short.MAX_VALUE)
						.addGap(77)
						.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 103, GroupLayout.PREFERRED_SIZE)
						.addContainerGap())
				);
		jPanel2Layout.setVerticalGroup(
				jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
						.addGroup(jPanel2Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelCard)
								.addComponent(textCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelNPages)
								.addComponent(textNPages, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelOverflow)
								.addComponent(textOverflow, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(labelFPages)
								.addComponent(textFPages, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addComponent(buttonUpdateRelation))
								.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
				);
		jPanel2.setLayout(jPanel2Layout);

		jPanel3.setBorder(javax.swing.BorderFactory.createTitledBorder(null, "Please Input Values", javax.swing.border.TitledBorder.DEFAULT_JUSTIFICATION, javax.swing.border.TitledBorder.DEFAULT_POSITION, new java.awt.Font("Cambria", 0, 14), new java.awt.Color(0, 102, 204))); // NOI18N
		jPanel3.setOpaque(false);

		labelColCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelColCard.setText("Cardinality (Distinct): ");
		labelColCard.setToolTipText("<html>\nColcard (integer):\n<br>\nNumber of distinct values in the column\n</html>\n");

		textColCard.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or Column Cardinality &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Column Cardinality &le Cardinality <br> &nbsp; &nbsp; Column Cardinality = Cardinality, for Unique column <br> &nbsp; &nbsp; Column Cardinality &le Primary Key Column Cardinality, for Foreign Key Column.  </HTML>");

		lableNullCount.setFont(new java.awt.Font("Cambria", 0, 14));
		lableNullCount.setText("Null Count: ");
		lableNullCount.setToolTipText("<html>\nNumnulls (integer):\n<br />\nNumber of null values in the column.\n</html>");

		textNullCount.setToolTipText("<HTML> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or Null Count &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Null Count &le Cardinality <br> &nbsp; &nbsp; Null Count = 0, for NOTNULL column <br> &nbsp; &nbsp; Null Count + Column Cardinality &le Cardinality </HTML>");

		labelHigh2Key.setFont(new java.awt.Font("Cambria", 0, 14));
		labelHigh2Key.setText("High2Key:");
		labelHigh2Key.setToolTipText("<html>\nHIGH2KEY (varchar):\n<br />\nSecond-highest data value. \n</html>");

		textHigh2Key.setToolTipText("<HTML> Datatype: VARCHAR(254) <br> Structural Constraint: <br> &nbsp; &nbsp; Empty String or Value whose type is same as column type <br>  Consistency Constraint: <br> &nbsp; &nbsp; High2Key  &le PK.High2Key, if Foreign Key Column </HTML>");

		labelLow2Key.setFont(new java.awt.Font("Cambria", 0, 14));
		labelLow2Key.setText("Low2Key:");
		labelLow2Key.setToolTipText("<html>\nLOW2KEY (varchar):\n<br />\nSecond-lowest data value. \n</html>");

		textLow2Key.setToolTipText("<HTML> Datatype: VARCHAR(254) <br> Structural Constraint: <br> &nbsp; &nbsp; Empty String or Value whose type is same as column type <br>  Consistency Constraint: <br> &nbsp; &nbsp; Low2Key  &ge PK.Low2Key, if Foreign Key Column <br> &nbsp; &nbsp; High2Key > Low2Key, if Column Cardinality > 3 </HTML>");

		labelAvgColLenChar.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgColLenChar.setText("Avg. Col. Len. :");
		labelAvgColLenChar.setToolTipText("<html> AVGCOLLEN (integer): <br /> Average space (in bytes) required for the column.  </html>");

		textAvgColLenChar.setToolTipText("<HTML> Datatype: INTEGER <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or Avg Col Len &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; None </HTML>");

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3Layout.setHorizontalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addContainerGap()
					.addComponent(labelColCard)
					.addGap(3)
					.addComponent(textColCard, GroupLayout.DEFAULT_SIZE, 125, Short.MAX_VALUE)
					.addGap(26)
					.addComponent(lableNullCount)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textNullCount, GroupLayout.DEFAULT_SIZE, 125, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelHigh2Key)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textHigh2Key, GroupLayout.DEFAULT_SIZE, 125, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelLow2Key)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textLow2Key, GroupLayout.DEFAULT_SIZE, 125, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelAvgColLenChar)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textAvgColLenChar, GroupLayout.PREFERRED_SIZE, 125, GroupLayout.PREFERRED_SIZE)
					.addGap(15))
		);
		jPanel3Layout.setVerticalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(labelAvgColLenChar)
							.addComponent(textAvgColLenChar, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(labelColCard)
							.addComponent(textColCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(lableNullCount)
							.addComponent(textNullCount, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(labelHigh2Key)
							.addComponent(textHigh2Key, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(labelLow2Key)
							.addComponent(textLow2Key, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
					.addContainerGap(17, Short.MAX_VALUE))
		);
		jPanel3.setLayout(jPanel3Layout);

		jPanel4.setOpaque(false);

		tableFrqHist.setFont(new java.awt.Font("Tahoma", 0, 14));
		tableFrqHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"COLVALUE", "VALCOUNT"
				}
				));
		for(int i=0;i<DB2ColumnStatistics.DefaultFrequencyBucketSize;i++){
			String[] data = {"",""};
			((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
		}
		tableFrqHist.setToolTipText("<HTML> Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : VARCHAR(254) : Value whose type is same as column type <br>  &nbsp; &nbsp; VALCOUNT : BIGINT : -1 or VALCOUNT >= 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; VALCOUNT must be increasing. <br> &nbsp; &nbsp; Sum (VALCOUNT's) <= Cardinality <br> &nbsp; &nbsp; Number of buckets <= Column Cardinality <br> &nbsp; &nbsp; There can be one entry greater than high2key and one entry lesser than low2key. </HTML>");
		jScrollPane4.setViewportView(tableFrqHist);

		jPanel7.setBorder(javax.swing.BorderFactory.createTitledBorder("Set Value"));

		labelNoFreqBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNoFreqBuckets.setText("Set the number of Buckets:");

		textNumFreqBuckets.setFont(new java.awt.Font("Cambria", 0, 14));

		buttonCreateFreqBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		buttonCreateFreqBuckets.setText("Create");
		buttonCreateFreqBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateFreqBucketsActionPerformed(evt);
			}
		});

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

		textFreqHistWrite.setEnabled(false);

		buttonFreqhistUpload.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonFreqhistUpload.setText("Upload From File");
		buttonFreqhistUpload.setToolTipText("Upload Frequency Distribution from file.");
		buttonFreqhistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonFreqhistUploadActionPerformed(evt);
			}
		});

		separator_1 = new JSeparator();

		javax.swing.GroupLayout jPanel7Layout = new javax.swing.GroupLayout(jPanel7);
		jPanel7Layout.setHorizontalGroup(
				jPanel7Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel7Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel7Layout.createSequentialGroup()
										.addComponent(checkFreqHistWrite, GroupLayout.PREFERRED_SIZE, 110, GroupLayout.PREFERRED_SIZE)
										.addContainerGap())
										.addGroup(jPanel7Layout.createSequentialGroup()
												.addComponent(textFreqHistWrite, GroupLayout.DEFAULT_SIZE, 253, Short.MAX_VALUE)
												.addGap(14))
												.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
														.addComponent(separator_1, GroupLayout.PREFERRED_SIZE, 253, GroupLayout.PREFERRED_SIZE)
														.addContainerGap())
														.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
																.addComponent(buttonFreqhistUpload, GroupLayout.PREFERRED_SIZE, 153, GroupLayout.PREFERRED_SIZE)
																.addGap(62))
																.addGroup(jPanel7Layout.createSequentialGroup()
																		.addComponent(jSeparator6, GroupLayout.DEFAULT_SIZE, 253, Short.MAX_VALUE)
																		.addGap(14))
																		.addGroup(jPanel7Layout.createSequentialGroup()
																				.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
																						.addGroup(jPanel7Layout.createSequentialGroup()
																								.addGap(132)
																								.addComponent(buttonCreateFreqBuckets, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
																								.addGroup(jPanel7Layout.createSequentialGroup()
																										.addComponent(labelNoFreqBuckets)
																										.addPreferredGap(ComponentPlacement.RELATED)
																										.addComponent(textNumFreqBuckets, GroupLayout.PREFERRED_SIZE, 50, GroupLayout.PREFERRED_SIZE)))
																										.addContainerGap(14, Short.MAX_VALUE))))
				);
		jPanel7Layout.setVerticalGroup(
				jPanel7Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel7Layout.createSequentialGroup()
						.addGap(8)
						.addGroup(jPanel7Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelNoFreqBuckets)
								.addComponent(textNumFreqBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(buttonCreateFreqBuckets, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(jSeparator6, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(buttonFreqhistUpload, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.UNRELATED)
								.addComponent(separator_1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addGap(18)
								.addComponent(checkFreqHistWrite)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(textFreqHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addContainerGap())
				);
		jPanel7.setLayout(jPanel7Layout);

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4Layout.setHorizontalGroup(
				jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel7, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addGap(18)
						.addComponent(jScrollPane4, GroupLayout.DEFAULT_SIZE, 769, Short.MAX_VALUE)
						.addContainerGap())
				);
		jPanel4Layout.setVerticalGroup(
				jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel4Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(jScrollPane4, 0, 0, Short.MAX_VALUE)
								.addComponent(jPanel7, GroupLayout.DEFAULT_SIZE, 210, Short.MAX_VALUE))
								.addContainerGap())
				);
		jPanel4.setLayout(jPanel4Layout);

		jTabbedPane1.addTab("Frequency Value", jPanel4);

		tableQuantHist.setFont(new java.awt.Font("Tahoma", 0, 14));
		tableQuantHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"COLVAUE", "VALCOUNT (Cumulative)", "DISTCOUNT (Cumulative)"
				}
				));
		for(int i=0;i<DB2ColumnStatistics.DefaultQuantileBucketSize;i++){
			String[] data = {"","",""};
			((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
		}
		tableQuantHist.setToolTipText("<HTML> Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : VARCHAR(254) : Value whose type is same as column type <br>  &nbsp; &nbsp; VALCOUNT : BIGINT : -1 or VALCOUNT >= 0 <br>  &nbsp; &nbsp; DISTCOUNT : BIGINT : Empty or DISTCOUNT >= 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; COLVALUE,VALCOUNT, DISTCOUNT must be increasing.<br> &nbsp; &nbsp; DISTCOUNT<sub>i</sub>  <= VALCOUNT<sub>i</sub> <br> &nbsp; &nbsp; The largest COLVALUE's VALCOUNT must be equal to Cardinality. <br> &nbsp; &nbsp; The largest COLVALUE's DISTCOUNT must be equal to Column Cardinality.<br> Optional Consistency Constraint: These constraints are informed to the user, but not enforced strictly. <br> &nbsp; &nbsp; There can be one unique entry greater than High2key and one unique entry lesser than Low2key.<br> &nbsp; &nbsp; The VALCOUNT of a Quantile Histogram bin b must be greater than the sum of VALCOUNTs <br> in the Frequency Histogram whose COLVALUE is less than the bâ€™s COLVALUE. </HTML>");
		jScrollPane3.setViewportView(tableQuantHist);

		jPanel8.setBorder(javax.swing.BorderFactory.createTitledBorder("Set value"));

		labelNumQuantBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNumQuantBuckets.setText("Set the number of Buckets:");

		textNumQuantBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		textNumQuantBuckets.setText(DB2ColumnStatistics.DefaultQuantileBucketSize + "");
		textNumQuantBuckets.setEnabled(false);

		buttonCreateQuantBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		buttonCreateQuantBuckets.setText("Create ");
		buttonCreateQuantBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateQuantBucketsActionPerformed(evt);
			}
		});

		checkQuantHistWrite.setBackground(new java.awt.Color(247, 246, 235));
		checkQuantHistWrite.setFont(new java.awt.Font("Tahoma", 0, 12));
		checkQuantHistWrite.setText("Write to File");
		checkQuantHistWrite.setToolTipText("Write Quantile Distribution to file.");
		checkQuantHistWrite.setContentAreaFilled(false);
		checkQuantHistWrite.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkQuantHistWriteActionPerformed(evt);
			}
		});

		textQuantHistWrite.setEnabled(false);

		buttonQuantHistUpload.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonQuantHistUpload.setText("Upload From File");
		buttonQuantHistUpload.setToolTipText("Upload Quantile Distribution from file.");
		buttonQuantHistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistUploadActionPerformed(evt);
			}
		});

		separator = new JSeparator();

		javax.swing.GroupLayout jPanel8Layout = new javax.swing.GroupLayout(jPanel8);
		jPanel8Layout.setHorizontalGroup(
				jPanel8Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel8Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel8Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel8Layout.createSequentialGroup()
										.addComponent(checkQuantHistWrite)
										.addContainerGap())
										.addGroup(jPanel8Layout.createSequentialGroup()
												.addComponent(textQuantHistWrite, GroupLayout.DEFAULT_SIZE, 259, Short.MAX_VALUE)
												.addContainerGap())
												.addGroup(jPanel8Layout.createSequentialGroup()
														.addComponent(separator, GroupLayout.PREFERRED_SIZE, 259, GroupLayout.PREFERRED_SIZE)
														.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
														.addGroup(Alignment.TRAILING, jPanel8Layout.createSequentialGroup()
																.addComponent(buttonQuantHistUpload, GroupLayout.PREFERRED_SIZE, 153, GroupLayout.PREFERRED_SIZE)
																.addGap(63))
																.addGroup(jPanel8Layout.createSequentialGroup()
																		.addComponent(jSeparator7, GroupLayout.DEFAULT_SIZE, 259, Short.MAX_VALUE)
																		.addContainerGap())
																		.addGroup(jPanel8Layout.createSequentialGroup()
																				.addGroup(jPanel8Layout.createParallelGroup(Alignment.TRAILING)
																						.addGroup(jPanel8Layout.createSequentialGroup()
																								.addComponent(labelNumQuantBuckets)
																								.addPreferredGap(ComponentPlacement.UNRELATED)
																								.addComponent(textNumQuantBuckets, GroupLayout.PREFERRED_SIZE, 50, GroupLayout.PREFERRED_SIZE))
																								.addGroup(jPanel8Layout.createSequentialGroup()
																										.addPreferredGap(ComponentPlacement.RELATED, 133, GroupLayout.PREFERRED_SIZE)
																										.addComponent(buttonCreateQuantBuckets, GroupLayout.PREFERRED_SIZE, 126, GroupLayout.PREFERRED_SIZE)))
																										.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))))
				);
		jPanel8Layout.setVerticalGroup(
				jPanel8Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel8Layout.createSequentialGroup()
						.addGap(6)
						.addGroup(jPanel8Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(labelNumQuantBuckets)
								.addComponent(textNumQuantBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(buttonCreateQuantBuckets, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(jSeparator7, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(buttonQuantHistUpload, GroupLayout.PREFERRED_SIZE, 35, GroupLayout.PREFERRED_SIZE)
								.addPreferredGap(ComponentPlacement.UNRELATED)
								.addComponent(separator, GroupLayout.DEFAULT_SIZE, 11, Short.MAX_VALUE)
								.addPreferredGap(ComponentPlacement.UNRELATED)
								.addComponent(checkQuantHistWrite)
								.addPreferredGap(ComponentPlacement.RELATED)
								.addComponent(textQuantHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
								.addContainerGap())
				);
		jPanel8.setLayout(jPanel8Layout);

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5Layout.setHorizontalGroup(
				jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel8, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addGap(12)
						.addComponent(jScrollPane3, GroupLayout.DEFAULT_SIZE, 771, Short.MAX_VALUE)
						.addContainerGap())
				);
		jPanel5Layout.setVerticalGroup(
				jPanel5Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel5Layout.createParallelGroup(Alignment.BASELINE)
								.addComponent(jScrollPane3, GroupLayout.DEFAULT_SIZE, 242, Short.MAX_VALUE)
								.addComponent(jPanel8, GroupLayout.PREFERRED_SIZE, 215, Short.MAX_VALUE))
								.addContainerGap())
				);
		jPanel5.setLayout(jPanel5Layout);

		jTabbedPane1.addTab("Quantile Value", jPanel5);

		jPanel6.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel6.setOpaque(false);

		labelIndCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndCard.setText("Index Cardinality:");
		labelIndCard.setToolTipText("Cardinality of the index.");

		textIndCard.setToolTipText("<HTML> Datatype: BIGINT<br> Structural Constraint: <br> &nbsp; &nbsp; -1 or Index Cardinality &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Index Cardinality = Cardinality </HTML>");

		labelNLeaf.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNLeaf.setText("NLeaf:");
		labelNLeaf.setToolTipText("Number of leaf pages.");

		textNLeaf.setToolTipText("<HTML> Datatype: BIGINT<br> Structural Constraint: <br> &nbsp; &nbsp; -1 or NLeafs &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; None </HTML>");

		labelNLevels.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNLevels.setText("NLevels: ");
		labelNLevels.setToolTipText("Number of index levels.");

		textNLevels.setToolTipText("<HTML> Datatype: SMALLINT<br> Structural Constraint: <br> &nbsp; &nbsp; -1 or NLevels &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NLevesl &le NLeafs </HTML>");

		labelDensity.setFont(new java.awt.Font("Cambria", 0, 14));
		labelDensity.setText("Density: ");
		labelDensity.setToolTipText("<html>Ratio of SEQUENTIAL_PAGES to number <br> of pages in the range of pages <br> occupied by the index, <br>expressed as a percent <br> (integer between 0 and 100) </html>");

		textDensity.setToolTipText("<HTML> Datatype: INTEGER <br> Structural Constraint: <br> &nbsp; &nbsp; -1 or 0 &le Density &le 100 <br>  Consistency Constraint: <br> &nbsp; &nbsp; None </HTML>");

		labelNumRID.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNumRID.setText("NumRID: ");
		labelNumRID.setToolTipText("<html> Total number of row identifiers (RIDs) <br> or block identifiers (BIDs) in the index</html>");

		textNumRID.setToolTipText("<HTML> Datatype: BIGINT<br> Structural Constraint: <br> &nbsp; &nbsp; -1 or NumRIDs &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NumRIDs &ge Index Cardinality </HTML>");

		labelNumEmptyLeafs.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNumEmptyLeafs.setText("NumEmptyLeafs: ");
		labelNumEmptyLeafs.setToolTipText("<html> Total number of index leaf pages that <br> have all of their row identifiers <br> (or block identifiers) marked deleted.</html>");

		textNumEmptyLeafs.setToolTipText("<HTML> Datatype: BIGINT<br> Structural Constraint: <br> &nbsp; &nbsp; -1 or NumEmptyLeafs &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Num Empty Leafs &le NLeafs </HTML>");

		javax.swing.GroupLayout jPanel6Layout = new javax.swing.GroupLayout(jPanel6);
		jPanel6Layout.setHorizontalGroup(
			jPanel6Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel6Layout.createSequentialGroup()
					.addComponent(labelIndCard)
					.addGap(3)
					.addComponent(textIndCard, GroupLayout.DEFAULT_SIZE, 149, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(labelNLeaf)
					.addGap(2)
					.addComponent(textNLeaf, GroupLayout.DEFAULT_SIZE, 107, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(labelNLevels)
					.addGap(1)
					.addComponent(textNLevels, GroupLayout.DEFAULT_SIZE, 110, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelDensity)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textDensity, GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelNumRID)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textNumRID, GroupLayout.DEFAULT_SIZE, 98, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(labelNumEmptyLeafs)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textNumEmptyLeafs, GroupLayout.PREFERRED_SIZE, 80, GroupLayout.PREFERRED_SIZE)
					.addGap(20))
		);
		jPanel6Layout.setVerticalGroup(
			jPanel6Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel6Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel6Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelIndCard)
						.addComponent(textIndCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelNumEmptyLeafs)
						.addComponent(textNumEmptyLeafs, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelDensity)
						.addComponent(textDensity, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelNLevels)
						.addComponent(textNLevels, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelNLeaf)
						.addComponent(textNLeaf, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelNumRID)
						.addComponent(textNumRID, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
		);
		jPanel6.setLayout(jPanel6Layout);

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

		buttonUpdateColumn.setText("Update Column");
		buttonUpdateColumn.setToolTipText("Updates the column level metadata");
		buttonUpdateColumn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateColumnActionPerformed(evt);
			}
		});

		comboBoxRelations.setToolTipText("Selected Relations for Construt");
		comboBoxRelations.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxRelationsActionPerformed(evt);
			}
		});

		labelRelationName.setFont(new java.awt.Font("Cambria", 0, 14));
		labelRelationName.setText("Relation Name:");

		labelAttribute.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAttribute.setText("Attribute Name:");

		comboBoxAttribute.setToolTipText("Attributes of selected relation");
		comboBoxAttribute.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxAttributeActionPerformed(evt);
			}
		});

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
			jPanel1Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(checkIndexStats)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addComponent(labelAttribute)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, 471, GroupLayout.PREFERRED_SIZE))
								.addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, 1255, Short.MAX_VALUE)
								.addComponent(jPanel3, 0, 0, Short.MAX_VALUE)
								.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 1255, Short.MAX_VALUE)
								.addComponent(jPanel6, GroupLayout.DEFAULT_SIZE, 1255, Short.MAX_VALUE)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addComponent(labelIndexStats)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(labelIndexStatsTip)
									.addPreferredGap(ComponentPlacement.RELATED, 483, Short.MAX_VALUE)
									.addComponent(buttonQuantHistShowGraph)
									.addGap(16))))
						.addGap(657)
						.addGroup(Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 132, GroupLayout.PREFERRED_SIZE)
							.addGap(12)
							.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 132, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 142, GroupLayout.PREFERRED_SIZE)
							.addGap(12)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(labelRelationName)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, 473, GroupLayout.PREFERRED_SIZE)))
					.addContainerGap())
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addGap(6)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelRelationName)
						.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(jPanel2, GroupLayout.PREFERRED_SIZE, 59, GroupLayout.PREFERRED_SIZE)
					.addGap(12)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelAttribute)
						.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel3, GroupLayout.PREFERRED_SIZE, 60, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 288, Short.MAX_VALUE)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.CENTER)
						.addComponent(labelIndexStats)
						.addComponent(labelIndexStatsTip)
						.addComponent(buttonQuantHistShowGraph))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(checkIndexStats)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel6, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE, false)
						.addComponent(Update, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
						.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
						.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
						.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE))
					.addGap(6))
		);
		jPanel1.setLayout(jPanel1Layout);

		getContentPane().add(jPanel1, BorderLayout.CENTER);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void backButtonActionPerformed(java.awt.event.ActionEvent evt) {
		String[] relations = new String[relUpdated.size()];
		int i = 0;
		for(String rel : relUpdated.keySet()){
			relations[i] = rel;
			i++;
		}
		new ConstructMode(DBConstants.DB2 , relations, database).setVisible(true);
		this.dispose();				
	}

	private void UpdateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_UpdateActionPerformed
		int status = JOptionPane.showConfirmDialog(null, "If you have chosen to construct without inputting or modifying all relations or columns,\nthen they are not validated for legal and consistency constraint.\n"
				+ "Do you want to continue with the construcion?", "Choose the option.", JOptionPane.YES_NO_OPTION);
		if (status == JOptionPane.YES_OPTION) {
			this.updateStats();
		}
	}//GEN-LAST:event_UpdateActionPerformed

	private void buttonQuantHistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistUploadActionPerformed
		String filePath;
		try {
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"DB2";
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

				while(tableQuantHist.getModel().getRowCount()>0){
					((DefaultTableModel)tableQuantHist.getModel()).removeRow(0);
				}
				String line = br.readLine();
				int i = 0;
				while (line != null) {
					((DefaultTableModel)tableQuantHist.getModel()).addRow(new String[2]);
					String[] vals = line.split("[|]");
					tableQuantHist.getModel().setValueAt(vals[0], i, 0);
					tableQuantHist.getModel().setValueAt(vals[1], i, 1);
					if(vals.length > 2)
					{
						tableQuantHist.getModel().setValueAt(vals[2], i, 2);
					}
					if (vals.length > 2) {
						Constants.CPrintToConsole(vals[0] + "\t" + vals[1] + "\t" + vals[2], Constants.DEBUG_SECOND_LEVEL_Information);
					} else {
						Constants.CPrintToConsole(vals[0] + "\t" + vals[1], Constants.DEBUG_SECOND_LEVEL_Information);
					}
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
		if (bucketSize >= 0 && bucketSize <= DB2ColumnStatistics.MaxQuantileBucketSize) {
			if(this.quantHistRows > bucketSize) {
				// Remove the last entries
				int removeCnt = this.quantHistRows - bucketSize;
				for (int i = 0; i < removeCnt; i++) {
					// remove 0th row, removeCnt times.
					((DefaultTableModel) tableQuantHist.getModel()).removeRow(this.quantHistRows - i - 1);
				}
				this.quantHistRows = bucketSize;
			} else {
				int addCnt = bucketSize - this.quantHistRows;
				this.quantHistRows = bucketSize;
				String[] data;
				for (int i = 0; i < addCnt; i++) {
					data = new String[]{"",""};
					((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
				}
			}
			tableQuantHist.getColumnModel().setColumnSelectionAllowed(true);
		} else {
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+DB2ColumnStatistics.MaxQuantileBucketSize+"]." , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonCreateQuantBucketsActionPerformed

	private void buttonFreqhistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonFreqhistUploadActionPerformed
		String filePath;
		try{
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"DB2";
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

	private void checkFreqHistWriteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkFreqHistWriteActionPerformed
		if (checkFreqHistWrite.isSelected()) {
			textFreqHistWrite.setEnabled(true);
		} else {
			textFreqHistWrite.setEnabled(false);
		}
	}//GEN-LAST:event_checkFreqHistWriteActionPerformed

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
		if (bucketSize >=0 && bucketSize <= DB2ColumnStatistics.MaxFrequencyBucketSize) {
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
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+DB2ColumnStatistics.MaxFrequencyBucketSize+"]." , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonCreateFreqBucketsActionPerformed

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
		if(populatingCols){
			return;			
		}
		String col = this.getCurrentAttribute();
		if(col != null && !col.trim().isEmpty())
		{
			String relationName = this.getCurrentRelation();
			DB2ColumnStatistics columnStatistics = (DB2ColumnStatistics) this.stats.get(relationName).getColumnStatistics(col);
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

	private void buttonResetColValuesActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonResetColValuesActionPerformed
		String col = this.getCurrentAttribute();
		this.InitializeColumnLevelMetadata(col);
	}//GEN-LAST:event_buttonResetColValuesActionPerformed

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
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"DB2";
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
						String colValue = (String) this.tableQuantHist.getModel().getValueAt(j, 0);
						String valCount = (String) this.tableQuantHist.getModel().getValueAt(j, 1);
						String distCount = (String) this.tableQuantHist.getModel().getValueAt(j, 2);
						if (colValue != null && valCount != null) {
							temp += colValue + "|" + valCount;
							if (distCount != null) {
								temp += "|" + distCount + "\n";
							}
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
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"DB2";
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
			JOptionPane.showMessageDialog(null, "Column Statistics Updated.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
			// Set the attribute as updated.
			String relationName = this.getCurrentRelation();
			String columnName = this.getCurrentAttribute();
			this.updateableStats.get(relationName).put(columnName, true);	
		}
	}//GEN-LAST:event_buttonUpdateColumnActionPerformed

	private void buttonQuantHistShowGraphActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistShowGraphActionPerformed
		/**
		 * Read all the information from the table and show it in the graph.
		 */
		boolean db2Hist = true;
		boolean unique = false;
		boolean noDistinct = false;
		int count = this.tableQuantHist.getModel().getRowCount();
		Constants.CPrintToConsole("Count: " + count, Constants.DEBUG_SECOND_LEVEL_Information);
		int i = 0;
		String relationName = this.getCurrentRelation();
		String colName = this.getCurrentAttribute();
		String type = null;
		try{
			type = database.getType(relationName, colName);
		}catch(Exception e)
		{
			Constants.CPrintErrToConsole(e);
		}
		String[] labels = new String[count];
		double[] data = new double[count];
		double[] dataDistinct = new double[count];
		double[] f = new double[count];
		double[] d =  new double[count];
		int realCnt = 0;

		while(count > 0)
		{
			String s1 = (String)tableQuantHist.getModel().getValueAt(i, 0);
			String s2 = (String)tableQuantHist.getModel().getValueAt(i,1);
			String s3 = (String)tableQuantHist.getModel().getValueAt(i,2);
			if( s1 != null && !s1.equals(this.emptyString) && s2 != null && !s2.equals(this.emptyString))
			{
				data[i] = Double.parseDouble(s2);
				if(s3 != null && !s3.equals(this.emptyString))
				{
					dataDistinct[i] = Double.parseDouble(s3);
				}
				else
				{
					dataDistinct[i] = 0;
					noDistinct = true;
				}

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
				double freq = data[i] - prevValCount;
				prevValCount = data[i];
				f[i] = freq;
				double dist = dataDistinct[i] - prevDistCount;
				prevDistCount = dataDistinct[i];
				d[i] = dist;
				Constants.CPrintToConsole("Bucket "+i+": valCount:" + freq+"  Distinct:"+dist, Constants.DEBUG_SECOND_LEVEL_Information);
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
				} else if(DataType.isNumeric(type)) {
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
				} else {
					val = new DataType(DataType.VARCHAR, "" + labels[i]);
					if (i == 0) {
						String minStr =  " ";
						prevVal = new DataType(DataType.VARCHAR, "" + minStr);
					} else {
						prevVal = new DataType(DataType.VARCHAR, "" + labels[i - 1]);
					}
				}
				Constants.CPrintToConsole("(" + prevVal.getString() + ", " + val.getString() + " ):: " + f[i] + " (" + freqPercent + ")", Constants.DEBUG_SECOND_LEVEL_Information);
				BucketItem bucket = new BucketItem(prevVal, val, f[i], d[i], freqPercent, distinctCountPercent);
				buckets.add(bucket);
			}
			DataType minDT;
			DataType maxDT;
			DataType minWidth;
			if(DataType.isDouble(type)) {
				double min = Double.parseDouble(labels[0]) - d[0];
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
					Constants.CPrintToConsole(temp, Constants.DEBUG_SECOND_LEVEL_Information);
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
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_WITHBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.DOUBLE, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isInteger(type)) {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_WITHBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.INTEGER, db2Hist, unique, noDistinct).setVisible(true);
			} else if(DataType.isNumeric(type)) {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_WITHBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.NUMERIC, db2Hist, unique, noDistinct).setVisible(true);
			} else {
				new GraphHistogram(this, GraphHistogram.FREQUENCY_MODE_WITHOUTBB, IntegerTotalFreqCount, IntegerTotalDistinctCount, buckets, minDT, maxDT, minHeight, minWidth, colName, DataType.VARCHAR, db2Hist, unique, noDistinct).setVisible(true);
			}
			//new histogramGUI(this,labels,data,2,max).setVisible(true);
		}
		else if(realCnt <= 0) {
			JOptionPane.showMessageDialog(null, "No Values in the Table.\n" , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonQuantHistShowGraphActionPerformed

	private void checkIndexStatsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkIndexStatsActionPerformed
		if (checkIndexStats.isSelected()) {
			this.textIndCard.setEnabled(true);
			this.textNLeaf.setEnabled(true);
			this.textNLevels.setEnabled(true);
			this.textDensity.setEnabled(true);
			this.textNumRID.setEnabled(true);
			this.textNumEmptyLeafs.setEnabled(true);
		} else {
			this.textIndCard.setEnabled(false);
			this.textNLeaf.setEnabled(false);
			this.textNLevels.setEnabled(false);
			this.textDensity.setEnabled(false);
			this.textNumRID.setEnabled(false);
			this.textNumEmptyLeafs.setEnabled(false);
		}
	}//GEN-LAST:event_checkIndexStatsActionPerformed

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
	private javax.swing.JComboBox comboBoxRelations;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel2;
	private javax.swing.JPanel jPanel3;
	private javax.swing.JPanel jPanel4;
	private javax.swing.JPanel jPanel5;
	private javax.swing.JPanel jPanel6;
	private javax.swing.JPanel jPanel7;
	private javax.swing.JPanel jPanel8;
	private javax.swing.JScrollPane jScrollPane3;
	private javax.swing.JScrollPane jScrollPane4;
	private javax.swing.JSeparator jSeparator6;
	private javax.swing.JSeparator jSeparator7;
	private javax.swing.JTabbedPane jTabbedPane1;
	private javax.swing.JLabel labelAttribute;
	private javax.swing.JLabel labelAvgColLenChar;
	private javax.swing.JLabel labelCard;
	private javax.swing.JLabel labelColCard;
	private javax.swing.JLabel labelDensity;
	private javax.swing.JLabel labelFPages;
	private javax.swing.JLabel labelHigh2Key;
	private javax.swing.JLabel labelIndCard;
	private javax.swing.JLabel labelIndexStats;
	private javax.swing.JLabel labelIndexStatsTip;
	private javax.swing.JLabel labelLow2Key;
	private javax.swing.JLabel labelNLeaf;
	private javax.swing.JLabel labelNLevels;
	private javax.swing.JLabel labelNPages;
	private javax.swing.JLabel labelNoFreqBuckets;
	private javax.swing.JLabel labelNumQuantBuckets;
	private javax.swing.JLabel labelNumEmptyLeafs;
	private javax.swing.JLabel labelNumRID;
	private javax.swing.JLabel labelOverflow;
	private javax.swing.JLabel labelRelationName;
	private javax.swing.JLabel lableNullCount;
	private javax.swing.JTable tableFrqHist;
	private javax.swing.JTable tableQuantHist;
	private javax.swing.JTextField textAvgColLenChar;
	private javax.swing.JTextField textCard;
	private javax.swing.JTextField textColCard;
	private javax.swing.JTextField textDensity;
	private javax.swing.JTextField textFPages;
	private javax.swing.JTextField textFreqHistWrite;
	private javax.swing.JTextField textHigh2Key;
	private javax.swing.JTextField textIndCard;
	private javax.swing.JTextField textLow2Key;
	private javax.swing.JTextField textNLeaf;
	private javax.swing.JTextField textNLevels;
	private javax.swing.JTextField textNPages;
	private javax.swing.JTextField textNumFreqBuckets;
	private javax.swing.JTextField textNumQuantBuckets;
	private javax.swing.JTextField textNullCount;
	private javax.swing.JTextField textNumEmptyLeafs;
	private javax.swing.JTextField textNumRID;
	private javax.swing.JTextField textOverflow;
	private javax.swing.JTextField textQuantHistWrite;
	private JSeparator separator;
	private JSeparator separator_1;
	private JButton backButton;
	// End of variables declaration//GEN-END:variables

	@Override
	public void setHistogram(ArrayList<BucketItem> buckets, boolean noDistinct) {
		//Remove 0th row quantHistRows times
		for (int r = 0; r < quantHistRows; r++) {
			((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
		}
		this.quantHistRows = buckets.size();
		this.textNumQuantBuckets.setText(buckets.size()+"");
		double prevFreq = 0;
		double prevDistCount = 0;
		for (int i = 0; i < buckets.size(); i++) {
			BucketItem bucket = buckets.get(i);
			Constants.CPrintToConsole("(" + bucket.getLValue().getString() + "," + bucket.getValue().getString() + ") :: " + bucket.getFreq() + " (" + bucket.getFreqPercent() + ")" + " : " + bucket.getDistinctCount() + " (" + bucket.getDistinctCountPercent() + ")", Constants.DEBUG_SECOND_LEVEL_Information);
			String colValue = bucket.getValue().getString();
			double freq = bucket.getFreq() + prevFreq;
			prevFreq = freq;
			//For DB2, VALCOUNT and DISTINCT COUNT COLUMNS are of INTEGER TYPE
			Long freqInt = (long)freq;
			String freqStr = ""+freqInt;
			double distCount = bucket.getDistinctCount()+prevDistCount;
			prevDistCount = distCount;
			String distCountStr=this.emptyString;
			if(!noDistinct)
			{
				//For DB2, VALCOUNT and DISTINCT COUNT COLUMNS are of INTEGER TYPE
				Long distCountInt = (long)distCount;
				distCountStr= ""+distCountInt;
			}
			String[] data = {colValue, freqStr, distCountStr};
			((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
		}
	}
}
