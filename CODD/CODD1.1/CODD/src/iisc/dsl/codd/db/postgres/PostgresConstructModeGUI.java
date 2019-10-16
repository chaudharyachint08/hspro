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

import iisc.dsl.codd.client.ConstructMode;
import iisc.dsl.codd.client.ConstructModeFrame;
import iisc.dsl.codd.client.GetRelationFrame;
import iisc.dsl.codd.client.gui.ExcelAdapter;
import iisc.dsl.codd.db.db2.*;
import iisc.dsl.codd.db.ColumnStatistics;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.HistogramObject;
import iisc.dsl.codd.db.IndexStatistics;
import iisc.dsl.codd.db.RelationStatistics;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JOptionPane;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.swing.JFileChooser;
import javax.swing.table.DefaultTableModel;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.ListSelectionModel;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

/**
 * Construct Mode Frame for Postgres.
 * @author dsladmin
 */
public class PostgresConstructModeGUI extends ConstructModeFrame {

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
	public PostgresConstructModeGUI(Database database, Statistics[] stats) {
		super("PostgreSQL Construct Mode");
		quantHistRows = PostgresColumnStatistics.DefaultHistogramBoundSize;
		freqHistRows = PostgresColumnStatistics.DefaultFrequencyBucketSize;
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
	private void populateRelationsComboBox(){
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
			PostgresRelationStatistics relationStatistics = (PostgresRelationStatistics) this.stats.get(relationName).getRelationStatistics();
			this.textCard.setText(relationStatistics.getCardinality().toPlainString()+"");
			this.textNPages.setText(relationStatistics.getRelPages().toPlainString()+"");
		}
		else
		{
			this.textCard.setText("");
			this.textNPages.setText("");
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
		this.buttonUpdateRelation.setEnabled(enable);
	}

	/**
	 * Column-Index Level Metadata
	 * Fills value from the Stats[].
	 */
	public void InitializeColumnLevelMetadata(String columnName){
		if(columnName != null)
		{
			String relationName = this.getCurrentRelation();
			DecimalFormat df = new DecimalFormat("#");
			df.setMaximumFractionDigits(Double.MAX_EXPONENT);
			
			PostgresColumnStatistics columnStatistics = (PostgresColumnStatistics) this.stats.get(relationName).getColumnStatistics(columnName);
			// Column-Level
			this.textColCard.setText(df.format(columnStatistics.getN_distinct()) + "");
			this.textNullCount.setText(df.format(columnStatistics.getNull_frac()) + "");
			this.textAvgWidth.setText(columnStatistics.getAvgWidth()+ "");
			this.textCorrelation.setText(df.format(columnStatistics.getCorrelation())+"");

			// Column-Histogram-Level
			// Freq Hist  - Postgres uses DB2 style frequency histogram
			TreeMap<Integer, DB2FreqHistObject> map = (TreeMap<Integer, DB2FreqHistObject>) columnStatistics.getFrequencyHistogram();

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
					BigDecimal tableCard = stats.get(this.getCurrentRelation()).getRelationStatistics().getCardinality();
					Double temp = valCount.doubleValue() / tableCard.doubleValue();
					String freq = df.format(temp);
					if(colValue != null) {
						((DefaultTableModel) tableFrqHist.getModel()).setValueAt(colValue, seqno-1, 0);
						String valCountStr = ""+freq;
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
				freqHistRows = PostgresColumnStatistics.DefaultFrequencyBucketSize;
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
			TreeMap<Integer, HistogramObject> mapQHist = (TreeMap<Integer, HistogramObject>) columnStatistics.getHistogramBounds();
			if (mapQHist != null) {

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
					if(colValue != null) {
						((DefaultTableModel) tableQuantHist.getModel()).setValueAt(colValue, seqno-1, 0);
					}
				}
			}
			else
			{
				//Remove 0th row quantHistRows times
				for (int r = 0; r < quantHistRows; r++) {
					((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
				}
				quantHistRows = PostgresColumnStatistics.DefaultHistogramBoundSize;
				this.textNumQuantBuckets.setText(quantHistRows+"");
				String[] data;
				for (int r = 0; r < quantHistRows; r++) {
					data = new String[]{"", ""};
					((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
				}
			}
			this.textQuantHistWrite.setText("Histogram_" + this.getCurrentRelation() + "_" + this.getCurrentAttribute());
			this.checkQuantHistWrite.setEnabled(true);
			this.checkQuantHistWrite.setSelected(false);
			this.textQuantHistWrite.setEnabled(false);


			// Index-Level
			HashMap<String, String> indexCols = indexColumns.get(relationName);

			String colNames = indexCols.get(columnName);
			if(colNames != null)
			{
				PostgresIndexStatistics indexStatistics = (PostgresIndexStatistics) this.stats.get(relationName).getIndexStatistics(colNames);
				this.checkIndexStats.setSelected(false);
				this.textIndCard.setText(indexStatistics.getReltuples()+"");
				this.textIndPages.setText(indexStatistics.getRelpages()+"");
				
				this.checkIndexStats.setEnabled(true);
				this.textIndCard.setEnabled(false);
				this.textIndPages.setEnabled(false);
			}
			else
			{
				this.checkIndexStats.setSelected(false);
				this.textIndCard.setText("");
				this.textIndPages.setText("");

				this.checkIndexStats.setEnabled(false);
				this.textIndCard.setEnabled(false);
				this.textIndPages.setEnabled(false);
			}
		}
		else
		{
			// Column-Level
			this.textColCard.setText("");
			this.textNullCount.setText("");
			this.textAvgWidth.setText("");
			this.textCorrelation.setText("");

			// Column-Histogram-Level
			// Freq Hist
			//Remove 0th row freqHistRows times
			for (int r = 0; r < freqHistRows; r++) {
				((DefaultTableModel) tableFrqHist.getModel()).removeRow(0);
			}
			freqHistRows = PostgresColumnStatistics.DefaultFrequencyBucketSize;
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
			quantHistRows = PostgresColumnStatistics.DefaultHistogramBoundSize;
			this.textNumQuantBuckets.setText(quantHistRows+"");
			for (int r = 0; r < quantHistRows; r++) {
				data = new String[]{""};
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
			this.textIndPages.setText("");
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
		this.textAvgWidth.setEnabled(enable);
		this.textCorrelation.setEnabled(enable);
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
		this.textIndPages.setEnabled(enable);

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
			String rel = (String) iter.next();
			Statistics stat = this.stats.get(rel);
			Constants.CPrintToConsole("Constructing Statistics for "+rel, Constants.DEBUG_SECOND_LEVEL_Information);
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
			JOptionPane.showMessageDialog(null, "Statistics Construct is Successful.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
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
		BigInteger card, npages;
		String relationName = this.getCurrentRelation();
		/**
		 * Node : Card (1)
		 * Datatype: BIGINT
		 * Structural Constraints:  >=0 and <= 9223372036854775807
		 * Consistency Constraints: NILL
		 */
		if(this.textCard.getText() == null || this.textCard.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Number of Tuples cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			card = new BigInteger(this.textCard.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Number of Tuples is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if(card.compareTo(new BigInteger(Long.MAX_VALUE+""))>0){
			valid = false;
			JOptionPane.showMessageDialog(null, "Maximum Number of Tuples allowed is: " + Long.MAX_VALUE + ". Please enter any positive integer value less than this value.");
			return valid;
		} else if (card.compareTo(BigInteger.ZERO) > 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Set the Number of Tuples to some positive integer value, Otherwise the statistics for the selected relation will remain unchanged.");
			return valid;
		}

		/**
		 * Node : RelPages (2)
		 * Datatype: BIGINT
		 * Structural Constraints: >=0
		 * Consistency Constraints: (1) RelPages <= RelTuples
		 */
		if(this.textNPages.getText() == null || this.textNPages.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Number of Pages cannot be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		try {
			npages = new BigInteger(this.textNPages.getText());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Number of Pages is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (npages.compareTo(BigInteger.ZERO) >= 0) {
			if (npages.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Number of Pages is greater than Number of Tuples.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Number of Pages can't be a negative value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		Statistics stat = stats.get(relationName);
		PostgresRelationStatistics postgresRelStat = (PostgresRelationStatistics)stat.getRelationStatistics();
		postgresRelStat.setCardinality(new BigDecimal(card));
		postgresRelStat.setRelPages(new BigDecimal(npages));
		stat.setRelationStatistics(postgresRelStat);
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
		PostgresRelationStatistics postgresRelStat = (PostgresRelationStatistics)stat.getRelationStatistics();
		PostgresColumnStatistics postgresColStat = (PostgresColumnStatistics)stat.getColumnStatistics(colName);
		Constraint constraint = postgresColStat.getConstraint();
		PostgresIndexStatistics postgresIndexStat = null;
		int avg_width;
		BigDecimal card, col_card, num_nulls;
		Double n_distinct, null_fraction, correlation;
		String mostCommonVals = "", mostCommonFreqs = "", histogramBounds = "";
		card = postgresRelStat.getCardinality();

		Statistics PKStat;
		PostgresColumnStatistics postgresPKColStat = null;
		try {
			String[] temp = database.getPrimaryKeyRelationAndColumn(relationName, colName);
			String PKRelation = temp[0];
			String PKColumn = temp[1];
			// If col is FK, retrieve the PK information if chosen to construct
			if (constraint.isFK() && stats.containsKey(PKRelation)) {
				PKStat = stats.get(PKRelation);
				postgresPKColStat = (PostgresColumnStatistics)PKStat.getColumnStatistics(PKColumn);
			}
		} catch(Exception e) {
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "CODD Exception: Retrieving Primary Key column.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
		}

		/**
		 * Node : Distinct Values - n_distinct [ColCard] (3)
		 * Datatype: DOUBLE
		 * Structural Constraints: >=-1
		 * Consistency Constraints: (4) n_distinct <= Card, if n_distinct > 0
		 * Attribute Constraints Checks: n_distinct = Card OR n_distinct = -1, if Unique
		 * Inter table Consistency Constraints: n_distinct <= PK.n_distinct, After conversion
		 */
		if(textColCard.getText() == null || textColCard.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Distinct Values can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			n_distinct = Double.parseDouble(textColCard.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Distinct Values is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (n_distinct >= -1) {
			// conversion
			col_card = PostgresColumnStatistics.convert_Postgres2Codd_DistinctValue(n_distinct, card);
			if (col_card.compareTo(card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Distinct Values must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if(constraint.isUnique() ) {
				if (col_card.equals(card) || n_distinct == -1.0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: Distinct Values must be equal to the Cardinality of the relation OR -1, since the column has a UNIQUE constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			if(constraint.isFK() && postgresPKColStat != null ) {
				if (col_card.compareTo(postgresPKColStat.getColCard()) <= 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Inter table Consistency Constraint: Distinct Values must be lesser than or equal to the Distinct Values of the PK relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Distinct value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Null Fraction (4)
		 * Structural Constraints: >=0 AND <= 1
		 * Consistency Constraints: (5) NumNulls <= Card
		 * Attribute Constraints Checks: NumNulls = 0, if NotNULL
		 */
		if(textNullCount.getText() == null || textNullCount.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nNull Fraction can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			null_fraction = Double.parseDouble(textNullCount.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Null Fraction is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (null_fraction >=0 && null_fraction <= 1) {
			num_nulls = PostgresColumnStatistics.convert_Postgres2Codd_NullFrac(null_fraction, card);
			if(constraint.isNotNULL()) {
				if (null_fraction == 0) {
					valid = true;
				} else {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Attribute Constraint Check: Null Fraction must be equal to 0, since the column has a NOTNULL constraint on it.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			}
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Null Fraction value. It must be in the range [0,1]. ", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Consistency Constraint: (3) col_card + num_nulls <= card
		 */
		BigDecimal sum = new BigDecimal(col_card+"");
		sum = sum.add(num_nulls);
		if (sum.compareTo(card) <= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Sum of Column Cardinality and Null Count must be lesser than or equal to the Cardinality of the relation.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Avg Width (5)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		if(textAvgWidth.getText() == null || textAvgWidth.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nAvg Width can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			avg_width = Integer.parseInt(textAvgWidth.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Avg Width is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (avg_width >= 0) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Avg Width value.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Correlation (7)
		 * Structural Constraints: >=-1 && <=1
		 * Consistency Constraints: NILL
		 */
		if(textCorrelation.getText() == null || textCorrelation.getText().trim().isEmpty()){
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: \nCorrelation can not be empty.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid; 
		}
		try {
			correlation = Double.parseDouble(this.textCorrelation.getText().trim());
		} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in Correlation is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		if (correlation >= -1 && correlation <= 1) {
			valid = true;
		} else {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Invalid Correlation value. It must be in the range [-1,1].", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}

		/**
		 * Node : Frequency Value Distribution (7)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: (7) Sum of Frequency values must be lesser than or equal to 1.
		 *                          (9) Number of entries must be lesser than or equal to the column cardinality.
		 */

		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,3,5,.. 19) ColValue - Data type must be same as column datatype
		 *                         Node (2,4,6... 20) -  >0 && <=1
		 * Consistency Constraints: ValCount nodes must be decreasing.
		 */

		int countFreq = this.tableFrqHist.getModel().getRowCount();
		String type = postgresColStat.getColumnType();
		DataType[] colValuesFreq = new DataType[countFreq];
		Double[] valCountsFreq = new Double[countFreq];
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
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - Common Column Value present in "+(i+1)+"th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				try {
					valCountsFreq[i] = Double.parseDouble(s2);
					if(!(valCountsFreq[i] > 0 && valCountsFreq[i] <= 1)) {
						valid = false;
						JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - Common Column Frequency present in "+(i+1)+"th row is not in the range 0 to 1.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
						return valid;
					}
				} catch (NumberFormatException e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Frequency Histogram - Common Column Frequency present in "+(i+1)+"th row is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} else{
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Frequency Histogram - Enter all the values in "+(i+1)+"th row.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}

		if(countFreq > 0)
		{
			Double sumValCounts = 0.0;
			for(int i=0;i<countFreq;i++)
			{
				sumValCounts = sumValCounts + valCountsFreq[i];
			}

			// Main Graph Consistency Constraints
			// (7) Sum of ValCount's must be lesser than or equal to 1.
			if (sumValCounts <= 1) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram - Sum of Common Value Frequencies is greater than 1.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			// (9) Number of entries must be lesser than or equal to the column cardinality.
			BigDecimal countFreqBI = new BigDecimal(countFreq+"");
			if (countFreqBI.compareTo(col_card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Consistency Constraint: Frequency Histogram - Number of entries in the Frequency Histogram is greater than the Column Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}

			// Everything is fine. Create the Most Common Vals, Most Common freqs, conversion to histogram map will be done later.
			for(int i=0;i<countFreq;i++)
			{
				if(i!=0) {
					mostCommonVals = mostCommonVals+",";
					mostCommonFreqs = mostCommonFreqs+",";
				}
				mostCommonVals = mostCommonVals + colValuesFreq[i].getString();
				mostCommonFreqs = mostCommonFreqs + valCountsFreq[i];
			}
		} else {
			mostCommonVals = null;
			mostCommonFreqs = null;
		}

		/**
		 * Node : Quantile Value Distribution (8)
		 * It is a super node. Validate the inner graph first, then validate the structural Constraints.
		 * Consistency Constraints: NILL.
		 */

		/*
		 * Inner Graph Validation.
		 * Structural Constraints: Nodes (1,4,7,.. 28) ColValue - Data type must be same as column datatype.
		 * Consistency Constraints: ColValue nodes must be increasing.
		 */
		int countQuant = this.tableQuantHist.getModel().getRowCount();
		DataType[] colValuesQuant = new DataType[countQuant];
		for(int i=0;i<countQuant;i++){
			String s1 = (String) tableQuantHist.getModel().getValueAt(i, 0);
			if (s1 != null && !s1.trim().equals(this.emptyString)) {
				try {
					colValuesQuant[i] = new DataType(type, s1.trim());
				} catch (Exception e) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Height Balanced Histogram - COLVALUE present in " + (i + 1) + "th row is not of Column data type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} else{
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Height Balanced Histogram - Enter the value in "+(i+1)+"th row.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
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

			// Everything is fine. Create the histogram_bounds. Map Quantile Histogram will converted later.
			for (int i = 0; i < countQuant; i++) {
				String col = colValuesQuant[i].getString();
				if (i != 0) {
					histogramBounds = histogramBounds + ",";
				}
				histogramBounds = histogramBounds + col;
			}
		}else
		{
			histogramBounds = null;
		}

		String Str_IndexCard = this.textIndCard.getText();
		String Str_IndPages = this.textIndPages.getText();

		BigDecimal ind_card = BigDecimal.ZERO, ind_pages = BigDecimal.ZERO;
		if(this.checkIndexStats.isSelected())
		{
			/**
			 * Node : IndCard (9)
			 * Structural Constraints: >=0
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
			 * Node : IndPages (10)
			 * Structural Constraints: >=0
			 * Consistency Constraints: (19) IndPages = RelPages
			 */
			try {
				ind_pages = new BigDecimal(new BigInteger(Str_IndPages));
			} catch (NumberFormatException e) {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Pages is not of Integer type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
			if (ind_pages.compareTo(ind_card) <= 0) {
				valid = true;
			} else {
				valid = false;
				JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Index Pages is greater than Index Cardinality.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
				return valid;
			}
		}
		// valid must be true
		/**
		 * Read the current relation stats and update into the allStats data structure.
		 */

		postgresColStat.setN_distinct(n_distinct);
		postgresColStat.setNull_frac(null_fraction);
		postgresColStat.setAvgWidth(avg_width);
		postgresColStat.setCorrelation(correlation);
		postgresColStat.setMost_common_vals(mostCommonVals);
		postgresColStat.setMost_common_freqs(mostCommonFreqs);
		postgresColStat.setHistogram_bounds(histogramBounds);
		try {
			postgresColStat.convert_PostgresFormat2CoddFormat(card);
		} catch(Exception e) {
			valid = false;
			Constants.CPrintErrToConsole(e);
			JOptionPane.showMessageDialog(null, "Exception in converting statistics to CODD Format.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
		}
		stat.setColumnStatistics(colName, postgresColStat);

		if(this.checkIndexStats.isSelected())
		{
			String colNames = this.indexColumns.get(relationName).get(colName);
			if(colNames != null) {
				postgresIndexStat = (PostgresIndexStatistics) stat.getIndexStatistics(colNames);
				postgresIndexStat.setReltuples(ind_card);
				postgresIndexStat.setRelpages(ind_pages);
				stat.setIndexStatistics(colNames, postgresIndexStat);
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
	@SuppressWarnings("unchecked")
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {

		jPanel1 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		jSeparator2 = new javax.swing.JSeparator();
		checkIndexStats = new javax.swing.JCheckBox();
		labelIndexStatsTip = new javax.swing.JLabel();
		labelIndexStats = new javax.swing.JLabel();
		buttonQuantHistShowGraph = new javax.swing.JButton();
		buttonResetColValues = new javax.swing.JButton();
		jPanel2 = new javax.swing.JPanel();
		labelCard = new javax.swing.JLabel();
		textCard = new javax.swing.JTextField();
		labelNPages = new javax.swing.JLabel();
		textNPages = new javax.swing.JTextField();
		buttonUpdateRelation = new javax.swing.JButton();
		jPanel3 = new javax.swing.JPanel();
		labelColCard = new javax.swing.JLabel();
		textColCard = new javax.swing.JTextField();
		lableNullCount = new javax.swing.JLabel();
		textNullCount = new javax.swing.JTextField();
		labelAvgWidth = new javax.swing.JLabel();
		textAvgWidth = new javax.swing.JTextField();
		labelCorrelation = new javax.swing.JLabel();
		textCorrelation = new javax.swing.JTextField();
		jTabbedPane1 = new javax.swing.JTabbedPane();
		jPanel4 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		jPanel6 = new javax.swing.JPanel();
		labelNumFreqBuckets = new javax.swing.JLabel();
		textNumFreqBuckets = new javax.swing.JTextField();
		jSeparator1 = new javax.swing.JSeparator();
		buttonCreateFreqBuckets = new javax.swing.JButton();
		checkFreqHistWrite = new javax.swing.JCheckBox();
		textFreqHistWrite = new javax.swing.JTextField();
		buttonFreqhistUpload = new javax.swing.JButton();
		jScrollPane4 = new javax.swing.JScrollPane();
		tableFrqHist = new javax.swing.JTable();
		tableFrqHist.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		tableFrqHist.setColumnSelectionAllowed(true);
		tableFrqHist.setFillsViewportHeight(true);
		jPanel5 = new iisc.dsl.codd.client.gui.BaseJPanel("img/bg_net_large.png");
		jPanel7 = new javax.swing.JPanel();
		labelNumQuantBuckets = new javax.swing.JLabel();
		textNumQuantBuckets = new javax.swing.JTextField();
		buttonCreateQuantBuckets = new javax.swing.JButton();
		jSeparator5 = new javax.swing.JSeparator();
		checkQuantHistWrite = new javax.swing.JCheckBox();
		textQuantHistWrite = new javax.swing.JTextField();
		buttonQuantHistUpload = new javax.swing.JButton();
		jScrollPane3 = new javax.swing.JScrollPane();
		tableQuantHist = new javax.swing.JTable();
		tableQuantHist.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
		tableQuantHist.setFillsViewportHeight(true);
		tableQuantHist.setColumnSelectionAllowed(true);
		Update = new javax.swing.JButton();
		jPanel8 = new javax.swing.JPanel();
		labelIndCard = new javax.swing.JLabel();
		textIndCard = new javax.swing.JTextField();
		labelIndPages = new javax.swing.JLabel();
		textIndPages = new javax.swing.JTextField();
		labelRelationName = new javax.swing.JLabel();
		comboBoxRelations = new javax.swing.JComboBox();
		labelAttribute = new javax.swing.JLabel();
		comboBoxAttribute = new javax.swing.JComboBox();
		buttonUpdateColumn = new javax.swing.JButton();
		// Used to add the copy paste functionality in JTable.
		new ExcelAdapter(tableFrqHist);
		new ExcelAdapter(tableQuantHist);

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

		jSeparator2.setForeground(new java.awt.Color(0, 0, 0));

		checkIndexStats.setBackground(new java.awt.Color(247, 246, 235));
		checkIndexStats.setFont(new java.awt.Font("Cambria", 0, 14));
		checkIndexStats.setText("Update index statistics (if it exists)");
		checkIndexStats.setOpaque(false);
		checkIndexStats.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkIndexStatsActionPerformed(evt);
			}
		});

		labelIndexStatsTip.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndexStatsTip.setText("[There is a system generated index on this attribute.]");

		labelIndexStats.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndexStats.setText("Index Statistics:");

		buttonQuantHistShowGraph.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonQuantHistShowGraph.setText("Show Histogram Bound Graph");
		buttonQuantHistShowGraph.setToolTipText("Graphically edit the histogram.");
		buttonQuantHistShowGraph.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistShowGraphActionPerformed(evt);
			}
		});

		buttonResetColValues.setText("Reset Values");
		buttonResetColValues.setToolTipText("Resets the Column level metadata");
		buttonResetColValues.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonResetColValuesActionPerformed(evt);
			}
		});

		jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel2.setOpaque(false);

		labelCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelCard.setText("Number of Tuples:");
		labelCard.setToolTipText("Number of rows in the table.");

		textCard.setToolTipText("<HTML>Number of Tuples (RelTuples):<br> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le RelTuples&le 9223372036854775807 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML> ");

		labelNPages.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNPages.setText("Number of Pages");
		labelNPages.setToolTipText("<html>Size of the on-disk representation of this table in pages (of size BLCKSZ) </html>");

		textNPages.setToolTipText("<HTML>Number of Pages (RelPages): <br> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; RelPages &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; RelPages &le RelTuples </HTML> ");

		buttonUpdateRelation.setText("Update");
		buttonUpdateRelation.setToolTipText("Updates Relation Level Metadata");
		buttonUpdateRelation.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateRelationActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
		jPanel2Layout.setHorizontalGroup(
			jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
					.addGap(6)
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.LEADING, false)
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addComponent(labelNPages)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(textNPages))
						.addGroup(jPanel2Layout.createSequentialGroup()
							.addComponent(labelCard)
							.addGap(3)
							.addComponent(textCard, GroupLayout.PREFERRED_SIZE, 219, GroupLayout.PREFERRED_SIZE)))
					.addPreferredGap(ComponentPlacement.RELATED, 30, Short.MAX_VALUE)
					.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 100, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		jPanel2Layout.setVerticalGroup(
			jPanel2Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel2Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelCard)
						.addComponent(textCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel2Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelNPages)
						.addComponent(textNPages, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(11, Short.MAX_VALUE))
				.addGroup(Alignment.TRAILING, jPanel2Layout.createSequentialGroup()
					.addContainerGap(24, Short.MAX_VALUE)
					.addComponent(buttonUpdateRelation, GroupLayout.PREFERRED_SIZE, 37, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		jPanel2.setLayout(jPanel2Layout);

		jPanel3.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel3.setOpaque(false);

		labelColCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelColCard.setText("Distinct Values: ");
		labelColCard.setToolTipText("<html> n_distinct (double): <br> Number of distinct values in the column </html> ");

		textColCard.setToolTipText("<HTML>Distinct Values (n_distinct): <br> Datatype: DOUBLE <br> Structural Constraint: <br> &nbsp; &nbsp; n_distinct &ge -1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; n_distinct &le RelTuples, if n_distinct > 0<br> &nbsp; &nbsp; n_distinct = RelTuples or n_distinct = -1, if unique Column <br> &nbsp; &nbsp; n_distinct &le Primary Key n_distinct, for Foreign Key Column <br> &nbsp; &nbsp; (n_distinct - negative values are converted to postive values for check).  </HTML> ");

		lableNullCount.setFont(new java.awt.Font("Cambria", 0, 14));
		lableNullCount.setText("Null Fraction: ");
		lableNullCount.setToolTipText("<html> NullFraction (Double): <br /> fraction of column entries that are null. </html>");

		textNullCount.setToolTipText("<HTML> Null Fraction:<br> Datatype: DOUBLE <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le Null Fraction &le 1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Null Fraction = 0, for NOTNULL column <br> &nbsp; &nbsp; Null fraction + n_distinct (after converting to positive ratio format) &le 1 </HTML> ");

		labelAvgWidth.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAvgWidth.setText("Avg. Width:");
		labelAvgWidth.setToolTipText("<html> AVG WIDTH(integer): <br /> Average space (in bytes) required for the column.  </html>");

		textAvgWidth.setToolTipText("<HTML>Avg Width: <br> Datatype: NUMBER <br> Structural Constraint: <br> &nbsp; &nbsp; Avg Width &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML> ");

		labelCorrelation.setFont(new java.awt.Font("Cambria", 0, 14));
		labelCorrelation.setText("Correlation:");
		labelCorrelation.setToolTipText("<html> Correlation (double): <br />Statistical correlation between physical row ordering and logical ordering of the column values.  </html>");

		textCorrelation.setToolTipText("<HTML>Correlation:<br> Datatype: DOUBLE <br> Structural Constraint: <br> &nbsp; &nbsp; null or -1 &le Correlation &le 1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; NILL </HTML> ");

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3Layout.setHorizontalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(labelColCard)
						.addComponent(labelAvgWidth))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(textAvgWidth, GroupLayout.DEFAULT_SIZE, 209, Short.MAX_VALUE)
						.addComponent(textColCard, GroupLayout.DEFAULT_SIZE, 209, Short.MAX_VALUE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(lableNullCount)
						.addComponent(labelCorrelation))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING, false)
						.addComponent(textNullCount)
						.addComponent(textCorrelation, GroupLayout.DEFAULT_SIZE, 194, Short.MAX_VALUE))
					.addContainerGap())
		);
		jPanel3Layout.setVerticalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelColCard)
						.addComponent(textColCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(lableNullCount)
						.addComponent(textNullCount, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelAvgWidth)
						.addComponent(textAvgWidth, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelCorrelation)
						.addComponent(textCorrelation, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
		);
		jPanel3.setLayout(jPanel3Layout);

		jPanel6.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel6.setOpaque(false);

		labelNumFreqBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNumFreqBuckets.setText("Set the number of Buckets:");
		labelNumFreqBuckets.setEnabled(false);

		textNumFreqBuckets.setText(PostgresColumnStatistics.DefaultFrequencyBucketSize+"");
		textNumFreqBuckets.setEnabled(false);

		buttonCreateFreqBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonCreateFreqBuckets.setText("Create");
		buttonCreateFreqBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateFreqBucketsActionPerformed(evt);
			}
		});

		checkFreqHistWrite.setBackground(new java.awt.Color(246, 247, 235));
		checkFreqHistWrite.setFont(new java.awt.Font("Cambria", 0, 14));
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
		buttonFreqhistUpload.setText("Upload");
		buttonFreqhistUpload.setToolTipText("Upload Frequency Distribution from file.");
		buttonFreqhistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonFreqhistUploadActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel6Layout = new javax.swing.GroupLayout(jPanel6);
		jPanel6Layout.setHorizontalGroup(
			jPanel6Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel6Layout.createSequentialGroup()
					.addGroup(jPanel6Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(Alignment.TRAILING, jPanel6Layout.createSequentialGroup()
							.addContainerGap()
							.addGroup(jPanel6Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(buttonFreqhistUpload, Alignment.TRAILING, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
								.addComponent(textFreqHistWrite, GroupLayout.DEFAULT_SIZE, 252, Short.MAX_VALUE)
								.addComponent(checkFreqHistWrite, GroupLayout.DEFAULT_SIZE, 248, Short.MAX_VALUE)))
						.addGroup(Alignment.TRAILING, jPanel6Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(labelNumFreqBuckets)
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(textNumFreqBuckets, GroupLayout.DEFAULT_SIZE, 49, Short.MAX_VALUE))
						.addGroup(Alignment.TRAILING, jPanel6Layout.createSequentialGroup()
							.addContainerGap(144, Short.MAX_VALUE)
							.addComponent(buttonCreateFreqBuckets, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel6Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 252, Short.MAX_VALUE)))
					.addContainerGap())
		);
		jPanel6Layout.setVerticalGroup(
			jPanel6Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel6Layout.createSequentialGroup()
					.addGap(35)
					.addGroup(jPanel6Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelNumFreqBuckets)
						.addComponent(textNumFreqBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(29)
					.addComponent(buttonCreateFreqBuckets)
					.addGap(18)
					.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 4, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(checkFreqHistWrite)
					.addGap(8)
					.addComponent(textFreqHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(buttonFreqhistUpload)
					.addContainerGap(26, Short.MAX_VALUE))
		);
		jPanel6.setLayout(jPanel6Layout);

		tableFrqHist.setFont(new java.awt.Font("Tahoma", 0, 14));
		tableFrqHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"Most Common Col Values", "Most Commom Freqs"
				}
				));
		for(int i=0;i<DB2ColumnStatistics.DefaultFrequencyBucketSize;i++){
			String[] data = {"",""};
			((DefaultTableModel) tableFrqHist.getModel()).addRow(data);
		}
		tableFrqHist.setToolTipText("<HTML> Frequency Distribution (Most Common Vals, Most Common Freqs): <br>Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : Value whose type is same as column type <br>  &nbsp; &nbsp; FREQ : DOUBLE : 0 &lt FREQ &le 1 <br>  Consistency Constraint: <br> &nbsp; &nbsp; Sum (FREQ's) &le 1 <br> &nbsp; &nbsp; Number of buckets &le n_distinct (after conversion) </HTML>");
		jScrollPane4.setViewportView(tableFrqHist);

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4Layout.setHorizontalGroup(
			jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
					.addContainerGap()
					.addComponent(jPanel6, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(jScrollPane4, GroupLayout.DEFAULT_SIZE, 865, Short.MAX_VALUE)
					.addContainerGap())
		);
		jPanel4Layout.setVerticalGroup(
			jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(Alignment.TRAILING, jPanel4Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel4Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(jScrollPane4, Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 273, Short.MAX_VALUE)
						.addComponent(jPanel6, Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 273, Short.MAX_VALUE))
					.addContainerGap())
		);
		jPanel4.setLayout(jPanel4Layout);

		jTabbedPane1.addTab("Frequency Values", jPanel4);

		jPanel7.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel7.setOpaque(false);

		labelNumQuantBuckets.setFont(new java.awt.Font("Cambria", 0, 14));
		labelNumQuantBuckets.setText("Set the number of Buckets:");
		labelNumQuantBuckets.setEnabled(false);

		textNumQuantBuckets.setText(PostgresColumnStatistics.DefaultHistogramBoundSize+"");
		textNumQuantBuckets.setEnabled(false);

		buttonCreateQuantBuckets.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonCreateQuantBuckets.setText("Create ");
		buttonCreateQuantBuckets.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonCreateQuantBucketsActionPerformed(evt);
			}
		});

		checkQuantHistWrite.setBackground(new java.awt.Color(247, 246, 235));
		checkQuantHistWrite.setFont(new java.awt.Font("Cambria", 0, 14));
		checkQuantHistWrite.setText("Write to File");
		checkQuantHistWrite.setToolTipText("Write Quantile Distribution to file.");
		checkQuantHistWrite.setOpaque(false);
		checkQuantHistWrite.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				checkQuantHistWriteActionPerformed(evt);
			}
		});

		textQuantHistWrite.setEnabled(false);

		buttonQuantHistUpload.setFont(new java.awt.Font("Tahoma", 0, 12));
		buttonQuantHistUpload.setText("Upload");
		buttonQuantHistUpload.setToolTipText("Upload Quantile Distribution from file.");
		buttonQuantHistUpload.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonQuantHistUploadActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel7Layout = new javax.swing.GroupLayout(jPanel7);
		jPanel7Layout.setHorizontalGroup(
			jPanel7Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel7Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
							.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
								.addGroup(jPanel7Layout.createSequentialGroup()
									.addGroup(jPanel7Layout.createParallelGroup(Alignment.LEADING)
										.addGroup(jPanel7Layout.createSequentialGroup()
											.addComponent(labelNumQuantBuckets)
											.addPreferredGap(ComponentPlacement.RELATED)
											.addComponent(textNumQuantBuckets, GroupLayout.DEFAULT_SIZE, 49, Short.MAX_VALUE))
										.addComponent(textQuantHistWrite, GroupLayout.DEFAULT_SIZE, 252, Short.MAX_VALUE))
									.addContainerGap())
								.addGroup(jPanel7Layout.createSequentialGroup()
									.addComponent(checkQuantHistWrite, GroupLayout.DEFAULT_SIZE, 248, Short.MAX_VALUE)
									.addGap(16))
								.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
									.addComponent(jSeparator5, GroupLayout.PREFERRED_SIZE, 252, Short.MAX_VALUE)
									.addContainerGap()))
							.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
								.addComponent(buttonCreateQuantBuckets, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
								.addContainerGap()))
						.addGroup(Alignment.TRAILING, jPanel7Layout.createSequentialGroup()
							.addComponent(buttonQuantHistUpload, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE)
							.addContainerGap())))
		);
		jPanel7Layout.setVerticalGroup(
			jPanel7Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel7Layout.createSequentialGroup()
					.addGap(35)
					.addGroup(jPanel7Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelNumQuantBuckets)
						.addComponent(textNumQuantBuckets, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(31)
					.addComponent(buttonCreateQuantBuckets)
					.addGap(18)
					.addComponent(jSeparator5, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(checkQuantHistWrite)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(textQuantHistWrite, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addComponent(buttonQuantHistUpload)
					.addContainerGap(28, Short.MAX_VALUE))
		);
		jPanel7.setLayout(jPanel7Layout);

		tableQuantHist.setFont(new java.awt.Font("Tahoma", 0, 14));
		tableQuantHist.setModel(new javax.swing.table.DefaultTableModel(
				new Object [][] {},
				new String [] {
						"COLVAUE"
				}
				));
		for(int i=0;i<DB2ColumnStatistics.DefaultQuantileBucketSize;i++){
			String[] data = {""};
			((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
		}
		tableQuantHist.setToolTipText("<HTML> Height Balanced Distribution (Histogram Bounds): <br>Structural Constraint: <br> &nbsp; &nbsp; COLVALUE : Value whose type is same as column type <br>  Consistency Constraint: <br> &nbsp; &nbsp; COLVALUE must be increasing. </HTML> ");
		jScrollPane3.setViewportView(tableQuantHist);

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5.setLayout(jPanel5Layout);
		jPanel5Layout.setHorizontalGroup(
				jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jPanel7, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addGap(18, 18, 18)
						.addComponent(jScrollPane3, javax.swing.GroupLayout.DEFAULT_SIZE, 754, Short.MAX_VALUE)
						.addContainerGap())
				);
		jPanel5Layout.setVerticalGroup(
				jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel5Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
								.addComponent(jScrollPane3, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.PREFERRED_SIZE, 0, Short.MAX_VALUE)
								.addComponent(jPanel7, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
								.addContainerGap())
				);

		jTabbedPane1.addTab("Histogram Bound", jPanel5);

		Update.setText("Construct");
		Update.setToolTipText("Input is over. Construct database.");
		Update.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				UpdateActionPerformed(evt);
			}
		});

		jPanel8.setBorder(javax.swing.BorderFactory.createEtchedBorder());
		jPanel8.setOpaque(false);

		labelIndCard.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndCard.setText("Index Cardinality:");
		labelIndCard.setToolTipText("Cardinality of the index.");

		textIndCard.setToolTipText("<HTML>Number of Tuples (IndTuples) : <br> Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; 0 &le IndTuples&le 9223372036854775807 <br>  Consistency Constraint:  <br> &nbsp; &nbsp; IndTuples = RelTuples </HTML>");

		labelIndPages.setFont(new java.awt.Font("Cambria", 0, 14));
		labelIndPages.setText("Index Pages:");
		labelIndPages.setToolTipText("Number of index pages.");

		textIndPages.setToolTipText("<HTML> Number of Pages (IndPages):<br>Datatype: BIGINT <br> Structural Constraint: <br> &nbsp; &nbsp; InfPages &ge 0 <br>  Consistency Constraint: <br> &nbsp; &nbsp; IndPages = RelPages </HTML>");

		javax.swing.GroupLayout jPanel8Layout = new javax.swing.GroupLayout(jPanel8);
		jPanel8Layout.setHorizontalGroup(
			jPanel8Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel8Layout.createSequentialGroup()
					.addContainerGap()
					.addComponent(labelIndCard)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(textIndCard, GroupLayout.PREFERRED_SIZE, 187, GroupLayout.PREFERRED_SIZE)
					.addGap(28)
					.addComponent(labelIndPages)
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(textIndPages, GroupLayout.PREFERRED_SIZE, 165, GroupLayout.PREFERRED_SIZE)
					.addContainerGap(569, Short.MAX_VALUE))
		);
		jPanel8Layout.setVerticalGroup(
			jPanel8Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel8Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel8Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelIndCard)
						.addComponent(textIndCard, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelIndPages)
						.addComponent(textIndPages, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
		);
		jPanel8.setLayout(jPanel8Layout);

		labelRelationName.setFont(new java.awt.Font("Cambria", 0, 14));
		labelRelationName.setText("Relation Name:");

		comboBoxRelations.setToolTipText("Selected Relations for Construt");
		comboBoxRelations.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxRelationsActionPerformed(evt);
			}
		});

		labelAttribute.setFont(new java.awt.Font("Cambria", 0, 14));
		labelAttribute.setText("Attribute Name:");

		comboBoxAttribute.setToolTipText("Attributes of selected relation");
		comboBoxAttribute.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				comboBoxAttributeActionPerformed(evt);
			}
		});

		buttonUpdateColumn.setText("Update Column");
		buttonUpdateColumn.setToolTipText("Updates the column level metadata");
		buttonUpdateColumn.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				buttonUpdateColumnActionPerformed(evt);
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
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(jPanel8, GroupLayout.DEFAULT_SIZE, 1200, Short.MAX_VALUE)
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 132, GroupLayout.PREFERRED_SIZE)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(buttonResetColValues)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(buttonUpdateColumn)
							.addPreferredGap(ComponentPlacement.UNRELATED)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 120, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
								.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 1200, Short.MAX_VALUE)
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
										.addGroup(jPanel1Layout.createSequentialGroup()
											.addComponent(labelRelationName)
											.addPreferredGap(ComponentPlacement.UNRELATED)
											.addComponent(comboBoxRelations, 0, 379, Short.MAX_VALUE))
										.addComponent(jPanel2, GroupLayout.DEFAULT_SIZE, 502, Short.MAX_VALUE))
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
										.addGroup(jPanel1Layout.createSequentialGroup()
											.addComponent(labelAttribute)
											.addGap(18)
											.addComponent(comboBoxAttribute, 0, 554, Short.MAX_VALUE))
										.addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, 680, Short.MAX_VALUE)))
								.addGroup(jPanel1Layout.createSequentialGroup()
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(labelIndexStats)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(checkIndexStats)
									.addGap(18)
									.addComponent(labelIndexStatsTip, GroupLayout.PREFERRED_SIZE, 380, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED, 189, Short.MAX_VALUE)
									.addComponent(buttonQuantHistShowGraph)))))
					.addContainerGap())
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelRelationName)
						.addComponent(comboBoxRelations, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(comboBoxAttribute, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelAttribute))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING, false)
						.addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addComponent(jPanel2, GroupLayout.PREFERRED_SIZE, 68, Short.MAX_VALUE))
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addGap(24)
							.addComponent(jSeparator2, GroupLayout.PREFERRED_SIZE, 10, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel1Layout.createSequentialGroup()
							.addPreferredGap(ComponentPlacement.RELATED)
							.addComponent(jTabbedPane1, GroupLayout.DEFAULT_SIZE, 324, Short.MAX_VALUE)))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(labelIndexStatsTip)
						.addComponent(labelIndexStats)
						.addComponent(checkIndexStats, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addComponent(buttonQuantHistShowGraph, GroupLayout.PREFERRED_SIZE, 33, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jPanel8, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(12)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
							.addComponent(Update, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
							.addComponent(buttonUpdateColumn, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE))
						.addComponent(buttonResetColValues, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE)
						.addComponent(backButton, GroupLayout.PREFERRED_SIZE, 40, GroupLayout.PREFERRED_SIZE))
					.addContainerGap())
		);
		jPanel1.setLayout(jPanel1Layout);

		getContentPane().add(jPanel1, java.awt.BorderLayout.CENTER);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void checkIndexStatsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_checkIndexStatsActionPerformed
		if (checkIndexStats.isSelected()) {
			this.textIndCard.setEnabled(true);
			this.textIndPages.setEnabled(true);
		} else {
			this.textIndCard.setEnabled(false);
			this.textIndPages.setEnabled(false);
		}
	}//GEN-LAST:event_checkIndexStatsActionPerformed

	private void backButtonActionPerformed(java.awt.event.ActionEvent evt) {
		String[] relations = new String[relUpdated.size()];
		int i = 0;
		for(String rel : relUpdated.keySet()){
			relations[i] = rel;
			i++;
		}
		new ConstructMode(DBConstants.POSTGRES , relations, database).setVisible(true);
		this.dispose();				
	}

	private void buttonQuantHistUploadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistUploadActionPerformed
		String filePath;
		try {
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Postgres";
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
					Constants.CPrintToConsole(line, Constants.DEBUG_SECOND_LEVEL_Information);
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
		if(bucketSize >= 0 && bucketSize <= PostgresColumnStatistics.MaxFrequencyBucketSize) {
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
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+PostgresColumnStatistics.MaxFrequencyBucketSize+"]." , "CODD - Error", 0);
		}
	}//GEN-LAST:event_buttonCreateFreqBucketsActionPerformed


	private void buttonQuantHistShowGraphActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonQuantHistShowGraphActionPerformed
		/**
		 * Read all the information from the table and show it in the graph.
		 */
		boolean db2Hist = false;
		boolean unique = false;
		boolean noDistinct = true;
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
		double[] f = new double[count];
		double[] d = new double[count];
		int realCnt = 0;

		while(count > 0)
		{
			String s1 = (String)tableQuantHist.getModel().getValueAt(i, 0);
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
			ArrayList<BucketItem> buckets = new ArrayList();
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
				Constants.CPrintToConsole("(" + prevVal.getString() + ", " + val.getString() + " ):: " + f[i] + " (" + freqPercent + ")", Constants.DEBUG_FIRST_LEVEL_Information);
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
		if(bucketSize >= 0 && bucketSize <= PostgresColumnStatistics.MaxHistogramBoundSize) {
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
		} else {
			JOptionPane.showMessageDialog(null, "Entered Num of buckets is not a valid value. The value should lie in [0, "+PostgresColumnStatistics.MaxHistogramBoundSize+"]." , "CODD - Error", 0);
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
							Double temp1 = Double.parseDouble(valCount);
							if (temp1 < 0) { // Convert to DB2 style format
								Double temp2 = temp1 * stats.get(this.getCurrentRelation()).getRelationStatistics().getCardinality().longValue();
								valCount = "" + temp2.longValue();
							}
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
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Postgres";
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
						if (colValue != null) {
							temp += colValue+"\n";
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
					String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Postgres";
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
			String pathName = Constants.WorkingDirectory+Constants.PathSeparator+"Histograms"+Constants.PathSeparator+"Postgres";
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
					Double temp1 = Double.parseDouble(vals[1]);
					/*if(temp1 > 0) { // Convert to DB2 style format
						vals[1] = "" + temp1 / stats.get(this.getCurrentRelation()).getRelationStatistics().getCardinality().longValue();
					} */
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
		if(rel!=null && !rel.trim().isEmpty()){
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
		else{
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
		if(col != null && !col.trim().isEmpty())
		{
			String relationName = this.getCurrentRelation();
			PostgresColumnStatistics columnStatistics = (PostgresColumnStatistics) this.stats.get(relationName).getColumnStatistics(col);
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
	private javax.swing.JSeparator jSeparator1;
	private javax.swing.JSeparator jSeparator2;
	private javax.swing.JSeparator jSeparator5;
	private javax.swing.JTabbedPane jTabbedPane1;
	private javax.swing.JLabel labelAttribute;
	private javax.swing.JLabel labelAvgWidth;
	private javax.swing.JLabel labelCard;
	private javax.swing.JLabel labelColCard;
	private javax.swing.JLabel labelCorrelation;
	private javax.swing.JLabel labelIndCard;
	private javax.swing.JLabel labelIndPages;
	private javax.swing.JLabel labelIndexStats;
	private javax.swing.JLabel labelIndexStatsTip;
	private javax.swing.JLabel labelNPages;
	private javax.swing.JLabel labelNumFreqBuckets;
	private javax.swing.JLabel labelNumQuantBuckets;
	private javax.swing.JLabel labelRelationName;
	private javax.swing.JLabel lableNullCount;
	private javax.swing.JTable tableFrqHist;
	private javax.swing.JTable tableQuantHist;
	private javax.swing.JTextField textAvgWidth;
	private javax.swing.JTextField textCard;
	private javax.swing.JTextField textColCard;
	private javax.swing.JTextField textCorrelation;
	private javax.swing.JTextField textFreqHistWrite;
	private javax.swing.JTextField textIndCard;
	private javax.swing.JTextField textIndPages;
	private javax.swing.JTextField textNPages;
	private javax.swing.JTextField textNumFreqBuckets;
	private javax.swing.JTextField textNumQuantBuckets;
	private javax.swing.JTextField textNullCount;
	private javax.swing.JTextField textQuantHistWrite;
	JButton backButton;
	// End of variables declaration//GEN-END:variables

	@Override
	public void setHistogram(ArrayList<BucketItem> buckets, boolean noDistinct) {

		//Remove 0th row quantHistRows times
		for (int r = 0; r < quantHistRows; r++) {
			((DefaultTableModel) tableQuantHist.getModel()).removeRow(0);
		}
		this.quantHistRows = buckets.size();
		this.textNumQuantBuckets.setText(buckets.size()+"");
		for (int i = 0; i < buckets.size(); i++) {
			BucketItem bucket = buckets.get(i);
			Constants.CPrintToConsole("(" + bucket.getLValue().getString() + "," + bucket.getValue().getString() + ") :: " + bucket.getFreq() + " (" + bucket.getFreqPercent() + ")" + " : " + bucket.getDistinctCount() + " (" + bucket.getDistinctCountPercent() + ")", Constants.DEBUG_FIRST_LEVEL_Information);
			String colValue = bucket.getValue().getString();
			String[] data = {colValue};
			((DefaultTableModel) tableQuantHist.getModel()).addRow(data);
		}
	}
}
