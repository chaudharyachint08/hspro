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

package iisc.dsl.codd.graphhistogram;

import iisc.dsl.codd.client.ConstructModeFrame;
import iisc.dsl.codd.ds.DataType;

import com.visutools.nav.bislider.ColorisationEvent;
import com.visutools.nav.bislider.ColorisationListener;

import iisc.dsl.codd.ds.Constants;

import java.util.ArrayList;

import javax.swing.UIManager;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Paint;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Hashtable;

import javax.swing.AbstractAction;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.KeyStroke;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartMouseEvent;
import org.jfree.chart.ChartMouseListener;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.ChartEntity;
import org.jfree.chart.entity.XYItemEntity;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.xy.XYIntervalSeries;
import org.jfree.data.xy.XYIntervalSeriesCollection;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.awt.Font;

import javax.swing.SwingConstants;

import java.math.BigDecimal;
import java.math.RoundingMode;

import javax.swing.JPanel;

import java.awt.GridBagLayout;

import javax.swing.JFormattedTextField;
import javax.swing.text.NumberFormatter;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeEvent;

import javax.swing.JComboBox;

import java.awt.event.ActionListener;

import javax.swing.DefaultComboBoxModel;

import java.awt.FlowLayout;


/**
 * This module creates a histogram graph and allows the user to modify the histogram through GUI.
 * There are two modes of operation.
 *  1) Frequency Mode. Operates on Frequency.
 *      1) Bucket Frequency, boundary Change is allowed.
 *      2) Add / Delete Bucket at front / end
 *      3) Split, Merge
 *      4) Distribute Excess
 *  2) DistinctCoutn Mode. Operates on distinct count.
 *      1) Bucket Frequency is allowed. No boundary Change
 *      2)  No Add / Delete Bucket at front / end
 *      3) No Split, Merge
 *      4) Distribute Excess
 *  
 * The GUI to modify Frequency histograms is not provided and left as future work.
 * @author dsladmin
 */


public class GraphHistogram extends javax.swing.JFrame implements ChartMouseListener, MouseListener, MouseMotionListener, ColorisationListener{

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 7252263303526187521L;
	/** GUI Variables **/

	// Specifies the number of intervals to be shown on the verticalSlider
	private int verticalSliderTickerCount = 10;
	// Graph Name to be shown on the top of the graph.
	private String graphName;
	// Graph X axis name to be shown on the graph.
	private String graphX;
	// Graph Y axis name to be shown on the graph.
	private String graphY;
	// Column Name for which histogram is drawn.
	private String colName; //Constructor Parameter
	// Data type of Column for which histogram is drawn.
	private String dataType; //Constructor Parameter
	/**
	 * Specifies the whether chart is being created now. If it is set, horizontalSlider
	 * Change events are not handled. It avoids recursive handling of change Event, when
	 * slider changed inside horizontalSlider change event function.
	 */
	boolean doNotFireHorizontalSliderChangeEvent;

	/** Data Structures **/

	// Total Number of rows in the histogram
	private long totalRowsCount; //Constructor Parameter
	/*
	 * Total Number of Distinct rows in the histogram
	 * totalDistinctCount <= totalCount
	 */
	private long totalDistinctRowsCount; //Constructor Parameter
	/**
	 * Boolean variable specifies whether the GraphHistogram Instance is of DB2 or not.
	 * For DB2, the frequency, distinct count can be of Integer only. Setting this variable
	 * to true, does rounding off for the frequencies of buckets after each operation in the graph.
	 */
	private boolean db2Hist;  // Constructor Parameter
	// List of buckets in the histogram.
	private ArrayList<BucketItem> buckets; //Constructor Parameter
	// Minimum Value of the histogram
	private DataType minValue; //Constructor Parameter
	// Maximum Value of the histogram
	private DataType maxValue; //Constructor Parameter
	// Specifies the selecetd Bucket in BUCKET_CHANGE_MODE.
	public int selectedBucket;
	// List the selecetd Buckets in DISTRIBUTE_MODE, MERGE_MODE.
	public ArrayList<Integer> selectedBucketsList;
	// Specifes the Excess / Less precent of freq / distinct count percentage in the graph histogram.
	private double excessPercent;
	// DecimalFormat class used to keep the freq / distinct count percentage with two precision.
	private DecimalFormat decimalFormat;
	// Specifies the minimum Height for the buckets (in percentage). One can decrease the bucket below this size.
	private double minHeight; // Constructor Parameter
	// Specifies the minimum Width for the buckets. One can decrease the bucket below this size.
	private DataType minWidth; // Construcor Parameter


	/** Operating Modes
	 *  1) Global Mode - Frequency [With / Without] BB / Only BB / Distinct Count
	 *  2) Local Mode - Bucket Change + Split / Merge ..
	 **/

	/** Global Mode. Frequency / Distinct Count Mode. **/
	/**
	 * Frequency of histogram is being modified.
	 * Bucket Boundary change is allowed.
	 * Supporting local Modes: All Local Modes
	 */
	public final static int FREQUENCY_MODE_WITHBB = 0;
	/**
	 * Frequency of histogram is being modified.
	 * Bucket Boundary change is NOT allowed.
	 * Supporting local Modes:
	 * BUCKET_CHANGE_MODE [No Boundary Change]
	 * DISTRIBUTE MODE
	 */
	public final static int FREQUENCY_MODE_WITHOUTBB = 1;
	/**
	 * Bucket Boundary of the Histogram is being modified.
	 * No Frequency Change.
	 * Supporting local Modes:
	 * All Local Modes. But do not change height of the buckets.
	 */
	public final static int FREQUENCY_MODE_ONLYBB = 2;
	/**
	 * Distinct Count of histogram is being modified.
	 * Supporting local Modes:
	 * BUCKET_CHANGE_MODE [No Boundary Change]
	 * No Split, Merge, Add/Remove Bucket
	 * DISTRIBUTE MODE
	 */
	public final static int DISTINCT_COUNT_MODE = 3;

	// Specifes the current Global Mode.
	private int globalMode; // Construcor Parameter

	/**
	 * At any given time, the graph can be in any of the following local operating modes.
	 */
	// Bucket Boundary, freq / distinct count percent are being modified.
	public final static int BUCKET_CHANGE_MODE = 0;
	// Buckets of histogram are being selected to distribute excessPercent.
	public final static int DISTRIBUTE_MODE = 1;
	// Buckets of histogram are being selected for Merge.
	public final static int MERGE_MODE = 2;

	// Specifes the current operating local Mode.
	public int localMode;

	// Specifes the column constraint. True for Unique, PK column.
	public boolean uniqueColumn; // Constructor Parameter

	// Specifes the whether Distinct Mode Graph is required or not.
	public boolean noDistinct; // Constructor Parameter

	// If Garph Histogram does not support the given data type, uniform bucket of this length will be created.
	public int UniformBucketInterval = 2;

	// Construct mode frame Object.
	public ConstructModeFrame constructModeGUI;

	/**
	 * Constructor for Graph Histogram.
	 * @param constructModeGUI Object to ConstructMode GUI. Use the callback function to set the modified values to the ConstructMode GUI
	 * @param globalMode Initial global mode for the Graph to start
	 * @param totalCount Total number of values (rows) in the column
	 * @param totalDistinctCount Total number of distinct values in the column
	 * @param buckets List of BucketItems
	 * @param minValue Minimum value (Left Margin) of the histogram
	 * @param maxValue Maximum value (Right Margin) of the histogram
	 * @param minHeight Minimum Height for any of the histogram bucket
	 * @param minWidth Minimum Width for any of the histogram bucket
	 * @param colName Column name, for which the histogram graph is built
	 * @param dataType DataType object of the column
	 * @param db2Hist true, if histogram graph is for DB2.
	 * @param unique true, if histogram graph column has UNIQUE constraint.
	 * @param noDistinct true, if GraphHistogram module needs to show DistinctMode Histogram Graph.
	 */
	public GraphHistogram(ConstructModeFrame constructModeGUI, int globalMode, long totalCount, long totalDistinctCount, ArrayList<BucketItem> buckets, DataType minValue, DataType maxValue, double minHeight, DataType minWidth, String colName, String dataType, boolean db2Hist, boolean unique, boolean noDistinct) {
		super("Graph Histogram Construct Mode");
		this.constructModeGUI = constructModeGUI;
		selectedBucket = -1;
		selectedBucketsList = null;
		
		java.text.NumberFormat numberFormat = java.text.NumberFormat
				.getIntegerInstance();
		NumberFormatter formatter = new NumberFormatter(numberFormat);
		formatter.setMinimum(new Integer(0));
		formatter.setMaximum(new Integer(100));

		NumberFormat f = NumberFormat.getNumberInstance();

		formattedTextField = new JFormattedTextField(formatter);
		leftBoundary = new JFormattedTextField(f);
		rightBoundary = new JFormattedTextField(f);
		
		initComponents();
		setGlobalMode(globalMode);
		this.totalRowsCount = totalCount;
		this.totalDistinctRowsCount = totalDistinctCount; //totalDistinctCount <= totalCount
		this.buckets = buckets;
		this.minValue = minValue;
		this.maxValue = maxValue;
		this.minHeight = minHeight;
		this.minWidth = minWidth;
		if(globalMode == GraphHistogram.FREQUENCY_MODE_WITHBB || globalMode == GraphHistogram.FREQUENCY_MODE_WITHOUTBB || globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB)
		{
			this.graphName = "Row Count Histogram";
			this.graphX = "Bucket Boundary";
			this.graphY = "Row Count %";
		}
		else
		{
			this.graphName = "Distinct Count Histogram";
			this.graphX = "Bucket Boundary";
			this.graphY = "Distinct Count %";
		}

		this.colName = colName;
		this.dataType = dataType;
		this.uniqueColumn = unique;
		this.db2Hist = db2Hist;
		this.noDistinct = noDistinct;
		// For Veritcal Slider ToolTip
		item = new JMenuItem();
		pop.add(item);
		pop.setDoubleBuffered( true );

		setVerticalSliderSettings(0, 10000);
		setHorizontalSliderSettings(minValue, minValue, maxValue, maxValue);
		verticalSlider.setEnabled(false);
		
		verticalSlider.addChangeListener(new javax.swing.event.ChangeListener() {
			public void stateChanged(javax.swing.event.ChangeEvent evt) {
				verticalSliderStateChanged(evt);
			}
		});
		
		formattedTextField.addPropertyChangeListener(new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				formattedTextFieldPropertyChange(evt);
			}
		});
		formattedTextField.setValue(new Integer(50));
		formattedTextField.setColumns(5); //get some space
		formattedTextField.getInputMap().put(KeyStroke.getKeyStroke(
                KeyEvent.VK_ENTER, 0),
                "check");
		formattedTextField.getActionMap().put("check", new AbstractAction() {
			public void actionPerformed(ActionEvent e) {
				if (!formattedTextField.isEditValid()) { // The text is invalid.
					Toolkit.getDefaultToolkit().beep();
					formattedTextField.selectAll();
				} else
					try { // The text is valid,
						formattedTextField.commitEdit(); // so use it.
					} catch (java.text.ParseException exc) {
					}
			}
		});
		
		GridBagConstraints gbc_formattedTextField = new GridBagConstraints();
		gbc_formattedTextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_formattedTextField.gridx = 0;
		gbc_formattedTextField.gridy = 1;
		panel.add(formattedTextField, gbc_formattedTextField);
		formattedTextField.setEnabled(false);
		horizontalSlider.setEnabled(false);
		
		leftBoundary.addPropertyChangeListener("value", new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				leftBoundaryPropertyChange(evt);
			}
		});
		leftBoundary.setColumns(25);
		leftBoundary.getInputMap().put(KeyStroke.getKeyStroke(
                KeyEvent.VK_ENTER, 0),
                "check");
		leftBoundary.getActionMap().put("check", new AbstractAction() {
			public void actionPerformed(ActionEvent e) {
				if (!leftBoundary.isEditValid()) { // The text is invalid.
					Toolkit.getDefaultToolkit().beep();
					leftBoundary.selectAll();
				} else
					try { // The text is valid,
						leftBoundary.commitEdit(); // so use it.
					} catch (java.text.ParseException exc) {
					}
			}
		});
		leftBoundary.setHorizontalAlignment(SwingConstants.CENTER);
		GridBagConstraints gbc_leftBoundary = new GridBagConstraints();
		gbc_leftBoundary.anchor = GridBagConstraints.EAST;
		gbc_leftBoundary.insets = new Insets(0, 0, 0, 5);
		gbc_leftBoundary.gridx = 0;
		gbc_leftBoundary.gridy = 1;
		panel_1.add(leftBoundary, gbc_leftBoundary);
		
		rightBoundary.addPropertyChangeListener("value", new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				rightBoundaryPropertyChange(evt);
			}
		});
		rightBoundary.setColumns(25);
		rightBoundary.getInputMap().put(KeyStroke.getKeyStroke(
                KeyEvent.VK_ENTER, 0),
                "check");
		rightBoundary.getActionMap().put("check", new AbstractAction() {
			public void actionPerformed(ActionEvent e) {
				if (!rightBoundary.isEditValid()) { // The text is invalid.
					Toolkit.getDefaultToolkit().beep();
					rightBoundary.selectAll();
				} else
					try { // The text is valid,
						rightBoundary.commitEdit(); // so use it.
					} catch (java.text.ParseException exc) {
					}
			}
		});
		rightBoundary.setHorizontalAlignment(SwingConstants.CENTER);
		GridBagConstraints gbc_rightBoundary = new GridBagConstraints();
		gbc_rightBoundary.anchor = GridBagConstraints.WEST;
		gbc_rightBoundary.insets = new Insets(0, 0, 0, 5);
		gbc_rightBoundary.gridx = 1;
		gbc_rightBoundary.gridy = 1;
		panel_1.add(rightBoundary, gbc_rightBoundary);
		
		setBoundarySettings(minValue, minValue, maxValue, maxValue);
		leftBoundary.setEnabled(false);
		rightBoundary.setEnabled(false);
		
		finishButton = new javax.swing.JButton();

		finishButton.setText("Finish");
		finishButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				finishButtonActionPerformed(evt);
			}
		});
		
		panel_2 = new JPanel();
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.insets = new Insets(0, 0, 0, 5);
		gbc_panel_2.gridx = 4;
		gbc_panel_2.gridy = 6;
		getContentPane().add(panel_2, gbc_panel_2);
		
		distLabel = new JLabel("Distribution");
		panel_2.add(distLabel);
		
		distList = new JComboBox();
		distList.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				setEnableDistribution(true);
			}
		});
		distList.setModel(new DefaultComboBoxModel(new String[] {"None", "Standard Normal", "Normal", "Poisson", "Negative Skew", "Positive Skew"}));
		panel_2.add(distList);
		
		
		gridBagConstraints_11 = new java.awt.GridBagConstraints();
		gridBagConstraints_11.anchor = GridBagConstraints.EAST;
		gridBagConstraints_11.gridx = 6;
		gridBagConstraints_11.gridy = 6;
		gridBagConstraints_11.insets = new java.awt.Insets(5, 0, 10, 10);
		getContentPane().add(finishButton, gridBagConstraints_11);
		// Listener for change in horizontal slider
		horizontalSlider.addColorisationListener(this);
		horizontalSlider.addMouseListener(this);
		doNotFireHorizontalSliderChangeEvent = false;
		setMode(GraphHistogram.BUCKET_CHANGE_MODE);
		// Update the excess percent.
		double total = 0;
		for(int b=0;b<buckets.size();b++)
		{
			total = total + getPercent(buckets.get(b));
		}
		excessPercent = total - 100;
		decimalFormat = new DecimalFormat("#.##");
		excessPercent =  Double.valueOf(decimalFormat.format(excessPercent));
		setExcessLabelFinishButton();
		initializeUndoRedo();
		// For Frame
		addMouseListener(this);
		addMouseMotionListener(this);
		
		if(dataType == DataType.VARCHAR) {
			panel_2.setVisible(false);
			panel_3.setVisible(false);
			panel_4.setVisible(false);
		}
	}

	/**
	 * Function sets the parameters of Vertical Slider.
	 * @param min Minimum Value of the Vertical Slider
	 * @param max Maximum Value of the Vertical Slider
	 */
	private void setVerticalSliderSettings(double min, double max)
	{
		int minVal = (int) Math.floor(min);
		int maxVal = (int) Math.ceil(max);
		verticalSlider.setMinimum(minVal);
		verticalSlider.setMaximum(maxVal);
		int sliderInterval = (maxVal - minVal)/verticalSliderTickerCount;
		// Precision support is for two digits.
		Hashtable<Integer, JLabel> table = new Hashtable<Integer, JLabel>();
		table.put(0, new JLabel("0"));
		table.put(1000, new JLabel("10"));
		table.put(2000, new JLabel("20"));
		table.put(3000, new JLabel("30"));
		table.put(4000, new JLabel("40"));
		table.put(5000, new JLabel("50"));
		table.put(6000, new JLabel("60"));
		table.put(7000, new JLabel("70"));
		table.put(8000, new JLabel("80"));
		table.put(9000, new JLabel("90"));
		table.put(10000, new JLabel("100"));
		verticalSlider.setLabelTable(table);
		verticalSlider.setMajorTickSpacing(sliderInterval);
		verticalSlider.setMinorTickSpacing(1);
		verticalSlider.setPaintTicks(true);
		verticalSlider.setPaintLabels(true);
		// Listener to show the current value when the slider is moving.
		verticalSlider.addMouseListener(this);
		verticalSlider.addMouseMotionListener(this);
	}

	/**
	 * Determines the tooltip to show for the current position in vertical slider.
	 * @param me
	 */
	private void setVerticalSliderToolTip(MouseEvent me)
	{
		Component comp = me.getComponent();
		if(comp instanceof JSlider)
		{
			JSlider slider = (JSlider) comp;
			int sliderValue = slider.getValue();
			String value = ((double)sliderValue/100.0) + " %";
			item.setText(value);
			//limit the tooltip location relative to the slider
			pop.show( verticalSlider, me.getX()+10, me.getY()+20 );
			item.setArmed( false );
		}
	}

    /**
     * Listen to the text field.  This method detects when the
     * value of the text field (not necessarily the same
     * number as you'd get from getText) changes.
     */
    public void formattedTextFieldPropertyChange(PropertyChangeEvent e) {
        if ("value".equals(e.getPropertyName())) {
            Number value = (Number)e.getNewValue();
            if (verticalSlider != null && value != null) {
				int sliderVal = value.intValue() * 100;
				verticalSlider.setValue(sliderVal);
            }
        }
    }
    
    /**
     * Listen to the text field.  This method detects when the
     * value of the text field (not necessarily the same
     * number as you'd get from getText) changes.
     */
    public void leftBoundaryPropertyChange(PropertyChangeEvent e) {
        if ("value".equals(e.getPropertyName())) {
            Number value = (Number)e.getNewValue();
            if (horizontalSlider != null && value != null) {
            	changeBucketBoundary(selectedBucket, value.doubleValue(), Double.parseDouble(rightBoundary.getText()));
            }
        }
    }
    
    /**
     * Listen to the text field.  This method detects when the
     * value of the text field (not necessarily the same
     * number as you'd get from getText) changes.
     */
    public void rightBoundaryPropertyChange(PropertyChangeEvent e) {
        if ("value".equals(e.getPropertyName())) {
            Number value = (Number)e.getNewValue();
            if (horizontalSlider != null && value != null) {
            	changeBucketBoundary(selectedBucket, Double.parseDouble(leftBoundary.getText()), value.doubleValue());
            }
        }
    }
    
    
	private void setBoundarySettings(DataType min, DataType leftValue, DataType rightValue, DataType max)
	{
		if(DataType.isDouble(dataType) || DataType.isInteger(dataType) || DataType.isBigDecimal(dataType) || DataType.isNumeric(dataType))
		{
			if(DataType.isDouble(dataType) || min.isDouble()){
			    leftBoundary.setText(String.valueOf(min.getDouble()));
			    rightBoundary.setText(String.valueOf(max.getDouble()));
			} else if(DataType.isInteger(dataType)){
			    leftBoundary.setText(String.valueOf(min.getInteger()));
			    rightBoundary.setText(String.valueOf(max.getInteger()));
			} else if(DataType.isNumeric(dataType)){
			    leftBoundary.setText(String.valueOf(min.getBigDecimal().doubleValue()));
			    rightBoundary.setText(String.valueOf(max.getBigDecimal().doubleValue()));
			} else{
			    leftBoundary.setText(String.valueOf(min.getBigInteger().longValue()));
			    rightBoundary.setText(String.valueOf(max.getBigInteger().longValue()));
			}
		}
		else // Bucket Boundary can not be changed
		{ // left and right boundary must be disabled.
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
		}
	}

	
	/**
	 * Function sets the parameters of the Horizontal Slider
	 * @param min Minimum Value of the Horizontal Slider
	 * @param leftValue Left arrow will be set at this position
	 * @param rightValue Right arrow will be set at this position
	 * @param max Minimum Value of the Horizontal Slider
	 * Make sure min <= leftValue <= rightValue <= max
	 */
	private void setHorizontalSliderSettings(DataType min, DataType leftValue, DataType rightValue, DataType max)
	{
		if(DataType.isDouble(dataType) || DataType.isInteger(dataType) || DataType.isBigDecimal(dataType))
		{
			double minD, leftValueD, rightValueD, maxD;
			if(DataType.isDouble(dataType) || min.isDouble()){  // Added this extra condition "min.isDouble()", since @getChart() method sets all values to double type
				minD = min.getDouble();
				leftValueD = leftValue.getDouble();
				rightValueD = rightValue.getDouble();
				maxD = max.getDouble();
			} else if(DataType.isInteger(dataType)){
				minD = min.getInteger();
				leftValueD = leftValue.getInteger();
				rightValueD = rightValue.getInteger();
				maxD = max.getInteger();
			} else if(DataType.isNumeric(dataType)){
				minD = min.getBigDecimal().doubleValue();
				leftValueD = leftValue.getBigDecimal().doubleValue();
				rightValueD = rightValue.getBigDecimal().doubleValue();
				maxD = max.getBigDecimal().doubleValue();
			} else{
				minD = min.getBigInteger().longValue();
				leftValueD = leftValue.getBigInteger().longValue();
				rightValueD = rightValue.getBigInteger().longValue();
				maxD = max.getBigInteger().longValue();
			}

			if (minD <= leftValueD && leftValueD <= rightValueD && rightValueD <= maxD) {
				horizontalSlider.setMaximumValue(maxD);
				horizontalSlider.setMinimumValue(minD);
				horizontalSlider.setMaximumColoredValue(rightValueD);
				horizontalSlider.setMinimumColoredValue(leftValueD);
			} else {
				JOptionPane.showMessageDialog(null, "Ranges passed to Horizontal Slider is not Valid.", "CODD Error Message - Action Required", 0);
			}
		}
		else // Bucket Boundary can not be changed
		{ // HorizontalSlider must be disabled.
			/*
			 * Set uniform bucket length.
			 */
			horizontalSlider.setMaximumValue(buckets.size() * UniformBucketInterval);
			horizontalSlider.setMinimumValue(0);
			horizontalSlider.setMaximumColoredValue(0);
			horizontalSlider.setMinimumColoredValue(buckets.size() * UniformBucketInterval);
			horizontalSlider.setEnabled(false);
		}
	}

	private void setEnableDistribution(boolean enabled)
	{
		boolean disabled = false;

		// clear both table entries first
		//clearTableEntries();
		if (enabled && this.distList.getSelectedIndex() == 1) {
			this.distInput1.setEnabled(disabled);
			this.distInput2.setEnabled(disabled);
			this.distLabel1.setText("Mean");
			this.distLabel2.setText("S.D");
			this.distInput1.setValue(new Float(0.0));
			this.distInput2.setValue(new Float(1.0));
		} else if (enabled && this.distList.getSelectedIndex() == 2) {
			this.distInput1.setEnabled(enabled);
			this.distInput2.setEnabled(enabled);
			this.distLabel1.setText("Mean");
			this.distLabel2.setText("S.D");
			this.distInput1.setValue(new Float(0.0));
			this.distInput2.setValue(new Float(1.0));
		} else if (enabled && this.distList.getSelectedIndex() == 3) {
			this.distLabel1.setText("Lambda");
			this.distLabel2.setText("Input 2");
			this.distInput1.setEnabled(enabled);
			this.distInput2.setEnabled(disabled);
			//this.distInput2.setEnabled(enabled);
			this.distInput1.setValue(new Float(0.001));
			this.distInput2.setValue(new Float(0.0));
			//this.distInput2.setValue(new Float(1.0));
		} else if (enabled && this.distList.getSelectedIndex() == 4) {
			this.distLabel1.setText("Skew");
			this.distLabel2.setText("Input 2");
			this.distInput1.setEnabled(enabled);
			this.distInput2.setEnabled(disabled);
			//this.distInput2.setEnabled(enabled);
			this.distInput1.setValue(new Float(0.0));
			this.distInput2.setValue(new Float(0.0));
			//this.distInput2.setValue(new Float(1.0));
		} else if (enabled && this.distList.getSelectedIndex() == 5) {
			this.distLabel1.setText("Skew");
			this.distLabel2.setText("Input 2");
			this.distInput1.setEnabled(enabled);
			this.distInput2.setEnabled(disabled);
			//this.distInput2.setEnabled(enabled);
			this.distInput1.setValue(new Float(0.0));
			this.distInput2.setValue(new Float(0.0));
			//this.distInput2.setValue(new Float(1.0));
		}
		
		// Freq Hist
	/*	this.textNumFreqBuckets.setEnabled(freqenable);
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
		this.buttonQuantHistShowGraph.setEnabled(hbenable);*/
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	
	/**
	 * Sets the global mode.
	 * @param mode globalMode
	 */
	private void setGlobalMode(int mode)
	{
		this.globalMode = mode;
		if(globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			addBucketAtFrontButton.setEnabled(false);
			addBucketAtEndButton.setEnabled(false);
			startMergeButton.setEnabled(false);
			horizontalSlider.setEnabled(false);
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
		}
		else if(globalMode == GraphHistogram.FREQUENCY_MODE_WITHOUTBB)
		{
			horizontalSlider.setEnabled(false);
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
		}
	}

	/**
	 * Sets the localMode and Enables/Disables appropriate buttons.
	 * @param mode localMode
	 */
	private void setMode(int mode)
	{
		localMode = mode;
		selectedBucket = -1;
		selectedBucketsList = null;
		selectedBucketsList = new ArrayList<Integer>();
		colInfo.setText("");
		if( mode ==  BUCKET_CHANGE_MODE)
		{
			verticalSlider.setEnabled(false);
			horizontalSlider.setEnabled(false);
			formattedTextField.setEnabled(false);
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
			if(globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
			{
				addBucketAtFrontButton.setEnabled(false);
				addBucketAtEndButton.setEnabled(false);
				// No Merge mode for Distinct Count
				startMergeButton.setEnabled(false);
			}
			else
			{
				startMergeButton.setEnabled(true);
			}
			mergeButton.setEnabled(false);
			splitButton.setEnabled(false);
			selectedButton.setEnabled(false);
			invSelectedButton.setEnabled(false);
			// Enable redo, undo
			drawUndoRedoButtons();
		}
		else if(mode == DISTRIBUTE_MODE)
		{
			verticalSlider.setEnabled(false);
			horizontalSlider.setEnabled(false);
			formattedTextField.setEnabled(false);
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
			startMergeButton.setEnabled(false);
			mergeButton.setEnabled(false);
			splitButton.setEnabled(false);

			distributeButton.setEnabled(false);
			selectedButton.setEnabled(true);
			invSelectedButton.setEnabled(true);
			// Disable redo, undo
			undoButton.setEnabled(false);
			redoButton.setEnabled(false);
		}
		else if(mode == MERGE_MODE)
		{
			verticalSlider.setEnabled(false);
			horizontalSlider.setEnabled(false);
			formattedTextField.setEnabled(false);
			leftBoundary.setEnabled(false);
			rightBoundary.setEnabled(false);
			splitButton.setEnabled(false);
			distributeButton.setEnabled(false);
			selectedButton.setEnabled(false);
			invSelectedButton.setEnabled(false);

			startMergeButton.setEnabled(false);
			mergeButton.setEnabled(true);
			// Disable redo, undo
			undoButton.setEnabled(false);
			redoButton.setEnabled(false);
		}
		createChart();
		/*
		 * Never call this.setExcessLabelFinishButton();
		 * It enables distribute button, which was disabled in DISTRIBUTE MODE
		 */
	}

	/** This method is called from within the constructor to
	 * initialize the form.
	 * WARNING: Do NOT modify this code. The content of this method is
	 * always regenerated by the Form Editor.
	 */
	// <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
	private void initComponents() {
		java.awt.GridBagConstraints gridBagConstraints;
		jPanel1 = new javax.swing.JPanel();
		addBucketAtFrontButton = new javax.swing.JButton();
		colInfo = new javax.swing.JLabel();

		setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gridBagLayout.rowWeights = new double[]{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		getContentPane().setLayout(gridBagLayout);
		
		panel = new JPanel();
		GridBagConstraints gbc_panel = new GridBagConstraints();
		gbc_panel.fill = GridBagConstraints.BOTH;
		gbc_panel.insets = new Insets(0, 0, 5, 5);
		gbc_panel.gridx = 0;
		gbc_panel.gridy = 0;
		getContentPane().add(panel, gbc_panel);
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0};
		gbl_panel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);
		
				verticalSlider = new javax.swing.JSlider();
				GridBagConstraints gbc_verticalSlider = new GridBagConstraints();
				gbc_verticalSlider.insets = new Insets(0, 0, 5, 0);
				gbc_verticalSlider.anchor = GridBagConstraints.WEST;
				gbc_verticalSlider.fill = GridBagConstraints.VERTICAL;
				gbc_verticalSlider.gridx = 0;
				gbc_verticalSlider.gridy = 0;
				panel.add(verticalSlider, gbc_verticalSlider);
				
						verticalSlider.setOrientation(javax.swing.JSlider.VERTICAL);
						verticalSlider.setToolTipText("Change the slider to change the frequency");
						verticalSlider.addChangeListener(new javax.swing.event.ChangeListener() {
							public void stateChanged(javax.swing.event.ChangeEvent evt) {
								verticalSliderStateChanged(evt);
							}
						});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout.setHorizontalGroup(
				jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 678, Short.MAX_VALUE)
				);
		jPanel1Layout.setVerticalGroup(
				jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 297, Short.MAX_VALUE)
				);

		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 1;
		gridBagConstraints.gridy = 0;
		gridBagConstraints.gridwidth = 6;
		gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
		gridBagConstraints.anchor = java.awt.GridBagConstraints.PAGE_START;
		gridBagConstraints.weightx = 0.9;
		gridBagConstraints.weighty = 0.9;
		gridBagConstraints.insets = new java.awt.Insets(10, 10, 10, 10);
		getContentPane().add(jPanel1, gridBagConstraints);

		addBucketAtFrontButton.setText("Add/Delete Bucket at Front");
		addBucketAtFrontButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				addBucketAtFrontButtonActionPerformed(evt);
			}
		});
		gridBagConstraints_10 = new java.awt.GridBagConstraints();
		gridBagConstraints_10.anchor = GridBagConstraints.WEST;
		gridBagConstraints_10.fill = GridBagConstraints.VERTICAL;
		gridBagConstraints_10.gridx = 1;
		gridBagConstraints_10.gridy = 1;
		gridBagConstraints_10.insets = new Insets(5, 0, 5, 5);
		getContentPane().add(addBucketAtFrontButton, gridBagConstraints_10);
		distributeButton = new javax.swing.JButton();

		distributeButton.setText("Distribute Excess/Less Values");
		distributeButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				distributeButtonActionPerformed(evt);
			}
		});
		addBucketAtEndButton = new javax.swing.JButton();

		addBucketAtEndButton.setText("Add/Delete Bucket at End");
		addBucketAtEndButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				addBucketAtEndButtonActionPerformed(evt);
			}
		});
		gridBagConstraints_3 = new java.awt.GridBagConstraints();
		gridBagConstraints_3.anchor = GridBagConstraints.EAST;
		gridBagConstraints_3.fill = GridBagConstraints.VERTICAL;
		gridBagConstraints_3.gridx = 6;
		gridBagConstraints_3.gridy = 1;
		gridBagConstraints_3.insets = new java.awt.Insets(5, 0, 5, 10);
		getContentPane().add(addBucketAtEndButton, gridBagConstraints_3);
		
		panel_1 = new JPanel();
		GridBagConstraints gbc_panel_1 = new GridBagConstraints();
		gbc_panel_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1.gridwidth = 6;
		gbc_panel_1.insets = new Insets(0, 0, 5, 0);
		gbc_panel_1.gridx = 1;
		gbc_panel_1.gridy = 2;
		getContentPane().add(panel_1, gbc_panel_1);
		GridBagLayout gbl_panel_1 = new GridBagLayout();
		gbl_panel_1.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_panel_1.rowHeights = new int[]{0, 0, 0};
		gbl_panel_1.columnWeights = new double[]{1.0, 1.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_panel_1.rowWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		panel_1.setLayout(gbl_panel_1);
		horizontalSlider = new com.visutools.nav.bislider.BiSlider();
		GridBagConstraints gbc_horizontalSlider = new GridBagConstraints();
		gbc_horizontalSlider.insets = new Insets(0, 0, 5, 0);
		gbc_horizontalSlider.fill = GridBagConstraints.HORIZONTAL;
		gbc_horizontalSlider.gridwidth = 6;
		gbc_horizontalSlider.gridx = 0;
		gbc_horizontalSlider.gridy = 0;
		panel_1.add(horizontalSlider, gbc_horizontalSlider);
		
				horizontalSlider.setMaximumColor(new java.awt.Color(255, 255, 255));
				horizontalSlider.setMinimumColor(new java.awt.Color(255, 255, 255));
		gridBagConstraints_4 = new java.awt.GridBagConstraints();
		gridBagConstraints_4.fill = GridBagConstraints.BOTH;
		gridBagConstraints_4.gridx = 2;
		gridBagConstraints_4.gridy = 3;
		gridBagConstraints_4.insets = new java.awt.Insets(5, 5, 5, 5);
		getContentPane().add(distributeButton, gridBagConstraints_4);

		label = new JLabel("=>");
		label.setFont(new Font("Arial Black", Font.BOLD, 18));
		label.setHorizontalAlignment(SwingConstants.CENTER);
		label.setForeground(new Color(255, 0, 0));
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.insets = new Insets(0, 0, 5, 5);
		gbc_label.gridx = 3;
		gbc_label.gridy = 3;
		getContentPane().add(label, gbc_label);
		selectedButton = new javax.swing.JButton();
		selectedButton.setToolTipText("Select the buckets in which you want to distribute excess values.");

		selectedButton.setText("Selected Buckets");
		selectedButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				selectedButtonActionPerformed(evt);
			}
		});
		gridBagConstraints_7 = new java.awt.GridBagConstraints();
		gridBagConstraints_7.fill = GridBagConstraints.BOTH;
		gridBagConstraints_7.gridx = 4;
		gridBagConstraints_7.gridy = 3;
		gridBagConstraints_7.insets = new java.awt.Insets(5, 5, 5, 5);
		getContentPane().add(selectedButton, gridBagConstraints_7);
		startMergeButton = new javax.swing.JButton();

		startMergeButton.setText("Start Merging Buckets");
		startMergeButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				startMergeButtonActionPerformed(evt);
			}
		});
		invSelectedButton = new javax.swing.JButton();
		invSelectedButton.setToolTipText("Excess data will be distributed to the buckets which are not selected.");

		invSelectedButton.setText("Not Selected Buckets");
		invSelectedButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				invSelectedButtonActionPerformed(evt);
			}
		});
		gridBagConstraints_9 = new java.awt.GridBagConstraints();
		gridBagConstraints_9.fill = GridBagConstraints.BOTH;
		gridBagConstraints_9.gridx = 5;
		gridBagConstraints_9.gridy = 3;
		gridBagConstraints_9.insets = new java.awt.Insets(5, 5, 5, 5);
		getContentPane().add(invSelectedButton, gridBagConstraints_9);
		gridBagConstraints_5 = new java.awt.GridBagConstraints();
		gridBagConstraints_5.fill = GridBagConstraints.BOTH;
		gridBagConstraints_5.gridx = 2;
		gridBagConstraints_5.gridy = 4;
		gridBagConstraints_5.insets = new java.awt.Insets(5, 5, 5, 5);
		getContentPane().add(startMergeButton, gridBagConstraints_5);

		label_1 = new JLabel("=>");
		label_1.setHorizontalAlignment(SwingConstants.CENTER);
		label_1.setForeground(Color.RED);
		label_1.setFont(new Font("Arial Black", Font.BOLD, 18));
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.insets = new Insets(0, 0, 5, 5);
		gbc_label_1.gridx = 3;
		gbc_label_1.gridy = 4;
		getContentPane().add(label_1, gbc_label_1);
		mergeButton = new javax.swing.JButton();

		mergeButton.setText("Merge Buckets");
		mergeButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				mergeButtonActionPerformed(evt);
			}
		});
		
		panel_4 = new JPanel();
		GridBagConstraints gbc_panel_4 = new GridBagConstraints();
		gbc_panel_4.insets = new Insets(0, 0, 5, 5);
		gbc_panel_4.fill = GridBagConstraints.HORIZONTAL;
		gbc_panel_4.gridx = 5;
		gbc_panel_4.gridy = 5;
		getContentPane().add(panel_4, gbc_panel_4);
		
		distLabel1 = new JLabel("Input 1");
		panel_4.add(distLabel1);
		
		//DecimalFormat format = new DecimalFormat( "####.##" );
		NumberFormat distInputFormat;
		distInputFormat = NumberFormat.getNumberInstance();
		distInputFormat.setMinimumFractionDigits(2);
		distInputFormat.setMaximumFractionDigits(2);
		distInputFormat.setRoundingMode(RoundingMode.HALF_UP);
		distInput1 = new JFormattedTextField(distInputFormat);
		
		class FormattedTextFieldListener implements PropertyChangeListener {

				@Override
				public void propertyChange(PropertyChangeEvent evt) {
					Object source = evt.getSource();
					// Store the history into UndoList
					addToUndoList();
					//double check = ((Number)distInput1.getValue()).doubleValue();
					if(source == distInput1 && (distList.getSelectedIndex() == 1)){

						int steps = buckets.size();
						double mean = 0.0;
						double sd = 1.0;
						double cumFreq = getTotalRows()/steps;
						double movement = round((cumFreq/100.0),2);
						double p = 0.0;	
					
						double right = 0.0;
						NormalDistribution nd = new NormalDistribution(mean, sd);
						//0.0001 - A starting boundary; 
                        //Boundary not appearing in Histogram, Need to check.
						double left = round(nd.inverseCumulativeProbability(0.0001),2); 
						for(int i=0; i<steps; i++) {
							p = round((p+movement),2);
							if(p>=1.0) p = 0.9999;
							right = round(nd.inverseCumulativeProbability(p),2);
							freshBucketBoundary(i, left, right);
							left = right;		
						}			
						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						createChart();
					}
					else if(source == distInput1 && distList.getSelectedIndex() == 2){
						int steps = buckets.size();
						double mean = ((Number)distInput1.getValue()).doubleValue();
						double sd = 1.0;
						long value = getTotalRows();
						double cumFreq = 100/steps;
						double movement = round((cumFreq/100.0),2);
						double p = 0.0;	
					
						double right = 0.0;
						NormalDistribution nd = new NormalDistribution(mean, sd);
						//0.0001 - A starting boundary; 
                        //Boundary not appearing in Histogram, Need to check.
						double left = round(nd.inverseCumulativeProbability(0.0001),2); 
						//double left = round(nd.inverseCumulativeProbability(p),2);
						for(int i=0; i<steps; i++) {
							p = round((p+movement),2);
							if(p>=1.0) p = 0.9999;
							right = round(nd.inverseCumulativeProbability(p),2);
							freshBucketBoundary(i, left, right);
							left = right;		
						}			
						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						createChart(); 
					}
					else if(source == distInput2 && distList.getSelectedIndex() == 2){
						int steps = buckets.size();
						double mean = ((Number)distInput1.getValue()).doubleValue();
						double sd = ((Number)distInput2.getValue()).doubleValue();
						double cumFreq = getTotalRows()/steps;
						double movement = round((cumFreq/100.0),2);
						double p = 0.0;	
					
						double right = 0.0;
						NormalDistribution nd = new NormalDistribution(mean, sd);
						//0.0001 - A starting boundary; 
                        //Boundary not appearing in Histogram, Need to check.
						double left = round(nd.inverseCumulativeProbability(0.0001),2); 
						for(int i=0; i<steps; i++) {
							p = round((p+movement),2);
							if(p>=1.0) p = 0.9999;
							right = round(nd.inverseCumulativeProbability(p),2);
							freshBucketBoundary(i, left, right);
							left = right;		
						}			
						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						createChart();
					}
					else if(source == distInput1 && distList.getSelectedIndex() == 3){
						int steps = buckets.size();
						double lambda = ((Number)distInput1.getValue()).doubleValue();
						double cumFreq = getTotalRows()/steps;
						double movement = round((cumFreq/100.0),2);
						double p = 0.0;	
					
						double right = 0.0;
						PoissonDistribution pd = new PoissonDistribution(lambda);
						//0.0001 - A starting boundary; 
                        //Boundary not appearing in Histogram, Need to check.
						double left = round(pd.inverseCumulativeProbability(0.0001),2);
						for(int i=0; i<steps; i++) {
							p = round((p+movement),2);
							if(p>=1.0) p = 0.9999;
							right = round(pd.inverseCumulativeProbability(p),2);
							freshBucketBoundary(i, left, right);
							left = right;		
						}			
						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						createChart();
					}
					else if(source == distInput1 && distList.getSelectedIndex() == 4){
						double skew = ((Number)distInput1.getValue()).doubleValue();
						double first = Double.parseDouble(buckets.get(0).getLValue().getString());
						double last = Double.parseDouble(buckets.get(buckets.size()-1).getValue().getString());
						int steps = buckets.size();

						double range = last - first;
						//double skew = 0.2;
						double movement = range * skew;
						double current = last;
					

						for(int i=buckets.size()-1; i>=0; i--) {
							double right = current;
							double left = current - movement;
							current = current - movement;
							movement = (current-first)*skew;
							if(i == 0)
								left = first;
							
							if(i == buckets.size()-1)
								right = last;
							
							left = (int )(left * 10)/10.0;
							right = (int )(right * 10)/10.0;
													
							freshBucketBoundary(i, left, right);
						}			

						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						// Refresh Chart to show modified buckets.
						createChart();
					}
					// TODO Auto-generated method stub
					else if(source == distInput1 && distList.getSelectedIndex() == 5){
						double skew = ((Number)distInput1.getValue()).doubleValue();
						double first = Double.parseDouble(buckets.get(0).getLValue().getString());
						double last = Double.parseDouble(buckets.get(buckets.size()-1).getValue().getString());
						int steps = buckets.size();

						double range = last - first;
						//double skew = 0.2;
						double movement = range * skew;
						double current = first;

						for(int i=0; i < buckets.size(); i++) {
							double left = current;
							double right = current + movement;
									
							if(i == 0)
								left = first;
							
							if(i == buckets.size()-1)
								right = last;
							
							left = (int )(left * 10)/10.0;
							right = (int )(right * 10)/10.0;
							
							freshBucketBoundary(i, left, right);
							
							current = current + movement;
							range = last - current;
							movement = range * skew;
						}
						
						setMode(GraphHistogram.BUCKET_CHANGE_MODE);
						// Refresh Chart to show modified buckets.
						createChart();
					}
					// TODO Auto-generated method stub
				}
				
			}
		
		distInput1.addPropertyChangeListener("value", new FormattedTextFieldListener());
		//distInput2.addPropertyChangeListener("value", new FormattedTextFieldListener());
		
	/*	distInput1.addFocusListener(new FocusAdapter() {
			@Override
			public void focusLost(FocusEvent arg0) {
				createDistribution(true);
			}
		});*/
		distInput1.setColumns(10);
		distInput1.setEnabled(false);
		panel_4.add(distInput1);
		
		panel_3 = new JPanel();
		GridBagConstraints gbc_panel_3 = new GridBagConstraints();
		gbc_panel_3.fill = GridBagConstraints.HORIZONTAL;
		gbc_panel_3.insets = new Insets(0, 0, 0, 5);
		gbc_panel_3.gridx = 5;
		gbc_panel_3.gridy = 6;
		getContentPane().add(panel_3, gbc_panel_3);
		panel_3.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
		
		distLabel2 = new JLabel("Input 2");
		panel_3.add(distLabel2);
		
		NumberFormat distInputFormat2;
		distInputFormat2 = NumberFormat.getNumberInstance();
		distInputFormat2.setMinimumFractionDigits(2);
		distInputFormat2.setMaximumFractionDigits(2);
		distInputFormat2.setRoundingMode(RoundingMode.HALF_UP);
		distInput2 = new JFormattedTextField(distInputFormat2);
		distInput2.addPropertyChangeListener("value", new FormattedTextFieldListener());
		
		distInput2.setEnabled(false);
		distInput2.setColumns(10);
		panel_3.add(distInput2);
		
		gridBagConstraints_8 = new java.awt.GridBagConstraints();
		gridBagConstraints_8.fill = GridBagConstraints.BOTH;
		gridBagConstraints_8.gridx = 4;
		gridBagConstraints_8.gridy = 4;
		gridBagConstraints_8.insets = new java.awt.Insets(5, 5, 5, 5);
		getContentPane().add(mergeButton, gridBagConstraints_8);
		redoButton = new javax.swing.JButton();

		redoButton.setText("Redo");
		redoButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				redoButtonActionPerformed(evt);
			}
		});
		splitButton = new javax.swing.JButton();

		splitButton.setText("Split a Bucket in Two Buckets");
		splitButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				splitButtonActionPerformed(evt);
			}
		});

		lblUseCtrlKey = new JLabel("(Use Ctrl key to select multiple buckets)");
		GridBagConstraints gbc_lblUseCtrlKey = new GridBagConstraints();
		gbc_lblUseCtrlKey.fill = GridBagConstraints.BOTH;
		gbc_lblUseCtrlKey.insets = new Insets(0, 0, 5, 5);
		gbc_lblUseCtrlKey.gridx = 5;
		gbc_lblUseCtrlKey.gridy = 4;
		getContentPane().add(lblUseCtrlKey, gbc_lblUseCtrlKey);
		gridBagConstraints_6 = new java.awt.GridBagConstraints();
		gridBagConstraints_6.fill = GridBagConstraints.BOTH;
		gridBagConstraints_6.gridx = 2;
		gridBagConstraints_6.gridy = 5;
		gridBagConstraints_6.insets = new Insets(5, 5, 5, 5);
		getContentPane().add(splitButton, gridBagConstraints_6);
		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 0;
		gridBagConstraints.gridy = 6;
		gridBagConstraints.insets = new java.awt.Insets(10, 10, 10, 10);
		getContentPane().add(redoButton, gridBagConstraints);
		redoButton.getAccessibleContext().setAccessibleName("REdo");
		undoButton = new javax.swing.JButton();

		undoButton.setText("Undo");
		undoButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				undoButtonActionPerformed(evt);
			}
		});
		gridBagConstraints = new java.awt.GridBagConstraints();
		gridBagConstraints.gridx = 1;
		gridBagConstraints.gridy = 6;
		gridBagConstraints.insets = new java.awt.Insets(10, 10, 10, 10);
		getContentPane().add(undoButton, gridBagConstraints);

		colInfo.setText("ColumnInfo");
		gridBagConstraints_12 = new java.awt.GridBagConstraints();
		gridBagConstraints_12.insets = new Insets(0, 0, 5, 5);
		gridBagConstraints_12.anchor = GridBagConstraints.WEST;
		gridBagConstraints_12.gridx = 3;
		gridBagConstraints_12.gridy = 1;
		gridBagConstraints_12.gridwidth = 3;
		getContentPane().add(colInfo, gridBagConstraints_12);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	/**
	 * Returns the rows in the histogram.
	 * If DistinctCount Mode, return totalDistinctRowsCount
	 * Else return totalRowsCount
	 * @return
	 */
	private long getTotalRows()
	{
		if(this.globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			return this.totalDistinctRowsCount;
		}
		else
		{
			return this.totalRowsCount;
		}
	}

	/**
	 * Returns the percentage (freq / distCount) for the given bucket.
	 * If DistinctCount Mode, return distCountPercent
	 * Else return FreqPercent
	 * @param bucket BucketItem
	 * @return
	 */
	private double getPercent(BucketItem bucket)
	{
		if(this.globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			return bucket.getDistinctCountPercent();
		}
		else
		{
			return bucket.getFreqPercent();
		}
	}

	/**
	 * Sets the percentage (freq / distCount) for the given bucket.
	 * If DistinctCount Mode, set distCountPercent
	 * Else set FreqPercent
	 * @param bucket BucketItem
	 * @param percent value
	 * returns value which was set..
	 */
	private double setPercent(BucketItem bucket, double percent)
	{
		double value = (getTotalRows() / 100.0) * percent;
		if(this.globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			// Check for Constraints
			double newValue = value;
			// Check for lowebound - only for db2
			if(this.db2Hist)
			{
				//sum(#F's in the interval)
			}
			// Check for upper bound
			if (DataType.isInteger(dataType)) {
				double interval = bucket.getValue().getInteger() - bucket.getLValue().getInteger();
				double min = Math.min(interval, bucket.getFreq());
				if (value > min) {
					newValue = min;
				}
			} else if (DataType.isNumeric(dataType)) {
				double interval = bucket.getValue().getBigDecimal().doubleValue() - bucket.getLValue().getBigDecimal().doubleValue();
				double min = Math.min(interval, bucket.getFreq());
				if (value > min) {
					newValue = min;
				}
			} else if (DataType.isBigDecimal(dataType)) {
				double interval = bucket.getValue().getBigInteger().longValue() - bucket.getLValue().getBigInteger().longValue();
				double min = Math.min(interval, bucket.getFreq());
				if (value > min) {
					newValue = min;
				}
			} else {
				if (value > bucket.getFreq()) {
					newValue = bucket.getFreq();
				}
			}
			bucket.setDistinctCount(newValue);
			double newPercent =  ((newValue * 100.0)/getTotalRows());  // percentage
			bucket.setDistinctCountPercent(newPercent);
			return newPercent;
		}
		else
		{
			// Check for Constraints
			double newValue = value;
			// Check for lowerbound - only for db2
			if(this.db2Hist)
			{
				// sum(F's in the interval)
			}
			// Check for upper bound
			if (this.uniqueColumn && (DataType.isInteger(dataType))) {
				double interval = bucket.getValue().getInteger() - bucket.getLValue().getInteger();
				if (value > interval) {
					newValue = interval;
				}
			}
			else if (this.uniqueColumn && (DataType.isNumeric(dataType))) {
				double interval = bucket.getValue().getBigDecimal().doubleValue() - bucket.getLValue().getBigDecimal().doubleValue();
				if (value > interval) {
					newValue = interval;
				}
			}
			else if (this.uniqueColumn && (DataType.isBigDecimal(dataType))) {
				double interval = bucket.getValue().getBigInteger().longValue() - bucket.getLValue().getBigInteger().longValue();
				if (value > interval) {
					newValue = interval;
				}
			}

			bucket.setFreq(newValue);
			double newPercent =  ((newValue * 100.0)/getTotalRows());  // percentage
			bucket.setFreqPercent(newPercent);
			return newPercent;
		}
	}

	/**
	 * Returns the freq / distCount for the given bucket.
	 * If DistinctCount Mode, return distCount
	 * Else return Freq
	 * @param bucket BucketItem
	 * @return
	 */
	private double getValue(BucketItem bucket)
	{
		if(this.globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			return bucket.getDistinctCount();
		}
		else
		{
			return bucket.getFreq();
		}
	}

	/**
	 * Sets the freq / distCount for the given bucket.
	 * If DistinctCount Mode, set distCount
	 * Else set Freq
	 * @param bucket BucketItem
	 * @param percent value
	 */
	private void setValue(BucketItem bucket, double value)
	{
		if(this.globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			Double newValue = new Double(value);
			// Check for lowebound - only for db2
			if(this.db2Hist)
			{
				// sum(#F's in the interval)
			}
			// Check for upper bound
			Double valueD = new Double(value);
			if (DataType.isInteger(dataType)) {
				double interval = bucket.getValue().getInteger() - bucket.getLValue().getInteger();
				Double min = Math.min(interval, bucket.getFreq());
				if (valueD.intValue() > min.intValue()) {
					newValue = min;
				}
			} else if (DataType.isNumeric(dataType)) {
				double interval = bucket.getValue().getBigInteger().longValue() - bucket.getLValue().getBigDecimal().doubleValue();
				Double min = Math.min(interval, bucket.getFreq());
				if (valueD.longValue() > min.longValue()) {
					newValue = min;
				}
			} else if (DataType.isBigDecimal(dataType)) {
				double interval = bucket.getValue().getBigInteger().longValue() - bucket.getLValue().getBigInteger().longValue();
				Double min = Math.min(interval, bucket.getFreq());
				if (valueD.longValue() > min.longValue()) {
					newValue = min;
				}
			} else {
				Double freq = bucket.getFreq();
				if (valueD.longValue() > freq.longValue()) {
					newValue = freq;
				}
			}
			bucket.setDistinctCount(newValue);
		}
		else
		{
			bucket.setFreq(value);
		}
	}

	/**
	 * Add / Remove Button is pressed in the front.
	 * Ask for Add / Delete Bucket and do appropriate actions.
	 * It is enabled only in GraphHistogram.FREQUENCY_MODE_WITHBB, GraphHistogram.FREQUENCY_MODE_WITHOUTBB and GraphHistogram.FREQUENCY_MODE_ONLYBB modes.
	 * @param evt
	 */
	private void addBucketAtFrontButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_addBucketAtFrontButtonActionPerformed
		Object[] possibilities = {"", "Add Bucket", "Delete Bucket"};
		String s = (String)JOptionPane.showInputDialog(
				this,
				"Choose the option:\n",
				"Add / Delete Bucket at front",
				JOptionPane.PLAIN_MESSAGE,
				null,
				possibilities,
				"");

		//If a string was returned, say so.
		if ((s != null) && (s.length() > 0)) {
			if(s.equals("Add Bucket"))
			{
				// Add Bucket in the left
				// Adjust MinValue, excessPercent
				JTextField value = new JTextField();
				JTextField percent = new JTextField();
				Object[] message = new Object[]{"MinValue", value, "Count %", percent};
				int r = JOptionPane.showConfirmDialog(this, message, "Enter Split Values", JOptionPane.OK_CANCEL_OPTION);

				if (r == JOptionPane.OK_OPTION) {
					boolean rightValues = false;
					DataType valueMin = new DataType (dataType, value.getText());
					DataType minTemp = new DataType (minValue);
					if(DataType.isDouble(dataType) || DataType.isInteger(dataType) || DataType.isNumeric(dataType) || DataType.isBigDecimal(dataType))
					{
						minTemp.subtract(minWidth);
					}
					if (minTemp.compare(valueMin) > 0) {
						rightValues = true;
					} else {
						JOptionPane.showMessageDialog(this, "Value Entry is not correct.", "CODD-Error", JOptionPane.ERROR_MESSAGE);
						rightValues = false;
						return;
					}

					double per = Double.parseDouble(percent.getText());
					if (per > minHeight && per < 100.00) {
						rightValues = rightValues & true;
					} else {
						JOptionPane.showMessageDialog(this, "Count Percentage is not correct.", "CODD-Error", JOptionPane.ERROR_MESSAGE);
						rightValues = false;
						return;
					}
					if (rightValues) {
						// Store the history into UndoList
						addToUndoList();
						// Add new Bucket
						DataType valueDB = new DataType(minValue);
						DataType lvalue = new DataType(valueMin);
						BucketItem bucket = new BucketItem(lvalue,valueDB,(long)(totalRowsCount*per/100),(long)(totalDistinctRowsCount*per/100), per, per);
						this.setPercent(bucket, per);
						minValue = valueMin;
						buckets.add(0, bucket);
						excessPercent = excessPercent + per;
					}
				}
			}
			else if(s.equals("Delete Bucket"))
			{
				// Store the history into UndoList
				addToUndoList();
				// Remove Bucket in the left
				// Adjust MinValue, excessPercent
				BucketItem bucket = buckets.remove(0);
				minValue = bucket.getValue();
				excessPercent = excessPercent - bucket.freqPercent;
			}
			// Refresh Chart to show the modified buckets.
			createChart();
			setExcessLabelFinishButton();
		}
		setMode(GraphHistogram.BUCKET_CHANGE_MODE);
	}//GEN-LAST:event_addBucketAtFrontButtonActionPerformed

	private void verticalSliderStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_verticalSliderStateChanged
		// Identify the changes and update buckets appropriately.
		if(globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB) {
			return;
		}
		JSlider source = (JSlider)evt.getSource();
		if (!source.getValueIsAdjusting()) {
			int value = source.getValue();
			double valueDouble = (double)value / 100.0;
			if(this.localMode == GraphHistogram.BUCKET_CHANGE_MODE)
			{
				// Store the history into UndoList (if not being set by program - when a bucket is clicked)
				if(!setSlider)
				{
					addToUndoList();
				}
				if(valueDouble >= minHeight)
					setHeightValueToSelectedBucket(valueDouble);
				else if(selectedBucket != -1) // newValue is less than minHeight
				{
					if(getPercent(buckets.get(selectedBucket)) > minHeight)
					{
						// Previously % was above minHeight
						setHeightValueToSelectedBucket(minHeight);
					}
					else
					{
						// Previously % was below minHeight, newValue also less than minHeight
						// If newValue is increased, update to newValue, else DO NOT UPDATE
						if(valueDouble > getPercent(buckets.get(selectedBucket)))
							setHeightValueToSelectedBucket(valueDouble);
					}
				}
			}
		}
		else {//value is adjusting; just set the text
			int value = source.getValue();
			double valueDouble = (double)value / 100.0;
            formattedTextField.setText(String.valueOf(valueDouble));
		}
	}//GEN-LAST:event_verticalSliderStateChanged

	private void distributeButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_distributeButtonActionPerformed
		setMode(GraphHistogram.DISTRIBUTE_MODE);
	}//GEN-LAST:event_distributeButtonActionPerformed

	private void startMergeButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_startMergeButtonActionPerformed
		setMode(GraphHistogram.MERGE_MODE);
	}//GEN-LAST:event_startMergeButtonActionPerformed

	private void splitButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_splitButtonActionPerformed
		if(selectedBucket != -1)
		{
			DataType min, max;
			if(selectedBucket > 0)
				min = buckets.get(selectedBucket - 1).getValue();
			else
				min = minValue;
			max = buckets.get(selectedBucket).getValue();

			boolean inputIsDone = false;
			boolean entriesMade = false;
			DataType interValue = null;
			double value1 = 0,value2 = 0;
			while(!inputIsDone)
			{
				JTextField intermediateValue = new JTextField();
				JTextField leftPercentage = new JTextField();
				JTextField rightPercentage = new JTextField();
				leftPercentage.setText("50.00");
				rightPercentage.setText("50.00");
				Object[] message = new Object[] {
						"Value", intermediateValue, "Left Bucket %", leftPercentage, "Right Bucket %", rightPercentage };
				int r = JOptionPane.showConfirmDialog(this, message, "Enter Split Values", JOptionPane.OK_CANCEL_OPTION);

				if (r == JOptionPane.OK_OPTION)
				{
					/* Inter Value Check. */
					interValue = new DataType (dataType, intermediateValue.getText());

					if(interValue.compare(min) > 0 && interValue.compare(max) < 0 )
						inputIsDone = true;
					else
						JOptionPane.showMessageDialog(this, "Value should be in between the bucket boundaries.", "CODD-Error",JOptionPane.ERROR_MESSAGE);

					/* Sum of percentage Check. */
					DecimalFormat decimalFormat = new DecimalFormat("#.##");
					value1 = Double.parseDouble(leftPercentage.getText());
					value1 =  Double.valueOf(decimalFormat.format(value1));
					value2 = Double.parseDouble(rightPercentage.getText());
					value2 =  Double.valueOf(decimalFormat.format(value2));
					double sum = value1 + value2;
					if(sum != 100.00)
					{
						JOptionPane.showMessageDialog(this, "Sum of percentages is not 100.", "CODD-Error",JOptionPane.ERROR_MESSAGE);
						inputIsDone = false;
					}
					else
						inputIsDone = inputIsDone & true;

					if(inputIsDone)
						entriesMade = true;
				}
				else
				{
					inputIsDone = true;
				}
			}

			if(entriesMade) // Inputs are entered and valid.
			{
				// Store the history into UndoList
				addToUndoList();
				// Do the split for the selected bucket
				ArrayList<BucketItem> tempBuckets = new ArrayList<BucketItem>();
				for(int i=0;i<buckets.size();i++)
				{
					BucketItem bucket = buckets.get(i);
					if(i == selectedBucket)
					{
						double leftFreq, rightFreq, leftDistinctCount, rightDistinctCount, freqPercent, distinctCountPercent;
						// Discard all values after decimal point.
						leftFreq = Math.round(bucket.getFreq() * value1 / 100.00);
						leftDistinctCount = Math.round(bucket.getDistinctCount() * value1 / 100.00);
						freqPercent = bucket.getFreqPercent() * (leftFreq/bucket.getFreq());
						distinctCountPercent = bucket.getDistinctCountPercent() * (leftDistinctCount / bucket.getDistinctCount());
						DataType lvalue = new DataType(bucket.getLValue());
						// lvalue is same as prevBucket
						BucketItem newBucket = new BucketItem(lvalue,interValue, leftFreq, leftDistinctCount, freqPercent, distinctCountPercent);
						tempBuckets.add(newBucket);

						// lvalue is the intermediateValue
						lvalue = new DataType(interValue);
						bucket.setLValue(lvalue);
						rightFreq = bucket.getFreq() - leftFreq;
						rightDistinctCount = bucket.getDistinctCount() - leftDistinctCount;
						freqPercent = bucket.getFreqPercent() * (rightFreq/bucket.getFreq());
						distinctCountPercent = bucket.getDistinctCountPercent() * (rightDistinctCount/bucket.getDistinctCount());
						bucket.setFreq(rightFreq);
						bucket.setDistinctCount(rightDistinctCount);
						bucket.setFreqPercent(freqPercent);
						bucket.setDistinctCountPercent(distinctCountPercent);
						tempBuckets.add(bucket);
					}
					else{
						tempBuckets.add(bucket);						
					}
				}
				// Re assign new Buckets.
				buckets = tempBuckets;
				// Refresh Chart to show modified buckets.
				createChart();
			}
		}
		setMode(GraphHistogram.BUCKET_CHANGE_MODE);
	}//GEN-LAST:event_splitButtonActionPerformed
	/**
	 * Adjusts the graphElements so that the sum becomes 100%.
	 * This function is internally called upon selected InvSelected, Seleceted Button
	 * to distribute the excessPercent rows.
	 * @param containedElements Distribute on selected buckets if true,
	 * otherwise on Inverse selected buckets.
	 * If Excess
	 *  Calculate the sum of selected buckets freqPercent
	 *  If the sum is > excess
	 *      Do weighted split and decrease on the selected buckets
	 *  Else Error Message to select more buckets
	 * If Less
	 *  Do weighted split and increase on the selected buckets
	 */
	private void doGraphAdjustment(boolean containedElements)
	{
		// Store the history into UndoList
		addToUndoList();
		if(excessPercent != 0)
		{
			// Find the sum of selected / inv-selected buckets
			double sum = 0;
			for(int i=0;i<buckets.size();i++)
			{
				Integer I = new Integer(i);
				if((containedElements && selectedBucketsList.contains(I)) || (!containedElements && !selectedBucketsList.contains(I)))
				{
					sum = sum + getPercent(buckets.get(i));
				}
			}
			// sum must be > excessPercent, in case of decrease operation for distribution
			if (excessPercent >0 && sum < excessPercent) {
				JOptionPane.showMessageDialog(null, "Sum of Seleceted Bucket Frequncy Percent is less than the Excess Value.", "CODD Error Message - Action Required", JOptionPane.ERROR_MESSAGE);
			}
			else {
				double totalDecrease = 0;
				for (int i = 0; i < buckets.size(); i++) {
					Integer I = new Integer(i);
					if((containedElements && selectedBucketsList.contains(I)) || (!containedElements && !selectedBucketsList.contains(I)))
					{

						BucketItem bucket = buckets.get(i);
						double prevValue = getPercent(bucket);
						double value = (excessPercent / sum)*prevValue;
						// As the value (excessPercent) is negative for Less
						// Weighted Increse / Decrease
						double newValue = (prevValue - value);
						double retValue = setPercent(bucket,newValue);
						value = prevValue - retValue; // prevValue - (prevValue - value) for noError-newValue is returned
						totalDecrease = totalDecrease + value;
					}
				}
				// As the value (excessPercent) is negative for Less
				excessPercent = excessPercent - totalDecrease;
				excessPercent =  Double.valueOf(decimalFormat.format(excessPercent));
				setMode(GraphHistogram.BUCKET_CHANGE_MODE);
				// Refresh Chart to show the modified buckets.
				createChart();
				setExcessLabelFinishButton();
			}
		}
	}

	private void selectedButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_selectedButtonActionPerformed
		// Selected Button is chosen.
		doGraphAdjustment(true);
	}//GEN-LAST:event_selectedButtonActionPerformed

	private void invSelectedButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_invSelectedButtonActionPerformed
		// Selected Button is chosen.
		doGraphAdjustment(false);
	}//GEN-LAST:event_invSelectedButtonActionPerformed

	private void mergeButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_mergeButtonActionPerformed
		// Merge Button is chosen. Merge the selected buckets into one.
		if(selectedBucketsList.size() > 0)
		{
			// Store the history into UndoList
			addToUndoList();
			int firstEle = (int) selectedBucketsList.get(0);
			int lastEle = selectedBucketsList.get(selectedBucketsList.size() - 1);

			ArrayList<BucketItem> tempBuckets = new ArrayList<BucketItem>();
			BucketItem tempBucket = new BucketItem(buckets.get(firstEle));
			for (int i = 0; i < buckets.size(); i++) {
				if (i > firstEle && i < lastEle) {
					BucketItem bucket = buckets.get(i);
					tempBucket.setDistinctCount(tempBucket.getDistinctCount() + bucket.getDistinctCount());
					tempBucket.setDistinctCountPercent(tempBucket.getDistinctCountPercent() + bucket.getDistinctCountPercent());
					tempBucket.setFreq(tempBucket.getFreq() + bucket.getFreq());
					tempBucket.setFreqPercent(tempBucket.getFreqPercent() + bucket.getFreqPercent());
				} else if (i == firstEle) {
					tempBucket.setLValue(buckets.get(i).getLValue());
					if(firstEle == lastEle)
					{
						tempBucket.setValue(buckets.get(i).getValue());
						tempBuckets.add(tempBucket);
					}
				}else if (i == lastEle) {
					BucketItem bucket = buckets.get(i);
					tempBucket.setDistinctCount(tempBucket.getDistinctCount() + bucket.getDistinctCount());
					tempBucket.setDistinctCountPercent(tempBucket.getDistinctCountPercent() + bucket.getDistinctCountPercent());
					tempBucket.setFreq(tempBucket.getFreq() + bucket.getFreq());
					tempBucket.setFreqPercent(tempBucket.getFreqPercent() + bucket.getFreqPercent());
					tempBucket.setValue(buckets.get(i).getValue());
					tempBuckets.add(tempBucket);
				} else {
					tempBuckets.add(buckets.get(i));
				}
			}
			// Re assign new Buckets.
			buckets = tempBuckets;
		}
		setMode(GraphHistogram.BUCKET_CHANGE_MODE);
		// Refresh Chart to show modified buckets.
		createChart();
		setExcessLabelFinishButton();
	}//GEN-LAST:event_mergeButtonActionPerformed

	
	private void finishButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_finishButtonActionPerformed

		if(db2Hist || globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
		{
			// For DB2, adjust count values (only Integer) so that the sum is correct.
			// For all other databases, adjust distinct count
			long total = 0;
			for (int i = 0; i < buckets.size(); i++) {
				BucketItem bucket = buckets.get(i);
				Double value = getValue(bucket);
				setValue(bucket, value.longValue());
				total = total + value.longValue();
			}
			long temp = getTotalRows() - total;
			for (int i = 0; i < buckets.size() && temp != 0; i++) {
				BucketItem bucket = buckets.get(i);
				Double value = getValue(bucket);
				if (temp > 0) {
					value = value + 1;
					temp--;
				} else {
					value = value - 1;
					temp++;
				}
				setValue(bucket, value.longValue());
			}
		}

		this.dispose();
		if(globalMode != GraphHistogram.DISTINCT_COUNT_MODE && !uniqueColumn && !noDistinct)
		{
			new GraphHistogram(this.constructModeGUI, GraphHistogram.DISTINCT_COUNT_MODE, this.totalRowsCount,this.totalDistinctRowsCount,buckets,minValue,maxValue,minHeight,minWidth,"testColumn",dataType,db2Hist,uniqueColumn, noDistinct).setVisible(true);
		}
		else
		{		
			this.constructModeGUI.setHistogram(buckets, this.noDistinct);
		}

		Constants.CPrintToConsole(" New Buckets. ", Constants.DEBUG_SECOND_LEVEL_Information);
		Constants.CPrintToConsole("TotalRows : "+this.totalRowsCount+"   TotalDistinctCount: "+this.totalDistinctRowsCount, Constants.DEBUG_SECOND_LEVEL_Information);

		for(int i=0; i<buckets.size();i++)
		{
			BucketItem bucket = buckets.get(i);
			Constants.CPrintToConsole("("+bucket.getLValue().getString()+","+bucket.getValue().getString()+") :: "+bucket.getFreq()+" ("+bucket.getFreqPercent()+")"+" : "+bucket.getDistinctCount()+" ("+bucket.getDistinctCountPercent()+")", Constants.DEBUG_SECOND_LEVEL_Information);
		}
	}//GEN-LAST:event_finishButtonActionPerformed

	private void addBucketAtEndButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_addBucketAtEndButtonActionPerformed
		Object[] possibilities = {"", "Add Bucket", "Delete Bucket"};
		String s = (String)JOptionPane.showInputDialog(
				this,
				"Choose the option:\n",
				"Add / Delete Bucket at end",
				JOptionPane.PLAIN_MESSAGE,
				null,
				possibilities,
				"");

		//If a string was returned, say so.
		if ((s != null) && (s.length() > 0)) {
			if (s.equals("Add Bucket")) {
				// Add Bucket in the right
				// Adjust Max Value
				JTextField value = new JTextField();
				JTextField percent = new JTextField();
				Object[] message = new Object[]{"MaxValue", value, "Count %", percent};
				int r = JOptionPane.showConfirmDialog(this, message, "Enter Split Values", JOptionPane.OK_CANCEL_OPTION);

				if (r == JOptionPane.OK_OPTION) {
					boolean rightValues = false;
					DataType valueMax = new DataType(dataType, value.getText());
					DataType maxTemp = new DataType(maxValue);
					if(DataType.isDouble(dataType) || DataType.isInteger(dataType) || DataType.isNumeric(dataType) || DataType.isBigDecimal(dataType))
					{
						maxTemp.add(minWidth);
					}
					if (maxTemp.compare(valueMax) < 0) {
						rightValues = true;
					} else {
						JOptionPane.showMessageDialog(this, "Value Entry is not correct.", "CODD-Error", JOptionPane.ERROR_MESSAGE);
						rightValues = false;
						return;
					}

					double per = Double.parseDouble(percent.getText());
					if (per > minHeight && per < 100.00) {
						rightValues = rightValues & true;
					} else {
						JOptionPane.showMessageDialog(this, "Count Percentage is not correct.", "CODD-Error", JOptionPane.ERROR_MESSAGE);
						rightValues = false;
						return;
					}
					if (rightValues) {
						// Store the history into UndoList
						addToUndoList();
						// Add new Bucket
						DataType valueDB = new DataType(valueMax);
						DataType lvalue = new DataType(maxValue);
						BucketItem bucket = new BucketItem(lvalue,valueDB, (long)(totalRowsCount*per/100), (long)(totalDistinctRowsCount*per/100), per, per);
						this.setPercent(bucket, per);
						maxValue = valueMax;
						buckets.add(bucket);
						excessPercent = excessPercent + per;
					}
				}
			} else if(s.equals("Delete Bucket")){
				// Store the history into UndoList
				addToUndoList();
				// Remove Bucket in the right
				// Adjust MaxValue, excessPercent
				BucketItem bucket = buckets.remove(buckets.size() - 1);
				maxValue = buckets.get(buckets.size()-1).getValue();
				excessPercent = excessPercent - bucket.freqPercent;
			}
			// Refresh Chart to show the modified buckets.
			createChart();
			setExcessLabelFinishButton();
		}
		setMode(GraphHistogram.BUCKET_CHANGE_MODE);
	}//GEN-LAST:event_addBucketAtEndButtonActionPerformed

	private void redoButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_redoButtonActionPerformed
		redo();
	}//GEN-LAST:event_redoButtonActionPerformed

	private void undoButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_undoButtonActionPerformed
		undo();
	}//GEN-LAST:event_undoButtonActionPerformed

	/**
	 * Sets the bucket height value to given value for the selectedBucket
	 * @param value value to set
	 */
	private void setHeightValueToSelectedBucket(double value)
	{
		if(this.localMode == GraphHistogram.BUCKET_CHANGE_MODE && selectedBucket != -1)
		{
			BucketItem bucket = buckets.get(selectedBucket);
			double prevValue = getPercent(bucket);
			double retVal = setPercent(bucket,value);
			value = retVal;
			excessPercent = excessPercent - prevValue + value;
			excessPercent =  Double.valueOf(decimalFormat.format(excessPercent));
			setExcessLabelFinishButton();
		}
		// Refresh Chart
		createChart();
	}

	/**
	 * Based on the excessValue
	 * finishButton and excess Label text are changed
	 */
	private void setExcessLabelFinishButton()
	{
		if(globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB) {
			excessPercent = 0;
		}
		if (excessPercent > 0) {
			distributeButton.setText("Distribute Excess/Less Values (" + excessPercent + "%)");
			finishButton.setEnabled(false);
			distributeButton.setEnabled(true);
		} else if (excessPercent < 0) {
			distributeButton.setText("Distribute Excess/Less Values (" + excessPercent + "%)");
			finishButton.setEnabled(false);
			distributeButton.setEnabled(true);
		} else {
			distributeButton.setText("Distribute Excess/Less Values (0%)");
			finishButton.setEnabled(true);
			distributeButton.setEnabled(false);
		}
		selectedButton.setEnabled(false);
		invSelectedButton.setEnabled(false);
		finishButton.repaint();
	}

	/**
	 * Creates a chart based on the values in the bucket and returns it.
	 * @return chart.
	 */
	private JFreeChart getChart() {
		int no = buckets.size();

		XYIntervalSeries series = new XYIntervalSeries(colName);
		double prevValue;

		if (DataType.isDouble(dataType)) {
			prevValue = minValue.getDouble();
		} else if (DataType.isInteger(dataType)) {
			prevValue = minValue.getInteger();
		} else if (DataType.isNumeric(dataType)) {
			prevValue = minValue.getBigDecimal().doubleValue();
		} else if (DataType.isBigDecimal(dataType)) {
			prevValue = minValue.getBigInteger().longValue();
		} else { // Use UniformBucketInterval
			prevValue = 0;
		}

		for (int n = 0; n < no; n++) {
			BucketItem bucket = buckets.get(n);
			double x;
			if (DataType.isDouble(dataType)) {
				x = bucket.getValue().getDouble();
			} else if (DataType.isInteger(dataType)) {
				x = bucket.getValue().getInteger();
			} else if (DataType.isNumeric(dataType)) {
				x = bucket.getValue().getBigDecimal().doubleValue();
			} else if (DataType.isBigDecimal(dataType)) {
				x = bucket.getValue().getBigInteger().longValue();
			} else { // Use UniformBucketInterval
				x = prevValue + this.UniformBucketInterval;
			}
			double y = getPercent(bucket);
			series.add(x, prevValue, x, y, 0, 0);
			prevValue = x;
		}

		XYIntervalSeriesCollection dataset = new XYIntervalSeriesCollection();
		dataset.addSeries(series);

		JFreeChart chart = ChartFactory.createXYBarChart(
				graphName,
				graphX,
				false,
				graphY,
				dataset,
				PlotOrientation.VERTICAL,
				true,
				true,
				false);

		XYPlot plot = (XYPlot) chart.getPlot();
		MyXYBarRenderer renderer = new MyXYBarRenderer(this);
		renderer.setDrawBarOutline(true);
		BasicStroke stroke = new BasicStroke(3f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND);
		renderer.setBaseOutlineStroke(stroke);
		plot.setRenderer(renderer);

		ValueAxis domain = plot.getDomainAxis();
		double horizontalSliderMin = domain.getLowerBound();
		double horizontalSliderMax = domain.getUpperBound();
		if (this.selectedBucket == -1) // Not Bucket Boundary Change.
		{
			DataType horizontalSliderMinDT = new DataType(DataType.DOUBLE, horizontalSliderMin + "");
			DataType horizontalSliderMaxDT = new DataType(DataType.DOUBLE, horizontalSliderMax + "");
			setHorizontalSliderSettings(horizontalSliderMinDT, horizontalSliderMinDT, horizontalSliderMaxDT, horizontalSliderMaxDT);
		}

		return chart;
	}

	/**
	 * Creates the JFreeChart and displays it on the panel.
	 */
	public void createChart()
	{
		doNotFireHorizontalSliderChangeEvent = true;
		JFreeChart chart = getChart();

		ChartPanel chartPanel = new ChartPanel(chart);
		//chartPanel.setBounds(jPanel1.bounds());
		chartPanel.setBounds(0,0,jPanel1.getWidth(),jPanel1.getHeight());
		chartPanel.setVisible(true);
		chartPanel.addChartMouseListener(this);
		jPanel1.removeAll();
		jPanel1.setLayout(new BorderLayout());
		jPanel1.add(chartPanel, BorderLayout.CENTER);
		/* get the chart axis values and set it in horizontal slider*/


		jPanel1.repaint();
		doNotFireHorizontalSliderChangeEvent = false;
	}

	/**
	 * @param args the command line arguments
	 */
//	public static void main(String args[]) {
//		/* Set the Nimbus look and feel */
//		//<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
//		/* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
//		 * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html
//		 */
//		try {
//			UIManager.setLookAndFeel("com.sun.java.swing.plaf.windows.WindowsLookAndFeel");
//		} catch (ClassNotFoundException ex) {
//			java.util.logging.Logger.getLogger(GraphHistogram.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
//		} catch (InstantiationException ex) {
//			java.util.logging.Logger.getLogger(GraphHistogram.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
//		} catch (IllegalAccessException ex) {
//			java.util.logging.Logger.getLogger(GraphHistogram.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
//		} catch (javax.swing.UnsupportedLookAndFeelException ex) {
//			java.util.logging.Logger.getLogger(GraphHistogram.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
//		}
//		//</editor-fold>
//
//		/* Create and display the form */
//		java.awt.EventQueue.invokeLater(new Runnable() {
//
//			public void run() {
//				String mode = "double"; //"varchar", "integer", "double"
//
//
//				/*
//                double min = 0;
//                double[] v = {10,12,32,40,60,66,90,150,160,200};
//                double[] f = {12,35,16,1,21,45,19,22,11,33};
//                double[] d = {3,2,3,1,9,6,4,16,9,27};
//                double max = 200;
//				 *
//				 */
//				double min = -2000;
//				double[] v= {-990.13, -404.57, 264.01, 878.57, 1474.02, 2029.03, 2631.72, 3200.56, 3784.27, 4332.54, 4958.26, 5509.25, 6071.99, 6669.90, 7172.96, 7712.58, 8238.27, 8862.93, 9413.98, 9993.46};
//				double[] f = {5, 525, 1055, 1580, 2105, 2630, 3160, 3685, 4210, 4735, 5265, 5790, 6315, 6840, 7370, 7895, 8420, 8945, 9475, 10000};
//				double[] d = {20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20};
//
//				String[] va = {"abc","def","ghi","jkl","mnm","nop","obc","pef","qhi","rkl","snm","top","ubc","vef","whi","xkl","znm"};
//				String minStr = "a";
//				/*
//                double min = -210000000;
//                double[] v = {-110000000,-11000000,-1100000,-110000,-11000,-1000,-100,-10,0,10,100,1000,11000,110000,1100000,11000000,110000000};
//                double[] f = {1,2,3,4,5,6,7,8,9,20,10,11,12,13,14,15,16};
//                double[] d = {1,1,1,1,1,2,1,1,1,1,1,2,1,1,1,1,1};
//                 double max = 110000000;
//				 */
//				double prevValCount = 0;
//				for(int i=0;i<v.length;i++)
//				{
//					double freq = f[i] -  prevValCount;
//					prevValCount = f[i];
//					Constants.CPrintToConsole(":: "+freq, Constants.DEBUG_SECOND_LEVEL_Information);
//					f[i] = freq;
//				}
//				double totalFreqCount = 0;
//				double totalDistCount = 0;
//
//				for(int i=0;i<v.length;i++)
//				{
//					totalFreqCount = totalFreqCount+f[i];
//					totalDistCount = totalDistCount + d[i];
//				}
//				ArrayList<BucketItem> buckets = new ArrayList<BucketItem>();
//				double totalFreqPercent = 0;
//				for(int i=0;i<v.length;i++)
//				{
//					double freqPercent = (f[i]*100.0) / totalFreqCount;
//					DecimalFormat decimalFormat = new DecimalFormat("#.##");
//					freqPercent =  Double.valueOf(decimalFormat.format(freqPercent));
//					totalFreqPercent = totalFreqPercent + freqPercent;
//					if( i == v.length -1)
//					{ // adjust to 100.00
//						double remainder = 100.00 - totalFreqPercent;
//						freqPercent = freqPercent + remainder;
//						totalFreqPercent = totalFreqPercent + remainder;
//						freqPercent =  Double.valueOf(decimalFormat.format(freqPercent));
//					}
//					double distinctCountPercent = (d[i]*100.0) / totalDistCount;
//					DataType val, prevVal;
//					if (mode.equals("double")) {
//						val = new DataType(DataType.DOUBLE, "" + v[i]);
//						if( i == 0)
//							prevVal = new DataType(DataType.DOUBLE, "" + min);
//						else
//							prevVal = new DataType(DataType.DOUBLE, "" + v[i-1]);
//					} else if (mode.equals("integer")) {
//						val = new DataType(DataType.INTEGER, "" + v[i]);
//						if( i == 0)
//							prevVal = new DataType(DataType.INTEGER, "" + min);
//						else
//							prevVal = new DataType(DataType.INTEGER, "" + v[i-1]);
//					} else {
//						val = new DataType(DataType.VARCHAR, "" + va[i]);
//						if( i == 0)
//							prevVal = new DataType(DataType.VARCHAR, "" + minStr);
//						else
//							prevVal = new DataType(DataType.VARCHAR, "" + v[i-1]);
//					}
//					Constants.CPrintToConsole("("+prevVal.getString()+", "+val.getString()+" ):: "+f[i]+" ("+freqPercent+")"+" : "+d[i]+" ("+distinctCountPercent+")", Constants.DEBUG_SECOND_LEVEL_Information);
//					BucketItem bucket = new BucketItem(prevVal,val, f[i], d[i], freqPercent, distinctCountPercent);
//					buckets.add(bucket);
//				}
//				if (mode.equals("double")) {
//				} else if (mode.equals("integer")) {
//				} else {
//				}
//				// 1.0 * (totalFreqCount / 100.0); // 1% of totalRows
//
//				//DataType minWidth = new DataType(DataType.INTEGER,"1");
//				if (mode.equals("double")) {
//					//new GraphHistogram(GraphHistogram.FREQUENCY_MODE_WITHBB, IntegerTotalFreqCount,IntegerTotalDistinctCount,buckets,minDT,maxDT,minHeight,minWidth,"s_acctbal",DataType.DOUBLE,db2Hist,unique).setVisible(true);
//				} else if (mode.equals("integer")) {
//					//new GraphHistogram(GraphHistogram.FREQUENCY_MODE_WITHBB, IntegerTotalFreqCount,IntegerTotalDistinctCount,buckets,minDT,maxDT,minHeight,minWidth,"testColumn",DataType.INTEGER,db2Hist,unique).setVisible(true);
//				} else {
//					//new GraphHistogram(GraphHistogram.FREQUENCY_MODE_WITHOUTBB, IntegerTotalFreqCount,IntegerTotalDistinctCount,buckets,minDT,maxDT,minHeight,minWidth,"testColumn",DataType.VARCHAR,db2Hist,unique).setVisible(true);
//				}
//				//new GraphHistogram(GraphHistogram.DISTINCT_COUNT_MODE, IntegerTotalDistCount,buckets,minDT,maxDT,minHeight,minWidth,"testColumn","double",db2Hist).setVisible(true);
//			}
//		});
//	}

	@Override
	public void chartMouseClicked(ChartMouseEvent event) {
		ChartEntity entity = event.getEntity();
		if (entity instanceof XYItemEntity) {
			XYItemEntity xyItemEntity = (XYItemEntity) entity;
			int item = xyItemEntity.getItem();
			Integer itemInt = new Integer(item);
			if(this.localMode == GraphHistogram.BUCKET_CHANGE_MODE)
			{
				selectedBucket = item;
				BucketItem bucket = this.buckets.get(item);
				String lValue;
				if(item > 0)
				{
					lValue = this.buckets.get(item-1).getValue().getString();
				}
				else
				{
					lValue = minValue.getString();
				}
				String rValue = this.buckets.get(item).getValue().getString();
				String count;
				if(globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
				{
					count = "Distinct Count: "+(long)this.buckets.get(item).getDistinctCount()+ "; Distinct count %: "+Math.round(this.buckets.get(item).getDistinctCountPercent()*100.0)/100.0;
				}
				else
				{
					count = "Row count: "+(long)this.buckets.get(item).getFreq()+ "; Row count %: "+Math.round(this.buckets.get(item).getFreqPercent()*100.0)/100.0;
				}

				String text = "("+lValue+", "+rValue+"); "+count;
				colInfo.setText(text);
				verticalSlider.setEnabled(true);
				formattedTextField.setEnabled(true);
				horizontalSlider.setEnabled(true);
				leftBoundary.setEnabled(true);
				rightBoundary.setEnabled(true);
				setSlider = true;
				int sliderVal = (int)(getPercent(bucket)*100);
				verticalSlider.setValue(sliderVal);
				double valueDouble = (double)sliderVal / 100.0;
	            formattedTextField.setText(String.valueOf(valueDouble));
				setSlider = false;
				verticalSlider.repaint();
				if(globalMode == GraphHistogram.FREQUENCY_MODE_WITHBB || globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB)
				{
					horizontalSlider.setEnabled(true);
					leftBoundary.setEnabled(true);
					rightBoundary.setEnabled(true);
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getDouble());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getDouble()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getDouble());
							leftBoundary.setText(String.valueOf(minValue.getDouble()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getDouble());
						rightBoundary.setText(String.valueOf(bucket.getValue().getDouble()));
					} else if (DataType.isInteger(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getInteger());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getInteger()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getInteger());
							leftBoundary.setText(String.valueOf(minValue.getInteger()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getInteger());
						rightBoundary.setText(String.valueOf(bucket.getValue().getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getBigDecimal().doubleValue());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getBigDecimal().doubleValue()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getBigDecimal().doubleValue());
							leftBoundary.setText(String.valueOf(minValue.getBigDecimal().doubleValue()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getBigDecimal().doubleValue());
						rightBoundary.setText(String.valueOf(bucket.getValue().getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getBigInteger().longValue());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getBigInteger().longValue()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getBigInteger().longValue());
							leftBoundary.setText(String.valueOf(minValue.getBigInteger().longValue()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getBigInteger().longValue());
						rightBoundary.setText(String.valueOf(bucket.getValue().getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
					horizontalSlider.repaint();
				}
				else
				{
					horizontalSlider.setEnabled(false);
					leftBoundary.setEnabled(false);
					rightBoundary.setEnabled(false);
				}
				jPanel1.repaint();

				if(globalMode == GraphHistogram.DISTINCT_COUNT_MODE)
				{
					startMergeButton.setEnabled(false);
					splitButton.setEnabled(false);
				}
				else
				{
					startMergeButton.setEnabled(true);
				//	focusFirstSkewButton.setEnabled(true);
				//	focusLastSkewButton.setEnabled(true);
					splitButton.setEnabled(true);
				}
			}
			else if(this.localMode == GraphHistogram.DISTRIBUTE_MODE)
			{
				if(selectedBucketsList.contains(itemInt))
				{ // Already selected. Unselect item now.
					selectedBucketsList.remove(itemInt);
				}
				else
				{
					selectedBucketsList.add(itemInt);
				}
				jPanel1.repaint();
			}
			else if(this.localMode == GraphHistogram.MERGE_MODE)
			{
				if(selectedBucketsList.contains(itemInt))
				{ // Already selected. Unselect item now.
					// Remove only if it is first or last item
					int firstEle = (int)selectedBucketsList.get(0);
					int lastEle = selectedBucketsList.get(selectedBucketsList.size()-1);
					if(item == firstEle || item == lastEle)
					{
						selectedBucketsList.remove(itemInt);
					}

				}
				else if(selectedBucketsList.size() > 0)
				{
					// We maintain Sorted Order.
					// If item is before first ele or after last ele then add it to list
					int firstEle = (int)selectedBucketsList.get(0);
					int lastEle = selectedBucketsList.get(selectedBucketsList.size()-1);
					if(item == firstEle-1)
					{
						ArrayList<Integer> temp = new ArrayList<Integer>();
						temp.add(itemInt);
						for(int i=0;i<selectedBucketsList.size();i++)
						{
							temp.add(selectedBucketsList.get(i));
						}
						selectedBucketsList = null;
						selectedBucketsList = temp;
					}
					else if(item == lastEle+1)
					{
						selectedBucketsList.add(itemInt);
					}
				}
				else
				{ // First Item
					selectedBucketsList.add(itemInt);
				}
				jPanel1.repaint();
			}
		}
	}

	@Override
	public void chartMouseMoved(ChartMouseEvent event) {
	}

	@Override
	public void mouseClicked(MouseEvent e) {

		setVerticalSliderToolTip( e );
		if(e.getComponent() instanceof JFrame) // Clicked on outside
		{
			setMode(localMode);
			createChart();
		}
	}

	@Override
	public void mousePressed(MouseEvent e) {
		setVerticalSliderToolTip( e );
	}

	@Override
	public void mouseReleased(MouseEvent e) {
		setVerticalSliderToolTip( e );
	}

	@Override
	public void mouseEntered(MouseEvent e) {

	}

	@Override
	public void mouseExited(MouseEvent e) {

	}

	@Override
	public void mouseDragged(MouseEvent e) {
		setVerticalSliderToolTip( e );
	}

	@Override
	public void mouseMoved(MouseEvent e) {

	}
	/** GUI Variables **/
	// Used to show the tooltup text for Vertical Slider when it is being modified.
	final JPopupMenu pop = new JPopupMenu();
	JMenuItem item;

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton addBucketAtEndButton;
	private javax.swing.JButton addBucketAtFrontButton;
	private javax.swing.JLabel colInfo;
	private javax.swing.JButton distributeButton;
	private javax.swing.JButton finishButton;
	private com.visutools.nav.bislider.BiSlider horizontalSlider;
	private javax.swing.JButton invSelectedButton;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JButton mergeButton;
	private javax.swing.JButton redoButton;
	private javax.swing.JButton selectedButton;
	private javax.swing.JButton splitButton;
	private javax.swing.JButton startMergeButton;
	private javax.swing.JButton undoButton;
	
	private javax.swing.JSlider verticalSlider;
	// End of variables declaration//GEN-END:variables

	/**
	 * Changes the bucket boundary of the specified bucket with the specified values.
	 * @param item bucket to change
	 * @param left left value
	 * @param right right value
	 */
	private void changeBucketBoundary(int item, double left, double right)
	{
		DataType leftDT;
		DataType rightDT;
		if(DataType.isInteger(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).intValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).intValue());			
		}else if(DataType.isNumeric(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).doubleValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).doubleValue());			
		}else if(DataType.isBigDecimal(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).longValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).longValue());			
		}else{
			leftDT = new DataType(dataType,""+left);
			rightDT = new DataType(dataType,""+right);
		}
		if(item < 0){
			return;			
		}
		BucketItem currentBucket = buckets.get(item);
		if(item > 0)
		{
			BucketItem prevBucket = buckets.get(item-1);
			//if(left < prevBucket.getValue())
			if(leftDT.compare(prevBucket.getValue()) < 0)
			{
				// Left moved on the leftSide
				// Ensure minWidth on the prevBucket
				int prevPrev = item -2;
				DataType prevPrevValue;
				if(prevPrev < 0)
					prevPrevValue = new DataType(minValue);
				else
					prevPrevValue = new DataType(buckets.get(prevPrev).getValue());
				prevPrevValue.add(minWidth);
				//if(left  > (prevPrevValue + minWidth))
				if(leftDT.compare(prevPrevValue) > 0)
				{
					prevBucket.setValue(new DataType(leftDT));
					currentBucket.setLValue(new DataType(leftDT));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getDouble());
						leftBoundary.setText(String.valueOf(leftDT.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getInteger());
						leftBoundary.setText(String.valueOf(leftDT.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getBigDecimal().doubleValue());
						leftBoundary.setText(String.valueOf(leftDT.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getBigInteger().longValue());
						leftBoundary.setText(String.valueOf(leftDT.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}
				else
				{
					prevBucket.setValue(new DataType(prevPrevValue));
					currentBucket.setLValue(new DataType(prevPrevValue));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMinimumColoredValue(prevPrevValue.getDouble());
						leftBoundary.setText(String.valueOf(prevPrevValue.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMinimumColoredValue(prevPrevValue.getInteger());
						leftBoundary.setText(String.valueOf(prevPrevValue.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMinimumColoredValue(prevPrevValue.getBigDecimal().doubleValue());
						leftBoundary.setText(String.valueOf(prevPrevValue.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMinimumColoredValue(prevPrevValue.getBigInteger().longValue());
						leftBoundary.setText(String.valueOf(prevPrevValue.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}

			}
			//else if(left > prevBucket.getValue())
			else if(leftDT.compare(prevBucket.getValue()) > 0)
			{
				// Left moved on the rightSide
				// Ensure minWidth on currentBucket
				// Increase the prev Bucket Boundary
				DataType curValue = new DataType(currentBucket.getValue());
				curValue.subtract(minWidth);
				if(leftDT.compare(curValue)  < 0)
				{
					prevBucket.setValue(new DataType(leftDT));
					currentBucket.setLValue(new DataType(leftDT));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getDouble());
						leftBoundary.setText(String.valueOf(leftDT.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getInteger());
						leftBoundary.setText(String.valueOf(leftDT.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getBigDecimal().doubleValue());
						leftBoundary.setText(String.valueOf(leftDT.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMinimumColoredValue(leftDT.getBigInteger().longValue());
						leftBoundary.setText(String.valueOf(leftDT.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}
				else
				{
					prevBucket.setValue(new DataType(curValue));
					currentBucket.setLValue(new DataType(curValue));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMinimumColoredValue(curValue.getDouble());
						leftBoundary.setText(String.valueOf(curValue.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMinimumColoredValue(curValue.getInteger());
						leftBoundary.setText(String.valueOf(curValue.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMinimumColoredValue(curValue.getBigDecimal().doubleValue());
						leftBoundary.setText(String.valueOf(curValue.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMinimumColoredValue(curValue.getBigInteger().longValue());
						leftBoundary.setText(String.valueOf(curValue.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}
			}
			//else // left is not moved
		}
		else
		{ // FirstBucket - Left must be minValue
			doNotFireHorizontalSliderChangeEvent = true;
			if (DataType.isDouble(dataType)) {
				horizontalSlider.setMinimumColoredValue(minValue.getDouble());
				leftBoundary.setText(String.valueOf(minValue.getDouble()));
			} else if (DataType.isInteger(dataType)) {
				horizontalSlider.setMinimumColoredValue(minValue.getInteger());
				leftBoundary.setText(String.valueOf(minValue.getInteger()));
			} else if (DataType.isNumeric(dataType)) {
				horizontalSlider.setMinimumColoredValue(minValue.getBigDecimal().doubleValue());
				leftBoundary.setText(String.valueOf(minValue.getBigDecimal().doubleValue()));
			} else if (DataType.isBigDecimal(dataType)) {
				horizontalSlider.setMinimumColoredValue(minValue.getBigInteger().longValue());
				leftBoundary.setText(String.valueOf(minValue.getBigInteger().longValue()));
			}
			doNotFireHorizontalSliderChangeEvent = false;
		}

		if(item < buckets.size() - 1)
		{
			BucketItem nextBucket = buckets.get(item+1);
			//if (right > currentBucket.getValue()) {
			if (rightDT.compare(currentBucket.getValue()) > 0) {
				// right moved on the rightSide
				// Ensure minWidth on nextBucket
				DataType nextValue = new DataType(nextBucket.getValue());
				nextValue.subtract(minWidth);
				//if(right  < (nextValue - minWidth))
				if(rightDT.compare(nextValue)< 0)
				{
					currentBucket.setValue(new DataType(rightDT));
					nextBucket.setLValue(new DataType(rightDT));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMaximumColoredValue(rightDT.getDouble());
						rightBoundary.setText(String.valueOf(rightDT.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMaximumColoredValue(rightDT.getInteger());
						rightBoundary.setText(String.valueOf(rightDT.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMaximumColoredValue(rightDT.getBigDecimal().doubleValue());
						rightBoundary.setText(String.valueOf(rightDT.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMaximumColoredValue(rightDT.getBigInteger().longValue());
						rightBoundary.setText(String.valueOf(rightDT.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}
				else
				{
					currentBucket.setValue(new DataType(nextValue));
					nextBucket.setLValue(new DataType(nextValue));
					doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						horizontalSlider.setMaximumColoredValue(nextValue.getDouble());
						rightBoundary.setText(String.valueOf(nextValue.getDouble()));
					} else if (DataType.isInteger(dataType)) {
						horizontalSlider.setMaximumColoredValue(nextValue.getInteger());
						rightBoundary.setText(String.valueOf(nextValue.getInteger()));
					} else if (DataType.isNumeric(dataType)) {
						horizontalSlider.setMaximumColoredValue(nextValue.getBigDecimal().doubleValue());
						rightBoundary.setText(String.valueOf(nextValue.getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						horizontalSlider.setMaximumColoredValue(nextValue.getBigInteger().longValue());
						rightBoundary.setText(String.valueOf(nextValue.getBigInteger().longValue()));
					}
					doNotFireHorizontalSliderChangeEvent = false;
				}

				//} else if (right < currentBucket.getValue()) {
			} else if (rightDT.compare(currentBucket.getValue()) < 0) {
				// right moved on the leftSide
				// Ensure minWidth on currentBucket
				DataType prevValue;
				if (item > 0) {
					prevValue = new DataType(buckets.get(item - 1).getValue());
				} else {
					prevValue = new DataType(minValue);
				}
				prevValue.add(minWidth);
				//if(right  > (prevValue + minWidth))
					if(rightDT.compare(prevValue)  > 0)
					{
						currentBucket.setValue(new DataType(rightDT));
						nextBucket.setLValue(new DataType(rightDT));
						doNotFireHorizontalSliderChangeEvent = true;
						if (DataType.isDouble(dataType)) {
							horizontalSlider.setMaximumColoredValue(rightDT.getDouble());
							rightBoundary.setText(String.valueOf(rightDT.getDouble()));
						} else if (DataType.isInteger(dataType)) {
							horizontalSlider.setMaximumColoredValue(rightDT.getInteger());
							rightBoundary.setText(String.valueOf(rightDT.getInteger()));
						} else if (DataType.isNumeric(dataType)) {
							horizontalSlider.setMaximumColoredValue(rightDT.getBigDecimal().doubleValue());
							rightBoundary.setText(String.valueOf(rightDT.getBigDecimal().doubleValue()));
						} else if (DataType.isBigDecimal(dataType)) {
							horizontalSlider.setMaximumColoredValue(rightDT.getBigInteger().longValue());
							rightBoundary.setText(String.valueOf(rightDT.getBigInteger().longValue()));
						}
						doNotFireHorizontalSliderChangeEvent = false;
					}
					else
					{
						currentBucket.setValue(new DataType(prevValue));
						nextBucket.setLValue(new DataType(prevValue));
						doNotFireHorizontalSliderChangeEvent = true;
						if (DataType.isDouble(dataType)) {
							horizontalSlider.setMaximumColoredValue(prevValue.getDouble());
							rightBoundary.setText(String.valueOf(prevValue.getDouble()));
						} else if (DataType.isInteger(dataType)) {
							horizontalSlider.setMaximumColoredValue(prevValue.getInteger());
							rightBoundary.setText(String.valueOf(prevValue.getInteger()));
						} else if (DataType.isNumeric(dataType)) {
							horizontalSlider.setMaximumColoredValue(prevValue.getBigDecimal().doubleValue());
							rightBoundary.setText(String.valueOf(prevValue.getBigDecimal().doubleValue()));
						} else if (DataType.isBigDecimal(dataType)) {
							horizontalSlider.setMaximumColoredValue(prevValue.getBigInteger().longValue());
							rightBoundary.setText(String.valueOf(prevValue.getBigInteger().longValue()));
						}
						doNotFireHorizontalSliderChangeEvent = false;
					}
			}
			//else // right is not moved
		}
		else
		{ // Last Bucket. Right must be maxValue.
			doNotFireHorizontalSliderChangeEvent = true;
			if (DataType.isDouble(dataType)) {
				horizontalSlider.setMaximumColoredValue(maxValue.getDouble());
				rightBoundary.setText(String.valueOf(maxValue.getDouble()));
			} else if (DataType.isInteger(dataType)) {
				horizontalSlider.setMaximumColoredValue(maxValue.getInteger());
				rightBoundary.setText(String.valueOf(maxValue.getInteger()));
			} else if (DataType.isNumeric(dataType)) {
				horizontalSlider.setMaximumColoredValue(maxValue.getBigDecimal().doubleValue());
				rightBoundary.setText(String.valueOf(maxValue.getBigDecimal().doubleValue()));
			} else if (DataType.isBigDecimal(dataType)) {
				horizontalSlider.setMaximumColoredValue(maxValue.getBigInteger().longValue());
				rightBoundary.setText(String.valueOf(maxValue.getBigInteger().longValue()));
			}
			doNotFireHorizontalSliderChangeEvent = false;
		}

		createChart();

	}

	
	private void freshBucketBoundary(int item, double left, double right)
	{
		DataType leftDT;
		DataType rightDT;
		if(DataType.isInteger(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).intValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).intValue());			
		}else if(DataType.isNumeric(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).doubleValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).doubleValue());			
		}else if(DataType.isBigDecimal(dataType)){
			leftDT = new DataType(dataType,""+Double.valueOf(left).longValue());
			rightDT = new DataType(dataType,""+Double.valueOf(right).longValue());			
		}else{
			leftDT = new DataType(dataType,""+left);
			rightDT = new DataType(dataType,""+right);
		}
		if(item < 0){
			return;			
		}
		BucketItem currentBucket = buckets.get(item);
		currentBucket.setLValue(new DataType(leftDT));
		currentBucket.setValue(new DataType(rightDT));
		
		
		
		//createChart();

	}
	
	
	
	
	
	
	@Override
	public void newColors(ColorisationEvent ColorisationEvent_Arg) {

		if((this.globalMode == GraphHistogram.FREQUENCY_MODE_WITHBB || globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB) && this.localMode == GraphHistogram.BUCKET_CHANGE_MODE && !doNotFireHorizontalSliderChangeEvent)
		{
			double right = ColorisationEvent_Arg.getMaximum();
			double left = ColorisationEvent_Arg.getMinimum();

			java.awt.AWTEvent event = java.awt.EventQueue.getCurrentEvent();
			if( event instanceof java.awt.event.MouseEvent)
			{
				java.awt.event.MouseEvent mouseEvent = (java.awt.event.MouseEvent) event;
				/**
				 * Do not change slider when it is a double click.
				 * Double click changes the slider to some arbitrary values.
				 * Catch MOUSE_PRESSED (501) and MOUSE_RELEASED (502) and check for the clickCount
				 *
				 * http://docs.oracle.com/javase/1.4.2/docs/api/constant-values.html#java.awt.event.MouseEvent.MOUSE_PRESSED
				 */
				boolean allowChangeBoundary = true;
				if(mouseEvent.getID() == MouseEvent.MOUSE_PRESSED || mouseEvent.getID() == MouseEvent.MOUSE_RELEASED)
				{
					draggedStart = false;
					if(mouseEvent.getClickCount() > 1)
						allowChangeBoundary = false;

				}
				if(allowChangeBoundary)
				{
					// Store the history into UndoList
					if(!draggedStart && mouseEvent.getID() == MouseEvent.MOUSE_DRAGGED)
					{
						addToUndoList();
						draggedStart = true;
					}
					changeBucketBoundary(selectedBucket,left,right);
				}
				else
				{
					// Restore the horizontalSlider cursors
					if (this.localMode == GraphHistogram.BUCKET_CHANGE_MODE) {

						if ((globalMode == GraphHistogram.FREQUENCY_MODE_WITHBB || globalMode == GraphHistogram.FREQUENCY_MODE_ONLYBB) && selectedBucket != -1) {
							int item = selectedBucket;
							BucketItem bucket = this.buckets.get(item);
							horizontalSlider.setEnabled(true);
							leftBoundary.setEnabled(true);
							rightBoundary.setEnabled(true);
							doNotFireHorizontalSliderChangeEvent = true;
					if (DataType.isDouble(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getDouble());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getDouble()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getDouble());
							leftBoundary.setText(String.valueOf(minValue.getDouble()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getDouble());
						rightBoundary.setText(String.valueOf(bucket.getValue().getDouble()));
					} else if (DataType.isInteger(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getInteger().doubleValue());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getInteger().doubleValue()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getInteger().doubleValue());
							leftBoundary.setText(String.valueOf(minValue.getInteger().doubleValue()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getInteger().doubleValue());
						rightBoundary.setText(String.valueOf(bucket.getValue().getInteger().doubleValue()));
					} else if (DataType.isNumeric(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getBigDecimal().doubleValue());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getBigDecimal().doubleValue()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getBigDecimal().doubleValue());
							leftBoundary.setText(String.valueOf(minValue.getBigDecimal().doubleValue()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getBigDecimal().doubleValue());
						rightBoundary.setText(String.valueOf(bucket.getValue().getBigDecimal().doubleValue()));
					} else if (DataType.isBigDecimal(dataType)) {
						if (item != 0) {
							horizontalSlider.setMinimumColoredValue(buckets.get(item - 1).getValue().getBigInteger().doubleValue());
							leftBoundary.setText(String.valueOf(buckets.get(item - 1).getValue().getBigInteger().doubleValue()));
						} else {
							horizontalSlider.setMinimumColoredValue(minValue.getBigInteger().doubleValue());
							leftBoundary.setText(String.valueOf(minValue.getBigInteger().doubleValue()));
						}
						horizontalSlider.setMaximumColoredValue(bucket.getValue().getBigInteger().doubleValue());
						rightBoundary.setText(String.valueOf(bucket.getValue().getBigInteger().doubleValue()));
					}
							doNotFireHorizontalSliderChangeEvent = false;
							horizontalSlider.repaint();
						} else {
							horizontalSlider.setEnabled(false);
							leftBoundary.setEnabled(false);
							rightBoundary.setEnabled(false);
						}
					}

				}
			}

		}
	}


	/**
	 * All Data Structures and functions to carry out Undo / Redo Operation
	 */

	// Specifies the whether dragging of BiSlider started. Helps in adding to undoList
	private boolean draggedStart;

	// Specifies the whether the verticcalSlider is being set after clicking the chart. [No need to save history now]
	private boolean setSlider;

	// Specifies the number of operations stored in the Undo List
	private final static int HISTORY_LIMIT = 10;

	// List that maintainst the Undo ArrayLists
	private ArrayList<ArrayList<BucketItem>> undoList;

	// List that maintainst the Redo ArrayLists
	private ArrayList<ArrayList<BucketItem>> redoList;

	// List that maintainst the ExcessLess Value for undoList
	private ArrayList<Double> undoExcessLessList;

	// List that maintainst the ExcessLess Value for redoList
	private ArrayList<Double> redoExcessLessList;

	// List that maintainst the minValue Value for undoList
	private ArrayList<DataType> undoMinValueList;

	// List that maintainst the minValue Value for redoList
	private ArrayList<DataType> redoMinValueList;

	// List that maintainst the maxValue Value for undoList
	private ArrayList<DataType> undoMaxValueList;

	// List that maintainst the maxValue Value for redoList
	private ArrayList<DataType> redoMaxValueList;
	private JLabel label;
	private JLabel label_1;
	private JLabel lblUseCtrlKey;
	private GridBagConstraints gridBagConstraints_3;
	private GridBagConstraints gridBagConstraints_4;
	private GridBagConstraints gridBagConstraints_5;
	private GridBagConstraints gridBagConstraints_6;
	private GridBagConstraints gridBagConstraints_7;
	private GridBagConstraints gridBagConstraints_8;
	private GridBagConstraints gridBagConstraints_9;
	private GridBagConstraints gridBagConstraints_10;
	private GridBagConstraints gridBagConstraints_11;
	private GridBagConstraints gridBagConstraints_12;
	private JPanel panel;
	private JFormattedTextField formattedTextField;
	private JPanel panel_1;
	private JFormattedTextField leftBoundary;
	private JFormattedTextField rightBoundary;
	private JPanel panel_2;
	private JComboBox distList;
	private JLabel distLabel;
	private JPanel panel_3;
	private JFormattedTextField distInput2;
	private JLabel distLabel2;
	private JPanel panel_4;
	private JFormattedTextField distInput1;
	private JLabel distLabel1;

	/**
	 * Constructor for Undo / Redo operation.
	 * Called from the GraphHistogram constructor.
	 */
	private void initializeUndoRedo()
	{
		draggedStart = false;
		setSlider = false;
		undoList = new ArrayList<ArrayList<BucketItem>>();
		redoList = new ArrayList<ArrayList<BucketItem>>();
		undoExcessLessList = new ArrayList<Double>();
		redoExcessLessList = new ArrayList<Double>();
		undoMinValueList = new ArrayList<DataType>();
		redoMinValueList = new ArrayList<DataType>();
		undoMaxValueList = new ArrayList<DataType>();
		redoMaxValueList = new ArrayList<DataType>();
		drawUndoRedoButtons();
	}

	/**
	 * Function that enables the redo / undo button.
	 */
	private void drawUndoRedoButtons()
	{
		if(undoList != null && !undoList.isEmpty())
		{
			undoButton.setEnabled(true);
		}
		else
		{
			undoButton.setEnabled(false);
		}

		if(redoList != null && !redoList.isEmpty())
		{
			redoButton.setEnabled(true);
		}
		else
		{
			redoButton.setEnabled(false);
		}
	}
	/**
	 * Function that does the undo operation
	 *
	 * If undoList is not empty
	 *      Add current histogram to redoList
	 *      Remove the latest histogram
	 *      Restore the histogram
	 *      Refresh the chart
	 *      drawUndoRedoButtons
	 */
	private void undo()
	{
		if(!undoList.isEmpty())
		{
			addToRedoList();
			ArrayList<BucketItem> toRestore = undoList.remove(undoList.size() - 1);
			this.buckets = toRestore;
			this.excessPercent = undoExcessLessList.remove(undoExcessLessList.size() - 1);
			this.minValue = undoMinValueList.remove(undoMinValueList.size() - 1);
			this.maxValue = undoMaxValueList.remove(undoMaxValueList.size() - 1);
			setExcessLabelFinishButton();
			setMode(localMode); // createsChart
			drawUndoRedoButtons();
		}
	}

	/**
	 * Function that does the undo operation
	 *
	 * If redoList is not empty
	 *      Add current histogram to undoList (addToUndoList)
	 *      Remove the latest histogram
	 *      Restore the histogram
	 *      Refresh the chart
	 *      drawUndoRedoButtons
	 */
	private void redo()
	{
		if(!redoList.isEmpty())
		{
			addToUndoList();
			ArrayList<BucketItem> toRestore = redoList.remove(redoList.size() - 1);
			this.buckets = toRestore;
			this.excessPercent = redoExcessLessList.remove(redoExcessLessList.size() - 1);
			this.minValue = redoMinValueList.remove(redoMinValueList.size() - 1);
			this.maxValue = redoMaxValueList.remove(redoMaxValueList.size() - 1);
			setExcessLabelFinishButton();
			setMode(localMode); // createsChart
			drawUndoRedoButtons();
		}
	}

	/**
	 * Function adds an instance of histogram to undoList
	 *
	 * If undoList is full
	 *      Remove the earliest histogram
	 * Add current histogram to undoList
	 * drawUndoRedoButtons
	 */
	private void addToUndoList()
	{
		if(undoList == null) //before initalization
		{
			return;
		}
		if(undoList.size() == GraphHistogram.HISTORY_LIMIT)
		{
			undoList.remove(0);
			undoExcessLessList.remove(0);
		}
		ArrayList<BucketItem> history = new ArrayList<BucketItem>();
		for(int i=0;i<buckets.size();i++)
		{
			BucketItem bucket = buckets.get(i);
			BucketItem newBucket = new BucketItem(bucket);
			history.add(newBucket);
		}
		undoList.add(history);
		undoExcessLessList.add(new Double(this.excessPercent));
		undoMinValueList.add(new DataType(this.minValue));
		undoMaxValueList.add(new DataType(this.maxValue));
		drawUndoRedoButtons();
	}

	/**
	 * Function adds an instance of histogram to redoList
	 * It is called by undo()
	 * If redoList is  full
	 *      Remove the earliest histogram
	 * Add current histogram to redoList
	 * drawUndoRedoButtons
	 */
	private void addToRedoList()
	{
		if(redoList.size() == GraphHistogram.HISTORY_LIMIT)
		{
			redoList.remove(0);
			redoExcessLessList.remove(0);
		}
		ArrayList<BucketItem> history = new ArrayList<BucketItem>();
		for(int i=0;i<buckets.size();i++)
		{
			BucketItem bucket = buckets.get(i);
			BucketItem newBucket = new BucketItem(bucket);
			history.add(newBucket);
		}
		redoList.add(history);
		redoExcessLessList.add(new Double(this.excessPercent));
		redoMinValueList.add(new DataType(this.minValue));
		redoMaxValueList.add(new DataType(this.maxValue));
	}
	public JFormattedTextField getFormattedTextField_1() {
		return distInput2;
	}
}
/**
 * Class for rendering the graph. Any change (like colors) has to be
 * done through this renderer.
 */
class MyXYBarRenderer extends XYBarRenderer {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -7048032325039805353L;
	GraphHistogram hist;

	public MyXYBarRenderer(GraphHistogram hist) {
		super();
		this.hist = hist;
	}
	/*
    public Paint getItemPaint(int series, int item) {
    // here we assume we're working with the primary dataset
    return Color.darkGray;

    }
	 */

	public boolean getShadowsVisible() {
		return false;
	}

	public boolean isDrawBarOutline() {
		return true;
	}

	public Paint getItemOutlinePaint(int series, int item) {
		/* Ashoke - Begin
		 * For better visibility of Outline of Histogram boundaries
		 */
		BasicStroke stroke = new BasicStroke(3f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND);
		this.setBaseOutlineStroke(stroke);
		/* Ashoke - End */
		if (hist.localMode == GraphHistogram.BUCKET_CHANGE_MODE && hist.selectedBucket == item) {
			return Color.GREEN;
		}
		else if (hist.localMode == GraphHistogram.DISTRIBUTE_MODE && hist.selectedBucketsList.contains(item)) {
			return Color.GREEN;
		} else if (hist.localMode == GraphHistogram.MERGE_MODE && hist.selectedBucketsList.contains(item)) {
			return Color.GREEN;
		}
		//else if(hist.localMode == HistogramTest2. && hist.selectedBucket == item))
		return Color.BLACK;
	}
}