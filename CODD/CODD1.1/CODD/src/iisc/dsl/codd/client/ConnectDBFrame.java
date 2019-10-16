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
package iisc.dsl.codd.client;

import iisc.dsl.codd.Main;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.db.db2.DB2Database;
import iisc.dsl.codd.db.mssql.MSSQLDatabase;
import iisc.dsl.codd.db.nonstopsql.NonStopSQLDatabase;
import iisc.dsl.codd.db.oracle.OracleDatabase;
import iisc.dsl.codd.db.oracle.OracleHardware;
import iisc.dsl.codd.db.postgres.PostgresDatabase;
import iisc.dsl.codd.db.sybase.SybaseDatabase;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.DBSettings;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import iisc.dsl.codd.client.gui.BaseJPanel;
import iisc.dsl.codd.client.gui.UIUtils;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingConstants;
import java.awt.Component;
/**
 * ConnectDBFrame, is used to get the connection information for the database.
 * This will be the first panel showed to user as soon as the software starts.
 * @author dsladmin
 */
public class ConnectDBFrame extends javax.swing.JFrame {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -7442910125477407426L;

	private String dbServerName, dbType, dbServerPort, dbName, dbSchema, dbCatalog, dbUserName, dbPassword, serverInstance, dbVersion; // input fields
	/**
	 * Stored Connection Parameters are read into connectionParameters.
	 * <ConfigName, ConnectionParameters>
	 * ConfigName is an unique name provided for a connection parameters.
	 * ConnectionParemeters contains server name, dbType, port, ....
	 */
	private HashMap<String, String> connectionParameters;
	/**
	 * Empty String used as a item in the Configuration ComboBox.
	 */
	//Copyright \u00a9 Indian Institute Of Science, Bangalore, India.
	private String emptyConfig;

	/**
	 * Constructor For ConnectDBFrame.
	 */
	public ConnectDBFrame() {
		initComponents();
		BaseJPanel logoPanel = new BaseJPanel("img/codd_logo_small.png");
		logoPanel.setSize(100, 45);
		getContentPane().setBackground(new java.awt.Color(255, 255, 204));
		setLocationRelativeTo(null);
		emptyConfig = new String("");
		loadConfig();
		UIUtils.disableTextField(textServerInstance , textCatalogName , textVersion);
	}

	/**
	 * Constructor For ConnectDBFrame. Used by the back button of GetRelationFrame, so that the previous connection values are retained.
	 * @param dbServerName Database Server Name (e.g. localhost / 10.16.14.22)
	 * @param dbType Database Vendor (e.g. SQL Server, Oracle,..)
	 * @param dbServerPort Port Number
	 * @param dbName Database Name
	 * @param dbSchema Schema Name
	 * @param dbUserName User Name
	 * @param dbPassword User Password
	 * @param serverInstance SQL Server Instance Name
	 */
	public ConnectDBFrame(String dbServerName, String dbType, String dbServerPort, String dbName, String dbCatalog, String dbSchema, String dbUserName, String dbPassword, String serverInstance, String dbVersion) {
		initComponents();
		getContentPane().setBackground(new java.awt.Color(255, 255, 204));
		//jLabel12.setText("Copyright \u00a9 Indian Institute Of Science, Bangalore, India.");
		setLocation(300, 150);
		this.dbServerName = dbServerName;
		this.dbType = dbType;
		this.dbServerPort = dbServerPort;
		this.dbName = dbName;
		this.dbSchema = dbSchema;
		this.dbCatalog = dbCatalog;
		this.dbUserName = dbUserName;
		this.dbPassword = dbPassword;
		this.serverInstance = serverInstance;
		this.dbVersion = dbVersion;
		emptyConfig = new String("");
		if (dbType.equals(DBConstants.MSSQL) || dbType.equals(DBConstants.NONSTOPSQL)) {
			// Enable Server Instance Field.
			UIUtils.enableTextField(textServerInstance);
			if (dbType.equals(DBConstants.NONSTOPSQL)) {
				UIUtils.enableTextField( textVersion , textCatalogName );
			}
		} else {
			// Disable Server Instance Field.
			UIUtils.disableTextField( textServerInstance );
			UIUtils.enableTextField( textVersion , textCatalogName );
		}
		loadConfig();
		setValues(dbServerName, dbType, dbServerPort, dbName, dbCatalog, dbSchema, dbUserName, dbPassword, serverInstance, dbVersion);
	}


	/**
	 * Loads the Configuration ComoboBox with the stored Connection parameters.
	 */
	private void loadConfig() {
		jComboBox2.removeAllItems();
		jComboBox2.addItem(emptyConfig);
		connectionParameters = DBConstants.readStoredConnectionParameters();
		if (connectionParameters != null) {
			Set s = connectionParameters.keySet();
			Iterator i = s.iterator();
			while (i.hasNext()) {
				String key = (String) i.next();
				jComboBox2.addItem(key);
			}
			jComboBox2.setSelectedItem(emptyConfig);
		} else {
			jComboBox2.removeAllItems();
		}
	}

	/**
	 * Sets the form fields with connection parameters.
	 */
	private void setValues() {
		jComboBox1.setSelectedItem(dbType);
		jTextField1.setText(dbServerName);
		jTextField2.setText(dbServerPort);
		jTextField3.setText(dbName);
		jTextField4.setText(dbSchema);
		textCatalogName.setText(dbCatalog);
		jTextField5.setText(dbUserName);
		jPasswordField1.setText(dbPassword);
		textServerInstance.setText(serverInstance);
		textVersion.setText(dbVersion);
	}

	/**
	 * Sets the form fields with connection parameters.
	 */
	private void setValues(String dbServerName, String dbType, String dbServerPort, String dbName, String dbCatalog, String dbSchema, String dbUserName, String dbPassword, String serverInstance, String dbVersion) {
		jComboBox1.setSelectedItem(dbType);
		jTextField1.setText(dbServerName);
		jTextField2.setText(dbServerPort);
		jTextField3.setText(dbName);
		jTextField4.setText(dbSchema);
		textCatalogName.setText(dbCatalog);
		jTextField5.setText(dbUserName);
		jPasswordField1.setText(dbPassword);
		textServerInstance.setText(serverInstance);
		textVersion.setText(dbVersion);
	}


	/**
	 * Reads the connection parameters from the input fields and validates the inputs.
	 * @return null, if all inputs are valid. Error Message, if there are any validation errors.
	 */
	private String readAndValidateFields() {
		dbServerName = jTextField1.getText();
		dbType = (String) jComboBox1.getSelectedItem();
		dbServerPort = jTextField2.getText();
		dbName = jTextField3.getText();
		dbSchema = jTextField4.getText();
		if(dbType==null ||dbType.isEmpty()){
			return "First choose the Database Engine from DropDown.";
		}
		if (dbType.equalsIgnoreCase(DBConstants.NONSTOPSQL)) {
			dbCatalog = textCatalogName.getText();
			dbVersion = textVersion.getText();
		} else {
			dbCatalog = "";
			dbVersion = "";
		}
		dbUserName = jTextField5.getText();
		dbPassword = new String(jPasswordField1.getPassword());
		serverInstance = new String(textServerInstance.getText());
		String msg = new String();
		boolean error = false;

		if (dbServerName == null || dbServerName.isEmpty()) {
			error = true;
			msg = msg + " Databse Server Machine Name,";
		}
		if (dbServerPort == null || dbServerPort.isEmpty()) {
			error = true;
			msg = msg + " DB Server Port,";
		}
		if ((dbName == null || dbName.isEmpty()) && !dbType.equals(DBConstants.NONSTOPSQL)) {
			error = true;
			msg = msg + " Database Name,";
		}
		if (dbSchema == null || dbSchema.isEmpty()) {
			error = true;
			msg = msg + " Database Schema,";
		}
		if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.NONSTOPSQL))) {
			if (dbCatalog == null || dbCatalog.isEmpty()) {
				error = true;
				msg = msg + " Database Catalog,";
			}
		}
		if (dbUserName == null || dbUserName.isEmpty()) {
			error = true;
			msg = msg + " Username,";
		}
		if (dbPassword == null || dbPassword.isEmpty()) {
			error = true;
			msg = msg + " Password,";
		}
		if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.MSSQL) || dbType.equals(DBConstants.NONSTOPSQL))) {
			if (serverInstance == null || serverInstance.isEmpty()) {
				error = true;
				msg = msg + " Server Instance,";
			}
		}
		if (dbType != null && !dbType.isEmpty() && (dbType.equals(DBConstants.NONSTOPSQL))) {
			if (dbVersion == null || dbVersion.isEmpty()) {
				error = true;
				msg = msg + " Version Number,";
			}
		}
		if (error) {
			msg = msg.substring(0, msg.lastIndexOf(","));
			msg = "Entries in the fields " + msg + " are not correct. Please enter the correct values.";
			return msg;
		} else {
			return null;
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

		jDialog1 = new javax.swing.JDialog();
		jLabel9 = new javax.swing.JLabel();
		buttonGroup1 = new javax.swing.ButtonGroup();
		buttonGroup2 = new javax.swing.ButtonGroup();
		jPanel5 = new BaseJPanel("img/bg_net.png");
		jPanel4 = new javax.swing.JPanel();
		jLabel7 = new javax.swing.JLabel();
		jTextField5 = new javax.swing.JTextField();
		jLabel8 = new javax.swing.JLabel();
		jPasswordField1 = new javax.swing.JPasswordField();
		jPanel3 = new javax.swing.JPanel();
		jLabel5 = new javax.swing.JLabel();
		jLabel6 = new javax.swing.JLabel();
		lableCatalog = new javax.swing.JLabel();
		labelVersion = new javax.swing.JLabel();
		textVersion = new javax.swing.JTextField();
		textCatalogName = new javax.swing.JTextField();
		jTextField4 = new javax.swing.JTextField();
		jTextField3 = new javax.swing.JTextField();
		jPanel1 = new javax.swing.JPanel();
		jLabel2 = new javax.swing.JLabel();
		jTextField1 = new javax.swing.JTextField();
		jLabel3 = new javax.swing.JLabel();
		jLabel4 = new javax.swing.JLabel();
		labelServerInstance = new javax.swing.JLabel();
		textServerInstance = new javax.swing.JTextField();
		jTextField2 = new javax.swing.JTextField();
		jComboBox1 = new javax.swing.JComboBox();
		jComboBox2 = new javax.swing.JComboBox();
		jButton4 = new javax.swing.JButton();
		jButton3 = new javax.swing.JButton();
		jSeparator1 = new javax.swing.JSeparator();
		jLabel1 = new javax.swing.JLabel();
		jButton1 = new javax.swing.JButton();
		jButton5 = new javax.swing.JButton();
		logopanel = new BaseJPanel("img/codd_logo_small.png");

		jLabel9.setText("SQL SERVER SELECTED");

		javax.swing.GroupLayout jDialog1Layout = new javax.swing.GroupLayout(jDialog1.getContentPane());
		jDialog1.getContentPane().setLayout(jDialog1Layout);
		jDialog1Layout.setHorizontalGroup(
				jDialog1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jDialog1Layout.createSequentialGroup()
						.addGap(141, 141, 141)
						.addComponent(jLabel9)
						.addContainerGap(147, Short.MAX_VALUE))
				);
		jDialog1Layout.setVerticalGroup(
				jDialog1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jDialog1Layout.createSequentialGroup()
						.addGap(121, 121, 121)
						.addComponent(jLabel9)
						.addContainerGap(165, Short.MAX_VALUE))
				);

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
		setTitle("Database Connection Properties");
		setBounds(new java.awt.Rectangle(150, 25, 0, 0));
		setResizable(false);

		jPanel4.setBorder(javax.swing.BorderFactory.createTitledBorder("Credentials"));
		jPanel4.setOpaque(false);

		jLabel7.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel7.setText("User Name:");

		jTextField5.setToolTipText("");
		jTextField5.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jTextField5ActionPerformed(evt);
			}
		});

		jLabel8.setFont(new java.awt.Font("Tahoma", 0, 14));
		jLabel8.setText("Password:");

		jPasswordField1.setToolTipText("");

		javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
		jPanel4Layout.setHorizontalGroup(
			jPanel4Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel4Layout.createSequentialGroup()
					.addContainerGap()
					.addComponent(jLabel7)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addComponent(jTextField5, GroupLayout.PREFERRED_SIZE, 228, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.RELATED, 72, Short.MAX_VALUE)
					.addComponent(jLabel8)
					.addGap(35)
					.addComponent(jPasswordField1, GroupLayout.PREFERRED_SIZE, 233, GroupLayout.PREFERRED_SIZE)
					.addContainerGap())
		);
		jPanel4Layout.setVerticalGroup(
			jPanel4Layout.createParallelGroup(Alignment.TRAILING)
				.addGroup(jPanel4Layout.createParallelGroup(Alignment.BASELINE)
					.addComponent(jLabel7)
					.addComponent(jTextField5, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addComponent(jPasswordField1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addComponent(jLabel8))
		);
		jPanel4Layout.linkSize(SwingConstants.VERTICAL, new Component[] {jLabel7, jTextField5, jLabel8, jPasswordField1});
		jPanel4.setLayout(jPanel4Layout);

		jPanel3.setBorder(javax.swing.BorderFactory.createTitledBorder("Schema Details"));
		jPanel3.setOpaque(false);

		jLabel5.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel5.setText("Database:");

		jLabel6.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel6.setText("Schema:");

		lableCatalog.setFont(new java.awt.Font("Verdana", 0, 14));
		lableCatalog.setText("Catalog:");

		labelVersion.setFont(new java.awt.Font("Verdana", 0, 14));
		labelVersion.setText("Version:");

		textVersion.setFont(new java.awt.Font("Cambria", 0, 14));

		textCatalogName.setFont(new java.awt.Font("Cambria", 0, 14));

		jTextField4.setFont(new java.awt.Font("Cambria", 0, 14));
		jTextField4.setToolTipText("Database schema");

		jTextField3.setFont(new java.awt.Font("Cambria", 0, 14));
		jTextField3.setToolTipText("Database name");

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3.setLayout(jPanel3Layout);
		jPanel3Layout.setHorizontalGroup(
				jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
								.addGroup(jPanel3Layout.createSequentialGroup()
										.addComponent(labelVersion, javax.swing.GroupLayout.PREFERRED_SIZE, 82, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addGap(18, 18, 18)
										.addComponent(textVersion, javax.swing.GroupLayout.DEFAULT_SIZE, 257, Short.MAX_VALUE))
										.addGroup(jPanel3Layout.createSequentialGroup()
												.addComponent(jLabel5)
												.addGap(18, 18, 18)
												.addComponent(jTextField3, javax.swing.GroupLayout.DEFAULT_SIZE, 257, Short.MAX_VALUE))
												.addGroup(jPanel3Layout.createSequentialGroup()
														.addComponent(lableCatalog)
														.addGap(18, 18, 18)
														.addComponent(textCatalogName, javax.swing.GroupLayout.DEFAULT_SIZE, 257, Short.MAX_VALUE))
														.addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup()
																.addComponent(jLabel6)
																.addGap(18, 18, 18)
																.addComponent(jTextField4, javax.swing.GroupLayout.DEFAULT_SIZE, 257, Short.MAX_VALUE)))
																.addContainerGap())
				);

		jPanel3Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {jLabel5, jLabel6, labelVersion, lableCatalog});

		jPanel3Layout.setVerticalGroup(
				jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
								.addComponent(jTextField3, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
								.addComponent(jLabel5))
								.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
										.addComponent(textCatalogName, javax.swing.GroupLayout.PREFERRED_SIZE, 24, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addComponent(lableCatalog))
										.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
										.addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
												.addComponent(jTextField4, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
												.addComponent(jLabel6))
												.addGap(11, 11, 11)
												.addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
														.addComponent(textVersion, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
														.addComponent(labelVersion, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE))
														.addContainerGap(14, Short.MAX_VALUE))
				);

		jPanel3Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] {jTextField3, jTextField4, textCatalogName, textVersion});

		jPanel3Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] {jLabel5, jLabel6, labelVersion, lableCatalog});

		jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder("Database Server Details"));
		jPanel1.setOpaque(false);

		jLabel2.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel2.setText("Machine Name / IP:");

		jTextField1.setFont(new java.awt.Font("Cambria", 0, 14));
		jTextField1.setToolTipText("IP address");
		jTextField1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jTextField1ActionPerformed(evt);
			}
		});

		jLabel3.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel3.setText("Engine:");

		jLabel4.setFont(new java.awt.Font("Verdana", 0, 14));
		jLabel4.setText("Port:");

		labelServerInstance.setFont(new java.awt.Font("Verdana", 0, 14));
		labelServerInstance.setText("Server Instance:");

		textServerInstance.setFont(new java.awt.Font("Cambria", 0, 14));
		textServerInstance.setToolTipText("SQL Server Database Instance");

		jTextField2.setFont(new java.awt.Font("Cambria", 0, 14));
		jTextField2.setToolTipText("Port number of the chosen DB Engine");
		jTextField2.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jTextField2ActionPerformed(evt);
			}
		});

		jComboBox1.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "DB2", "ORACLE", "SQL SERVER", "NON STOP SQL", "POSTGRES" }));
		jComboBox1.setSelectedIndex(-1);
		jComboBox1.setSelectedItem(0);
		jComboBox1.setToolTipText("Choose the DB Engine");
		jComboBox1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jComboBox1ActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1Layout.setHorizontalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(jLabel4)
						.addComponent(jLabel2)
						.addComponent(jLabel3)
						.addComponent(labelServerInstance))
					.addGap(17)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(textServerInstance, GroupLayout.DEFAULT_SIZE, 194, Short.MAX_VALUE)
						.addComponent(jTextField1, Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 211, Short.MAX_VALUE)
						.addComponent(jTextField2, GroupLayout.DEFAULT_SIZE, 211, Short.MAX_VALUE)
						.addComponent(jComboBox1, 0, 194, Short.MAX_VALUE))
					.addContainerGap())
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(jLabel3)
						.addComponent(jComboBox1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(jTextField1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(jLabel2))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(jTextField2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(jLabel4))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(textServerInstance, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(labelServerInstance))
					.addContainerGap(18, Short.MAX_VALUE))
		);
		jPanel1.setLayout(jPanel1Layout);

		jComboBox2.setFont(new java.awt.Font("Cambria", 0, 14));
		jComboBox2.setToolTipText("Load the stored settings");
		jComboBox2.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jComboBox2ActionPerformed(evt);
			}
		});

		jButton4.setText("Delete Config");
		jButton4.setToolTipText("Deletes the configuration");
		jButton4.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton4ActionPerformed(evt);
			}
		});

		jButton3.setText("Save Config");
		jButton3.setToolTipText("Save the connection settings in to a file");
		jButton3.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton3ActionPerformed(evt);
			}
		});

		jLabel1.setFont(new java.awt.Font("Verdana", 1, 12));
		jLabel1.setText("Choose From Pre-Saved Connections:");

		jButton1.setText("Connect");
		jButton1.setToolTipText("Connect to the database");
		jButton1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton1ActionPerformed(evt);
			}
		});

		jButton5.setText("Refresh");
		jButton5.setToolTipText("Refreshes the connection configurations.");
		jButton5.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton5ActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout logopanelLayout = new javax.swing.GroupLayout(logopanel);
		logopanel.setLayout(logopanelLayout);
		logopanelLayout.setHorizontalGroup(
				logopanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 100, Short.MAX_VALUE)
				);
		logopanelLayout.setVerticalGroup(
				logopanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGap(0, 45, Short.MAX_VALUE)
				);

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5Layout.setHorizontalGroup(
			jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addGroup(jPanel5Layout.createSequentialGroup()
							.addComponent(jLabel1)
							.addGap(18)
							.addComponent(jComboBox2, GroupLayout.PREFERRED_SIZE, 511, GroupLayout.PREFERRED_SIZE))
						.addGroup(Alignment.TRAILING, jPanel5Layout.createSequentialGroup()
							.addGroup(jPanel5Layout.createParallelGroup(Alignment.TRAILING)
								.addComponent(jPanel4, Alignment.LEADING, GroupLayout.DEFAULT_SIZE, 774, Short.MAX_VALUE)
								.addGroup(jPanel5Layout.createSequentialGroup()
									.addComponent(jButton3)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(jButton5)
									.addPreferredGap(ComponentPlacement.UNRELATED)
									.addComponent(jButton4)
									.addPreferredGap(ComponentPlacement.RELATED)
									.addComponent(jButton1, GroupLayout.PREFERRED_SIZE, 124, GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(ComponentPlacement.RELATED, 130, Short.MAX_VALUE)
									.addComponent(logopanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addGroup(jPanel5Layout.createSequentialGroup()
									.addComponent(jPanel1, GroupLayout.DEFAULT_SIZE, 379, Short.MAX_VALUE)
									.addGap(18)
									.addComponent(jPanel3, GroupLayout.PREFERRED_SIZE, 377, GroupLayout.PREFERRED_SIZE)))
							.addGap(16)))
					.addContainerGap())
		);
		jPanel5Layout.setVerticalGroup(
			jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(jLabel1, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addComponent(jComboBox2, GroupLayout.PREFERRED_SIZE, 28, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 11, GroupLayout.PREFERRED_SIZE)
					.addGap(18)
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING, false)
						.addComponent(jPanel1, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
						.addComponent(jPanel3, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
					.addGap(18)
					.addComponent(jPanel4, GroupLayout.PREFERRED_SIZE, 51, GroupLayout.PREFERRED_SIZE)
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.CENTER)
						.addComponent(jButton3, GroupLayout.PREFERRED_SIZE, 29, GroupLayout.PREFERRED_SIZE)
						.addComponent(jButton5)
						.addComponent(jButton4, GroupLayout.PREFERRED_SIZE, 29, GroupLayout.PREFERRED_SIZE)
						.addComponent(jButton1, GroupLayout.PREFERRED_SIZE, 39, GroupLayout.PREFERRED_SIZE)
						.addComponent(logopanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGap(23))
		);
		jPanel5Layout.linkSize(SwingConstants.VERTICAL, new Component[] {jButton4, jButton3, jButton1, jButton5});
		jPanel5Layout.linkSize(SwingConstants.HORIZONTAL, new Component[] {jButton4, jButton3, jButton1, jButton5});
		jPanel5.setLayout(jPanel5Layout);

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
		layout.setHorizontalGroup(
				layout.createParallelGroup(Alignment.LEADING)
				.addComponent(jPanel5, GroupLayout.DEFAULT_SIZE, 700, Short.MAX_VALUE)
				);
		layout.setVerticalGroup(
				layout.createParallelGroup(Alignment.LEADING)
				.addComponent(jPanel5, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
				);
		getContentPane().setLayout(layout);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
		String retString = readAndValidateFields();
		if (retString == null) {
			try {
				DBSettings settings = new DBSettings(dbServerName, dbServerPort, dbType, dbName, dbSchema, dbUserName, dbPassword);
				if (dbType.equals(DBConstants.MSSQL)) {
					settings.setSqlServerInstanceName(serverInstance);
				}
				if (dbType.equals(DBConstants.NONSTOPSQL)) {
					settings.setCatalog(dbCatalog);
					settings.setVersion(dbVersion);
					settings.setSqlServerInstanceName(serverInstance);
				}
				Database database = null;
				if (dbType.equals(DBConstants.DB2)) {
					database = (Database) new DB2Database(settings);
				} else if (dbType.equals(DBConstants.MSSQL)) {
					database = (Database) new MSSQLDatabase(settings);
				} else if (dbType.equals(DBConstants.ORACLE)) {
					database = (Database) new OracleDatabase(settings);
				} else if (dbType.equals(DBConstants.POSTGRES)) {
					database = (Database) new PostgresDatabase(settings);
				} else if (dbType.equals(DBConstants.SYBASE)) {
					database = (Database) new SybaseDatabase(settings);
				} else if (dbType.equals(DBConstants.NONSTOPSQL)) {
					database = (Database) new NonStopSQLDatabase(settings);
				}

				/*
				 * Important: 
				 * The variable "isJdbcDriver" will have the following values:
				 * "true" - If working on databases other than HP NonStopSQLMX, eg. DB2, Oracle etc.
				 * "true" - If working on HP NonStopSQLMX, and using the official JDBC driver.
				 * "false" - If working on HP NonStopSQLMX, but not using the official JDBC driver.
				 */
				if(Main.isJdbcDriver){
					if (database == null) {
						JOptionPane.showMessageDialog(null, "Error: Could not get database object.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
					} 
					else if (database.connect()) {
						Constants.CPrintToConsole("Connected to database. ", Constants.DEBUG_FIRST_LEVEL_Information);
						if (!database.stopAutoUpdateOfStats()) {
							JOptionPane.showMessageDialog(null, "Warning: Could not able to stop auto maintanance of statistics.", "CODD - Warning", JOptionPane.INFORMATION_MESSAGE);
							return;
						}
						//if(dbType.equals(DBConstants.DB2)){
						//	new ConfigurationTypeSelection(database).setVisible(true);
						//	this.dispose();
						//}else{
						if(dbType.equals(DBConstants.ORACLE)){
							new OracleHardware(database).setVisible(true);
							this.dispose();
						} else{
							new GetRelationFrame(database).setVisible(true);
							this.dispose();	
						}
						//}
					} 
					else {
						JOptionPane.showMessageDialog(null, "Error: Could not able to connect to database.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
						return;
					}
				}else{
					new GetRelationFrame(database).setVisible(true);
					this.dispose();
					Constants.CPrintToConsole("Connected to Database.", Constants.DEBUG_FIRST_LEVEL_Information);
				}           	
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, e.getMessage() , "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
		} else {
			JOptionPane.showMessageDialog(null, retString, "CODD - Error", 0);
		}

	}//GEN-LAST:event_jButton1ActionPerformed

	private void jComboBox1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBox1ActionPerformed
		jTextField1.setText("");
		jTextField2.setText("");
		jTextField3.setText("");
		jTextField4.setText("");
		jTextField5.setText("");
		jPasswordField1.setText("");
		textServerInstance.setText("");
		textVersion.setText("");
		textCatalogName.setText("");

		if (jComboBox1.getSelectedItem().equals("SQL SERVER") || jComboBox1.getSelectedItem().equals("NON STOP SQL")) {
			// Enable Server Instance Field.
			UIUtils.enableTextField( textServerInstance );
			UIUtils.disableTextField( textVersion , textCatalogName );

			if (jComboBox1.getSelectedItem().equals("NON STOP SQL")) {
				UIUtils.enableTextField( textVersion , textCatalogName );
			}

		} else {
			// Disable Server Instance Field.
			UIUtils.disableTextField( textServerInstance , textVersion , textCatalogName );
		}
		if(jComboBox1.getSelectedItem().equals("NON STOP SQL")) {
			UIUtils.disableTextField(jTextField3);
		}
		if(!jComboBox1.getSelectedItem().equals("NON STOP SQL"))  {
			UIUtils.enableTextField(jTextField3);
		}
		// Set the port number automatically based on the chosen DB Engine.
		if (jComboBox1.getSelectedItem().equals(DBConstants.MSSQL)) {
			jTextField2.setText("1433");
		} else if (jComboBox1.getSelectedItem().equals(DBConstants.ORACLE)) {
			jTextField2.setText("1521");
		} else if (jComboBox1.getSelectedItem().equals(DBConstants.DB2)) {
			jTextField2.setText("50000");
		} else if (jComboBox1.getSelectedItem().equals(DBConstants.SYBASE)) {
			jTextField2.setText("5000");
		} else if (jComboBox1.getSelectedItem().equals(DBConstants.POSTGRES)) {
			jTextField2.setText("5432");
		} else if (jComboBox1.getSelectedItem().equals("NON STOP SQL")) {        // Added by - Deepali Nemade
			jTextField2.setText("23456");
		} else {
			// Set Empty String if none of the engine is selected.
			jTextField2.setText("");
		}
	}//GEN-LAST:event_jComboBox1ActionPerformed

	private void jButton3ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton3ActionPerformed
		/**
		 * Save the current Settings in to a configuration file.
		 */
		String retString = readAndValidateFields();
		if (retString == null) {
			// Entries are fine. Go ahead and save.
			try {
				JFileChooser filechooser = new JFileChooser();
				filechooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
				filechooser.addChoosableFileFilter(new MyFilter(".cfg"));
				String currDir = Constants.WorkingDirectory;
				filechooser.setCurrentDirectory(new File(currDir));
				int returnvalue = filechooser.showSaveDialog(this);
				if (returnvalue == JFileChooser.APPROVE_OPTION) {
					File selectedFile = filechooser.getSelectedFile();
					String fileName = selectedFile.getName();
					if (!fileName.endsWith(".cfg")) {
						fileName = fileName + ".cfg";
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName)));
					writer.write("ServerName::" + dbServerName + "\n");
					writer.write("DBType::" + dbType + "\n");
					writer.write("Port::" + dbServerPort + "\n");
					if(!dbType.equals(DBConstants.NONSTOPSQL)) 
						writer.write("DBName::" + dbName + "\n");
					writer.write("DBSchema::" + dbSchema + "\n");
					writer.write("UserName::" + dbUserName + "\n");
					writer.write("Password::" + dbPassword + "\n");
					if (dbType.equals(DBConstants.MSSQL)) {
						writer.write("ServerInstance::" + serverInstance + "\n");
					}
					if (dbType.equals(DBConstants.NONSTOPSQL)) {
						writer.write("DBCatalog::" + dbCatalog + "\n");
						writer.write("ServerInstance::" + serverInstance + "\n");
						writer.write("VersionNumber::" + dbVersion + "\n");
					}
					writer.close();
					/**
					 * Write into Global Configuration File
					 */
					String fileNameExCfg = fileName.substring(0, fileName.indexOf(".cfg"));
					String lineToRemove = null;
					BufferedReader reader = null;
					try {
						// Check for containment, the same parameters. In that case delete the old file.
						reader = new BufferedReader(new FileReader(new File(Constants.ConfigFileName)));
						String line = reader.readLine();
						while (line != null) {
							String[] temp = line.split("::");
							if (temp[0] != null && temp[0].equals(fileNameExCfg)) {
								lineToRemove = fileNameExCfg;
							}
							line = reader.readLine();
						}
						reader.close();
					} catch (FileNotFoundException e) {
					}
					if (lineToRemove != null) {
						removeLineFromConfigFile(lineToRemove);
					}
					writer = new BufferedWriter(new FileWriter(new File(Constants.ConfigFileName), true));  //true for append
					String Parameters = dbServerName + "%%" + dbType + "%%" + dbServerPort + "%%" + dbName + "%%" + dbSchema + "%%" + dbUserName + "%%" + dbPassword;
					if (dbType.equals(DBConstants.MSSQL)) {
						Parameters = Parameters + "%%" + serverInstance;
					}
					if (dbType.equals(DBConstants.NONSTOPSQL)) {
						Parameters = Parameters + "%%" + dbCatalog + "%%" + serverInstance + "%%" + dbVersion;
					}
					writer.write(fileNameExCfg + "::" + Parameters + "\n");
					writer.close();
					JOptionPane.showMessageDialog(null, "Database Connection Settings are saved successfully in the file " + fileName, "CODD", JOptionPane.INFORMATION_MESSAGE);
					loadConfig(); // Loads the configuration File.
				}
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
			}
		} else {
			JOptionPane.showMessageDialog(null, retString, "CODD - Error", JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_jButton3ActionPerformed

	private void jButton4ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton4ActionPerformed
		/**
		 * Delete the chosen the file.
		 */
		String configName = (String) jComboBox2.getSelectedItem();
		if (configName != null && !configName.equals(emptyConfig)) {
			removeLineFromConfigFile(configName);
		}
		loadConfig();
	}//GEN-LAST:event_jButton4ActionPerformed

	private void jComboBox2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jComboBox2ActionPerformed
		/**
		 * Load the selected Configuration.
		 */
		if (jComboBox2.getSelectedItem() == null) {
			return;
		}
		if (jComboBox2.getSelectedItem().equals(emptyConfig)) {
			dbServerName = emptyConfig;
			dbType = emptyConfig;
			dbServerPort = emptyConfig;
			dbName = emptyConfig;
			dbSchema = emptyConfig;
			dbCatalog = emptyConfig;
			dbUserName = emptyConfig;
			dbPassword = emptyConfig;
			serverInstance = emptyConfig;
			dbVersion = emptyConfig;
			setValues();
			return;
		}
		String params = connectionParameters.get(jComboBox2.getSelectedItem());
		String[] temp = params.split("%%");
		if (temp.length != 7 && temp.length != 8 && temp.length != 10) {
			JOptionPane.showMessageDialog(null, "Error in reading configuration.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
		} else {
			dbServerName = temp[0];
			dbType = temp[1];
			dbServerPort = temp[2];
			dbName = temp[3];
			dbSchema = temp[4];
			dbUserName = temp[5];
			dbPassword = temp[6];
			if (dbType.equals(DBConstants.MSSQL)) {
				serverInstance = temp[7];
			} else {
				dbCatalog = emptyConfig;
				serverInstance = emptyConfig;
				dbVersion = emptyConfig;
				if (dbType.equals(DBConstants.NONSTOPSQL)) {
					dbCatalog = temp[7];
					serverInstance = temp[8];
					dbVersion = temp[9];
				} else {
					dbCatalog = emptyConfig;
					serverInstance = emptyConfig;
					dbVersion = emptyConfig;
				}
			}
			setValues();
		}

	}//GEN-LAST:event_jComboBox2ActionPerformed

	private void jButton5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton5ActionPerformed
		// Refreshes the configuraion from Config Files
		loadConfig();
	}//GEN-LAST:event_jButton5ActionPerformed

	private void jTextField1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField1ActionPerformed
	}//GEN-LAST:event_jTextField1ActionPerformed

	private void jTextField2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField2ActionPerformed
	}//GEN-LAST:event_jTextField2ActionPerformed

	private void jTextField5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField5ActionPerformed
	}//GEN-LAST:event_jTextField5ActionPerformed

	private void removeLineFromConfigFile(String lineToRemove) {
		/**
		 * Removes the configuration from the file.
		 */
		Constants.CPrintToConsole("Removing Configuration of " + lineToRemove, Constants.DEBUG_FIRST_LEVEL_Information);
		try {
			File inputFile = new File(Constants.ConfigFileName);
			String tempFileName = "myTempFile";
			File tempFile = new File(tempFileName);

			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

			String currentLine;

			while ((currentLine = reader.readLine()) != null) {
				// trim newline when comparing with lineToRemove
				String configName = currentLine.substring(0, currentLine.indexOf("::"));
				if (configName.equals(lineToRemove)) {
					continue;
				}
				writer.write(currentLine + "\n");
			}
			writer.close();
			reader.close();

			BufferedReader br = new BufferedReader(new FileReader(new File(tempFileName)));
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Constants.ConfigFileName)));
			String sCurrentLine = "";

			while ((sCurrentLine = br.readLine()) != null) {
				bw.write(sCurrentLine);
				bw.newLine();
			}

			br.close();
			bw.close();

			File org = new File(tempFileName);
			org.delete();
		} catch (Exception e) {
			Constants.CPrintErrToConsole(e);
		}
	}
	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.ButtonGroup buttonGroup1;
	private javax.swing.ButtonGroup buttonGroup2;
	private javax.swing.JButton jButton1;
	private javax.swing.JButton jButton3;
	private javax.swing.JButton jButton4;
	private javax.swing.JButton jButton5;
	private javax.swing.JComboBox jComboBox1;
	private javax.swing.JComboBox jComboBox2;
	private javax.swing.JDialog jDialog1;
	private javax.swing.JLabel jLabel1;
	private javax.swing.JLabel jLabel2;
	private javax.swing.JLabel jLabel3;
	private javax.swing.JLabel jLabel4;
	private javax.swing.JLabel jLabel5;
	private javax.swing.JLabel jLabel6;
	private javax.swing.JLabel jLabel7;
	private javax.swing.JLabel jLabel8;
	private javax.swing.JLabel jLabel9;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel3;
	private javax.swing.JPanel jPanel4;
	private javax.swing.JPanel jPanel5;
	private javax.swing.JPasswordField jPasswordField1;
	private javax.swing.JSeparator jSeparator1;
	private javax.swing.JTextField jTextField1;
	private javax.swing.JTextField jTextField2;
	private javax.swing.JTextField jTextField3;
	private javax.swing.JTextField jTextField4;
	private javax.swing.JTextField jTextField5;
	private javax.swing.JLabel labelServerInstance;
	private javax.swing.JLabel labelVersion;
	private javax.swing.JLabel lableCatalog;
	private javax.swing.JPanel logopanel;
	private javax.swing.JTextField textCatalogName;
	private javax.swing.JTextField textServerInstance;
	private javax.swing.JTextField textVersion;
	// End of variables declaration//GEN-END:variables
}