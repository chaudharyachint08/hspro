/*
#
# COPYRIGHT INFORMATION
#
# Copyright (C) 2012 Indian Institute of Science
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
# Telephone: (+91) 80 2293-2793
# Fax : (+91) 80 2360-2648
# Email: haritsa@dsl.serc.iisc.ernet.in
# WWW: http://dsl.serc.iisc.ernet.in/˜haritsa
#
 */

package iisc.dsl.codd.db.oracle;

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.Database;
import java.math.BigDecimal;
import java.math.BigInteger;
import javax.swing.JOptionPane;

import iisc.dsl.codd.client.ConnectDBFrame;
import iisc.dsl.codd.client.GetRelationFrame;
import iisc.dsl.codd.client.gui.BaseJPanel;
import javax.swing.GroupLayout.Alignment;
import javax.swing.GroupLayout;
import javax.swing.LayoutStyle.ComponentPlacement;
import javax.swing.SwingConstants;

import java.awt.Component;
import java.awt.Rectangle;
import java.awt.Font;

import javax.swing.JTextField;
import javax.swing.JPanel;
import javax.swing.JLabel;

public class OracleHardware extends javax.swing.JFrame {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = -7442910125477407426L;
	Database database;

	/**
	 * Constructor For ConnectDBFrame.
	 * @wbp.parser.constructor 
	 */
	
	public OracleHardware(Database database) throws DatalessException {
		this.database = database;
		initComponents();
		BaseJPanel logoPanel = new BaseJPanel("img/codd_logo_small.png");
		logoPanel.setSize(100, 45);
		getContentPane().setBackground(new java.awt.Color(255, 255, 204));
		setLocationRelativeTo(null);
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
	public OracleHardware(Database database, OracleSystemStatistics sysStat) {
		initComponents();
		getContentPane().setBackground(new java.awt.Color(255, 255, 204));
		//jLabel12.setText("Copyright \u00a9 Indian Institute Of Science, Bangalore, India.");
		setLocation(300, 150);

		setValues(sysStat);
	} 




	/**
	 * Sets the form fields with connection parameters.
	 */
	/*private void setValues() {
		cpuSpeedNWTF.setText(dbServerName);
		ioSeekTimTF.setText(dbServerPort);
		cpuSpeedTF.setText(dbName);
		mReadTimTF.setText(dbSchema);
		sReadTimTF.setText(dbCatalog);
		maxThrTF.setText(dbUserName);
		ioTFRSpeedTF.setText(serverInstance);
		mbrcTF.setText(dbVersion);
	}*/

	/**
	 * Sets the form fields with connection parameters.
	 */
	private void setValues(OracleSystemStatistics sysStat) {
		cpuSpeedNWTF.setText(sysStat.getCpuSpeedNW().toPlainString());
		ioSeekTimTF.setText(sysStat.getIoSeekTim().toPlainString());
		cpuSpeedTF.setText(sysStat.getCpuSpeed().toPlainString());
		mReadTimTF.setText(sysStat.getmReadTim().toPlainString());
		sReadTimTF.setText(sysStat.getsReadTim().toPlainString());
		maxThrTF.setText(sysStat.getMaxThr().toPlainString());
		ioTFRSpeedTF.setText(sysStat.getIoTFRSpeed().toPlainString());
		mbrcTF.setText(sysStat.getMbrc().toPlainString());
		parallelMaxServersTF.setText(sysStat.getParallelMaxServers().toString());
	}

	private boolean validateCurrentSystemStats(OracleSystemStatistics sysStat)
	{
		boolean valid = false;
		BigDecimal cpuspeedNW = null, ioseektim = null, iotfrspeed = null, cpuspeed = null, maxthr = null, slavethr = null, sreadtim = null, mreadtim = null, mbrc = null;
		BigInteger parallelmaxservers = null;
		
		/**
		 * Node : Card (1)
		 * Structural Constraints: >=0
		 * Consistency Constraints: NILL
		 */
		if(this.cpuSpeedNWTF.getText() != null && !(this.cpuSpeedNWTF.getText().trim().isEmpty())){
			try {
				cpuspeedNW = new BigDecimal(this.cpuSpeedNWTF.getText());
				if(cpuspeedNW.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in CPUSPEEDNW cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}		
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in CPUSPEEDNW is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.ioSeekTimTF.getText() != null && !(this.ioSeekTimTF.getText().trim().isEmpty())){
			try {
				ioseektim = new BigDecimal(this.ioSeekTimTF.getText());
				if(ioseektim.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in IOSEEKTIM cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}	
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in IOSEEKTIM is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.ioTFRSpeedTF.getText() != null && !(this.ioTFRSpeedTF.getText().trim().isEmpty())){
			try {
				iotfrspeed = new BigDecimal(this.ioTFRSpeedTF.getText());
				if(iotfrspeed.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in IOTFRSPEED cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}	
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in IOTFRSPEED is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.cpuSpeedTF.getText() != null && !(this.cpuSpeedTF.getText().trim().isEmpty())){
			try {
				cpuspeed = new BigDecimal(this.cpuSpeedTF.getText());
				if(cpuspeed.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in CPUSPEED cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in CPUSPEED is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.sReadTimTF.getText() != null && !(this.sReadTimTF.getText().trim().isEmpty())){
			try {
				sreadtim = new BigDecimal(this.sReadTimTF.getText());
				if(sreadtim.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in SREADTIM cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in SREADTIM is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.mReadTimTF.getText() != null && !(this.mReadTimTF.getText().trim().isEmpty())){
			try {
				mreadtim = new BigDecimal(this.mReadTimTF.getText());
				if(mreadtim.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MREADTIM cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MREADTIM is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.mbrcTF.getText() != null && !(this.mbrcTF.getText().trim().isEmpty())){
			try {
				mbrc = new BigDecimal(this.mbrcTF.getText());
				if(mbrc.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MBRC cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MBRC is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.maxThrTF.getText() != null && !(this.maxThrTF.getText().trim().isEmpty())){
			try {
				maxthr = new BigDecimal(this.maxThrTF.getText());
				if(maxthr.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MAXTHR cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MAXTHR is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.slaveThrTF.getText() != null && !(this.slaveThrTF.getText().trim().isEmpty())){
			try {
				slavethr = new BigDecimal(this.slaveThrTF.getText());
				if(slavethr.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in SLAVETHR cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in SLAVETHR is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
		if(this.parallelMaxServersTF.getText() != null && !(this.parallelMaxServersTF.getText().trim().isEmpty())){
			try {
				parallelmaxservers = new BigInteger(this.parallelMaxServersTF.getText());
				if(parallelmaxservers.signum() < 0) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in PARALLEL_MAX_SERVERS cannot be negative.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
				if(parallelmaxservers.compareTo(new BigInteger("3599")) == 1) {
					valid = false;
					JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in PARALLEL_MAX_SERVERS cannot be more than 3599.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return valid;
				}
			} catch (NumberFormatException e) {
			valid = false;
			JOptionPane.showMessageDialog(null, "Validation Error - Structural Constraint: Value present in MAX_PARALLEL_SERVERS is not of Valid type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			return valid;
			}
		}
  
		// valid must be true
		valid = true;
		
		sysStat.setCpuSpeedNW(cpuspeedNW);
		sysStat.setIoSeekTim(ioseektim);
		sysStat.setIoTFRSpeed(iotfrspeed);
		sysStat.setCpuSpeed(cpuspeed);
		sysStat.setsReadTim(sreadtim);
		sysStat.setmReadTim(mreadtim);
		sysStat.setMbrc(mbrc);
		sysStat.setMaxThr(maxthr);
		sysStat.setSlaveThr(slavethr);
		sysStat.setParallelMaxServers(parallelmaxservers);
		sysStat.setSkipped(false);

		return valid;
	}
	
   // @Override
    public boolean setSystemStatistics() throws DatalessException {
        //boolean sameDBType = false;
    	OracleSystemStatistics sysStat =  OracleSystemStatistics.getInstance();
    	if(validateCurrentSystemStats(sysStat))
    		return true;
    	
    	return false;
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
		jPanel3 = new javax.swing.JPanel();
		cpuSpeed = new javax.swing.JLabel();
		mReadTim = new javax.swing.JLabel();
		sReadTim = new javax.swing.JLabel();
		mbrc = new javax.swing.JLabel();
		mbrcTF = new javax.swing.JTextField();
		mbrcTF.setToolTipText("Average multiblock read count sequentially");
		sReadTimTF = new javax.swing.JTextField();
		sReadTimTF.setToolTipText("Average time to read a single block randomly");
		mReadTimTF = new javax.swing.JTextField();
		cpuSpeedTF = new javax.swing.JTextField();
		jPanel1 = new javax.swing.JPanel();
		cpuSpeedNW = new javax.swing.JLabel();
		cpuSpeedNWTF = new javax.swing.JTextField();
		ioSeekTim = new javax.swing.JLabel();
		ioTFRSpeed = new javax.swing.JLabel();
		ioTFRSpeedTF = new javax.swing.JTextField();
		ioSeekTimTF = new javax.swing.JTextField();
		update = new javax.swing.JButton();

		jSeparator1 = new javax.swing.JSeparator();
		jLabel1 = new javax.swing.JLabel();
		skip = new javax.swing.JButton();
		back = new javax.swing.JButton();
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
		setTitle("Oracle System Settings");
		setBounds(new Rectangle(150, 25, 0, 0));
		setResizable(false);

		jPanel3.setBorder(javax.swing.BorderFactory.createTitledBorder("Workload Statistics"));
		jPanel3.setOpaque(false);

		cpuSpeed.setFont(new java.awt.Font("Verdana", 0, 14));
		cpuSpeed.setText("CPUSPEED (Millions/sec)");

		mReadTim.setFont(new java.awt.Font("Verdana", 0, 14));
		mReadTim.setText("MREADTIM (ms)");

		sReadTim.setFont(new java.awt.Font("Verdana", 0, 14));
		sReadTim.setText("SREADTIM (ms)");

		mbrc.setFont(new java.awt.Font("Verdana", 0, 14));
		mbrc.setText("MBRC");

		mbrcTF.setFont(new java.awt.Font("Cambria", 0, 14));

		sReadTimTF.setFont(new java.awt.Font("Cambria", 0, 14));

		mReadTimTF.setFont(new java.awt.Font("Cambria", 0, 14));
		mReadTimTF.setToolTipText("Average time to read a multiblock sequentially");

		cpuSpeedTF.setFont(new java.awt.Font("Cambria", 0, 14));
		cpuSpeedTF.setToolTipText("Average number of CPU cycles in each second");
		maxThr = new javax.swing.JLabel();
		
				maxThr.setFont(new java.awt.Font("Verdana", 0, 14));
				maxThr.setText("MAXTHR (Bytes/sec)");
		maxThrTF = new javax.swing.JTextField();
		maxThrTF.setFont(new Font("Cambria", Font.PLAIN, 14));
		
				maxThrTF.setToolTipText("Maximum I/O throughput");
				maxThrTF.addActionListener(new java.awt.event.ActionListener() {
					public void actionPerformed(java.awt.event.ActionEvent evt) {
						jTextField5ActionPerformed(evt);
					}
				});
		slaveThr = new javax.swing.JLabel();
		
				slaveThr.setFont(new java.awt.Font("Tahoma", 0, 14));
				slaveThr.setText("SLAVETHR (Bytes/sec)");
		
		slaveThrTF = new JTextField();
		slaveThrTF.setFont(new Font("Cambria", Font.PLAIN, 14));
		slaveThrTF.setColumns(10);

		javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
		jPanel3Layout.setHorizontalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.TRAILING)
						.addGroup(jPanel3Layout.createSequentialGroup()
							.addComponent(cpuSpeed)
							.addGap(18)
							.addComponent(cpuSpeedTF, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE))
						.addGroup(jPanel3Layout.createSequentialGroup()
							.addComponent(sReadTim)
							.addGap(18)
							.addComponent(sReadTimTF, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE))
						.addGroup(jPanel3Layout.createSequentialGroup()
							.addComponent(mReadTim)
							.addGap(18)
							.addComponent(mReadTimTF, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE))
						.addGroup(Alignment.LEADING, jPanel3Layout.createSequentialGroup()
							.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(mbrc, GroupLayout.PREFERRED_SIZE, 82, GroupLayout.PREFERRED_SIZE)
								.addComponent(slaveThr)
								.addComponent(maxThr))
							.addGap(18)
							.addGroup(jPanel3Layout.createParallelGroup(Alignment.LEADING)
								.addComponent(maxThrTF, Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE)
								.addComponent(mbrcTF, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE)
								.addComponent(slaveThrTF, Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 133, Short.MAX_VALUE))))
					.addContainerGap())
		);
		jPanel3Layout.setVerticalGroup(
			jPanel3Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel3Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(cpuSpeedTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(cpuSpeed))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(sReadTimTF, GroupLayout.PREFERRED_SIZE, 24, GroupLayout.PREFERRED_SIZE)
						.addComponent(sReadTim))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(mReadTimTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(mReadTim))
					.addPreferredGap(ComponentPlacement.RELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(mbrcTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(mbrc, GroupLayout.PREFERRED_SIZE, 23, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.RELATED, 10, Short.MAX_VALUE)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(maxThr, GroupLayout.PREFERRED_SIZE, 20, GroupLayout.PREFERRED_SIZE)
						.addComponent(maxThrTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel3Layout.createParallelGroup(Alignment.TRAILING)
						.addComponent(slaveThr, GroupLayout.PREFERRED_SIZE, 20, GroupLayout.PREFERRED_SIZE)
						.addComponent(slaveThrTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)))
		);
		jPanel3Layout.linkSize(SwingConstants.VERTICAL, new Component[] {mbrcTF, sReadTimTF, mReadTimTF, cpuSpeedTF});
		jPanel3Layout.linkSize(SwingConstants.VERTICAL, new Component[] {cpuSpeed, mReadTim, sReadTim, mbrc});
		jPanel3Layout.linkSize(SwingConstants.HORIZONTAL, new Component[] {cpuSpeed, mReadTim, sReadTim, mbrc});
		jPanel3.setLayout(jPanel3Layout);

		jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder("Noworkload Statistics"));
		jPanel1.setOpaque(false);

		cpuSpeedNW.setFont(new java.awt.Font("Verdana", 0, 14));
		cpuSpeedNW.setText("CPUSPEEDNW (Millions/sec)");

		cpuSpeedNWTF.setFont(new java.awt.Font("Cambria", 0, 14));
		cpuSpeedNWTF.setToolTipText("Average number of CPU cycles in each second");
		cpuSpeedNWTF.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jTextField1ActionPerformed(evt);
			}
		});

		ioSeekTim.setFont(new java.awt.Font("Verdana", 0, 14));
		ioSeekTim.setText("IOSEEKTIM (ms)");

		ioTFRSpeed.setFont(new java.awt.Font("Verdana", 0, 14));
		ioTFRSpeed.setText("IOTFRSPEED (Bytes/ms)");

		ioTFRSpeedTF.setFont(new java.awt.Font("Cambria", 0, 14));
		ioTFRSpeedTF.setToolTipText("I/O transfer speed");

		ioSeekTimTF.setFont(new java.awt.Font("Cambria", 0, 14));
		ioSeekTimTF.setToolTipText("I/O seek time");
		ioSeekTimTF.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jTextField2ActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1Layout.setHorizontalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(ioSeekTim)
						.addComponent(cpuSpeedNW)
						.addComponent(ioTFRSpeed))
					.addGap(17)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.LEADING)
						.addComponent(ioTFRSpeedTF, GroupLayout.DEFAULT_SIZE, 203, Short.MAX_VALUE)
						.addComponent(cpuSpeedNWTF, Alignment.TRAILING, GroupLayout.DEFAULT_SIZE, 203, Short.MAX_VALUE)
						.addComponent(ioSeekTimTF, GroupLayout.DEFAULT_SIZE, 203, Short.MAX_VALUE))
					.addContainerGap())
		);
		jPanel1Layout.setVerticalGroup(
			jPanel1Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(cpuSpeedNWTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(cpuSpeedNW))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(ioSeekTimTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(ioSeekTim))
					.addPreferredGap(ComponentPlacement.UNRELATED)
					.addGroup(jPanel1Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(ioTFRSpeedTF, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(ioTFRSpeed))
					.addContainerGap(53, Short.MAX_VALUE))
		);
		jPanel1.setLayout(jPanel1Layout);

		update.setText("Update");
		update.setToolTipText("Update system parameter settings");
		update.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
					updateActionPerformed(evt);
			}
		});

		jLabel1.setFont(new java.awt.Font("Verdana", 1, 12));

		skip.setText("Skip >>");
		skip.setToolTipText("Skip system parameter settings");
		skip.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton1ActionPerformed(evt);
			}
		});

		back.setText("<< Back");
		back.setToolTipText("Back to previous window");
		back.addActionListener(new java.awt.event.ActionListener() {
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
		
		panel = new JPanel();

		javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
		jPanel5Layout.setHorizontalGroup(
			jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
					.addContainerGap()
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel5Layout.createSequentialGroup()
							.addComponent(jLabel1)
							.addPreferredGap(ComponentPlacement.RELATED, 501, Short.MAX_VALUE)
							.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel5Layout.createSequentialGroup()
							.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING, false)
								.addGroup(jPanel5Layout.createSequentialGroup()
									.addGap(240)
									.addComponent(back)
									.addGap(18)
									.addComponent(update)
									.addGap(18)
									.addComponent(skip, GroupLayout.PREFERRED_SIZE, 124, GroupLayout.PREFERRED_SIZE)
									.addGap(73)
									.addComponent(logopanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
								.addGroup(jPanel5Layout.createSequentialGroup()
									.addGap(32)
									.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
										.addGroup(jPanel5Layout.createSequentialGroup()
											.addComponent(jPanel1, GroupLayout.PREFERRED_SIZE, 382, GroupLayout.PREFERRED_SIZE)
											.addPreferredGap(ComponentPlacement.RELATED, 54, Short.MAX_VALUE)
											.addComponent(jPanel3, GroupLayout.PREFERRED_SIZE, 353, GroupLayout.PREFERRED_SIZE))
										.addGroup(jPanel5Layout.createSequentialGroup()
											.addComponent(panel, GroupLayout.PREFERRED_SIZE, 370, GroupLayout.PREFERRED_SIZE)
											.addPreferredGap(ComponentPlacement.RELATED, 402, Short.MAX_VALUE)))))
							.addGap(75)))
					.addGap(0))
		);
		jPanel5Layout.setVerticalGroup(
			jPanel5Layout.createParallelGroup(Alignment.LEADING)
				.addGroup(jPanel5Layout.createSequentialGroup()
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.LEADING)
						.addGroup(jPanel5Layout.createSequentialGroup()
							.addGap(33)
							.addComponent(jSeparator1, GroupLayout.PREFERRED_SIZE, 11, GroupLayout.PREFERRED_SIZE))
						.addGroup(jPanel5Layout.createSequentialGroup()
							.addContainerGap()
							.addComponent(jLabel1, GroupLayout.DEFAULT_SIZE, 33, Short.MAX_VALUE)))
					.addGap(18)
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.BASELINE)
						.addComponent(jPanel1, GroupLayout.DEFAULT_SIZE, 231, Short.MAX_VALUE)
						.addComponent(jPanel3, GroupLayout.PREFERRED_SIZE, 231, GroupLayout.PREFERRED_SIZE))
					.addGap(29)
					.addComponent(panel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
					.addGap(41)
					.addGroup(jPanel5Layout.createParallelGroup(Alignment.CENTER)
						.addComponent(skip, GroupLayout.PREFERRED_SIZE, 39, GroupLayout.PREFERRED_SIZE)
						.addComponent(logopanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
						.addComponent(update, GroupLayout.PREFERRED_SIZE, 29, GroupLayout.PREFERRED_SIZE)
						.addComponent(back))
					.addGap(23))
		);
		jPanel5Layout.linkSize(SwingConstants.VERTICAL, new Component[] {update, skip, back});
		jPanel5Layout.linkSize(SwingConstants.HORIZONTAL, new Component[] {update, skip, back});
		
		parallelMaxServers = new JLabel("PARALLEL_MAX_SERVERS");
		parallelMaxServers.setFont(new Font("Verdana", Font.PLAIN, 14));
		panel.add(parallelMaxServers);
		
		parallelMaxServersTF = new JTextField();
		parallelMaxServersTF.setFont(new Font("Cambria", Font.PLAIN, 14));
		parallelMaxServersTF.setColumns(12);
		panel.add(parallelMaxServersTF);
		jPanel5.setLayout(jPanel5Layout);

		javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
		layout.setHorizontalGroup(
			layout.createParallelGroup(Alignment.LEADING)
				.addComponent(jPanel5, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
		);
		layout.setVerticalGroup(
			layout.createParallelGroup(Alignment.LEADING)
				.addComponent(jPanel5, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
		);
		getContentPane().setLayout(layout);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void updateActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
		
		try {
			if(setSystemStatistics()){
				new GetRelationFrame(database).setVisible(true);
				this.dispose();	
			}
		} catch (DatalessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
		try {
			new GetRelationFrame(database).setVisible(true);
		} catch (DatalessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.dispose();	

	}//GEN-LAST:event_jButton1ActionPerformed

	private void jButton5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton5ActionPerformed
		String dbServerName, dbType, dbServerPort, dbName, dbCatalog, dbSchema, dbUserName, dbPassword, serverInstance, dbVersion;
		try{
			//if(database.getSettings().getDbVendor().equals(DBConstants.DB2)){
			//	new ConfigurationTypeSelection(database).setVisible(true);
			//}else{
				dbServerName = database.getSettings().getServerName();
				dbType = database.getSettings().getDbVendor();
				dbServerPort = database.getSettings().getServerPort();
				dbName = database.getSettings().getDbName();
				dbCatalog = database.getSettings().getCatalog();
				dbSchema = database.getSettings().getSchema();
				dbUserName = database.getSettings().getUserName();
				dbPassword = database.getSettings().getPassword();
				serverInstance = database.getSettings().getSqlServerInstanceName();
				dbVersion = database.getSettings().getVersion();
				database.close();        		
				new ConnectDBFrame(dbServerName, dbType, dbServerPort, dbName, dbCatalog, dbSchema, dbUserName, dbPassword, serverInstance, dbVersion).setVisible(true);
			//}
		}catch(Exception e){
			JOptionPane.showMessageDialog(this, "Exception Thrown :" + e.getMessage(), "Fatal Error", JOptionPane.ERROR_MESSAGE);
		} finally {
			this.dispose();
		}
	}//GEN-LAST:event_jButton5ActionPerformed

	private void jTextField1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField1ActionPerformed
	}//GEN-LAST:event_jTextField1ActionPerformed

	private void jTextField2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField2ActionPerformed
	}//GEN-LAST:event_jTextField2ActionPerformed

	private void jTextField5ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jTextField5ActionPerformed
	}//GEN-LAST:event_jTextField5ActionPerformed


	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.ButtonGroup buttonGroup1;
	private javax.swing.ButtonGroup buttonGroup2;
	private javax.swing.JButton skip;
	private javax.swing.JButton update;
	private javax.swing.JButton back;
	private javax.swing.JDialog jDialog1;
	private javax.swing.JLabel jLabel1;
	private javax.swing.JLabel cpuSpeedNW;
	private javax.swing.JLabel ioSeekTim;
	private javax.swing.JLabel cpuSpeed;
	private javax.swing.JLabel mReadTim;
	private javax.swing.JLabel maxThr;
	private javax.swing.JLabel slaveThr;
	private javax.swing.JLabel jLabel9;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JPanel jPanel3;
	private javax.swing.JPanel jPanel5;
	private javax.swing.JSeparator jSeparator1;
	private javax.swing.JTextField cpuSpeedNWTF;
	private javax.swing.JTextField ioSeekTimTF;
	private javax.swing.JTextField cpuSpeedTF;
	private javax.swing.JTextField mReadTimTF;
	private javax.swing.JTextField maxThrTF;
	private javax.swing.JLabel ioTFRSpeed;
	private javax.swing.JLabel mbrc;
	private javax.swing.JLabel sReadTim;
	private javax.swing.JPanel logopanel;
	private javax.swing.JTextField sReadTimTF;
	private javax.swing.JTextField ioTFRSpeedTF;
	private javax.swing.JTextField mbrcTF;
	private JTextField slaveThrTF;
	private JPanel panel;
	private JLabel parallelMaxServers;
	private JTextField parallelMaxServersTF;
}

