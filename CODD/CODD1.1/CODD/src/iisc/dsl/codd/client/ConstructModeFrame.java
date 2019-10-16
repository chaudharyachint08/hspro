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

import iisc.dsl.codd.graphhistogram.BucketItem;
import java.util.ArrayList;

/**
 * Super class for Engine Specific ConstrutMode Frame windows.
 * @author dsladmin
 */
public abstract class ConstructModeFrame extends javax.swing.JFrame {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 6396108479433820391L;

	public ConstructModeFrame(String title)
	{
		super(title);
	}

	/**
	 * Replaces the histogram values in the GUI, with the specified histogram.
	 * @param buckets List of buckets.
	 * @param noDistinct Use the distinct values in the buckets.
	 */
	abstract public void setHistogram(ArrayList<BucketItem> buckets, boolean noDistinct);
}
