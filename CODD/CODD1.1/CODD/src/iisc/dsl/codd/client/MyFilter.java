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

import java.io.File;

/**
 * CODD implemented class for file filter.
 * @author dsladmin
 */
public class MyFilter extends javax.swing.filechooser.FileFilter{

    String filter;  // Contains '.type'
    MyFilter(String filter)
    {
        this.filter = filter;
    }

    @Override
    public boolean accept(File f) {
        String filename = f.getName();
        return filename.endsWith(filter);
    }

    @Override
    public String getDescription() {
        return "*"+filter;
    }
}