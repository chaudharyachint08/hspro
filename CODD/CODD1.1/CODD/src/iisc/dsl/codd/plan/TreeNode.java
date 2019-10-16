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
package iisc.dsl.codd.plan;

import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.ds.Term;
import java.awt.Color;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;
import java.util.Hashtable;
import java.util.ListIterator;

/**
 * TreeNode represents the a node in the plan tree.
 * @author dsladmin
 */
public class TreeNode {
        private int             id; // added for execution, used in the filename
	private int 		depth;
	private int             type;
	private TreeNode 	parent;
	private Vector		children;
	private boolean		showAttr = false;

	private String		nodeName;
	private double		actualCost;  // refers to cpu_cost
	private double		estimatedCost; // refers to cpu_cost
    	private double		cardinality;
	private Hashtable	attributes;
	private boolean isDependent; 		// true for dependent nodes

        // Variables added for Cost Scaling Operations

        private double output; // original output of operator - used in SCM_cost of parent operator
        private Term scaledOutput; // scaeld output of operator - used in SCM_scaledCost of parent operator
        private double operatorCPUCost; // excluding sub operators cost from est.cost
        private double SCM_cost; // Simple Cost Model cost wrt original input to the operator
        private HashMap<String,Term> SCM_scaledCost; // Simple Cost Model cost wrt scaled input to the operator. It has scaling factors.
        private HashMap<String,Term> scaledOperatorCost; // = operatorCPUCost *(SCM_scaledCost / SCM_cost)
        private HashSet<String> subTreeRelations; // contains all the relations in its subtree

	public TreeNode(int d, TreeNode p) {
		parent = p;
		depth = d;
		attributes = new Hashtable();
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

	public String toString() {
            return(nodeName);
	}

	// Get and Set methods..
	public boolean getDependency()
	{
		return isDependent;
	}
	public void setDependency(boolean isDep)
	{
		isDependent = isDep;
	}
	public int getDepth() {
		return depth;
	}

	public void setDepth(int d) {
		depth = d;
	}

	public int getType() {
		return type;
	}

	public void setType(int t) {
		type = t;
	}

	public boolean showAttrs() {
		return showAttr;
	}

	public void setShowAttr(boolean sim) {
		showAttr = sim;
	}

	public Hashtable getAttributes() {
		return attributes;
	}

	public void setAttributes(Hashtable attrib) {
		attributes = attrib;
	}

	public double getCardinality() {
		return cardinality;
	}
        public double getEstimatedCost() {
            return estimatedCost;
        }

        public void setEstimatedCost(double estimatedCost) {
            this.estimatedCost = estimatedCost;
        }

        public boolean isIsDependent() {
            return isDependent;
        }

        public void setIsDependent(boolean isDependent) {
            this.isDependent = isDependent;
        }

	public TreeNode getParent() {
		return parent;
	}

	public void setParent(TreeNode p) {
		parent = p;
	}

	public Vector getChildren() {
		return(children);
	}

	public void setChildren(Vector c) {
		children = c;
	}

	public void setNodeValues(int id, String name, int type, double ac,
			double ec, double c, Vector argType, Vector argValue) {
		this.id = id;
                nodeName = name;
		this.type = type;
		actualCost = ac;
		estimatedCost = ec;
		cardinality = c;
		if(argType == null)
			return;
		ListIterator itt = argType.listIterator();
		ListIterator itv = argValue.listIterator();
		while(itt.hasNext() && itv.hasNext()){
			Object a = itt.next(), b = itv.next();
			if(a !=null && b!=null)
				attributes.put(a, b);
		}
	}

	public String getNodeName() {
		return nodeName;
	}

	public double[] getNodeValues() {
		double[] val = new double[3];

		val[0] = actualCost;
		val[1] = estimatedCost;
		val[2] = cardinality;
		return val;
	}

            public double getSCM_cost() {
        return SCM_cost;
        }

        public void setSCM_cost(double SCM_cost) {
            this.SCM_cost = SCM_cost;
        }

        public HashMap getSCM_scaledCost() {
            return SCM_scaledCost;
        }

        public void setSCM_scaledCost(HashMap SCM_scaledCost) {
            this.SCM_scaledCost = SCM_scaledCost;
        }

        public double getOperatorCPUCost() {
            return operatorCPUCost;
        }

        public void setOperatorCPUCost(double operatorCPUCost) {
            this.operatorCPUCost = operatorCPUCost;
        }

        public double getOutput() {
            return output;
        }

        public void setOutput(double output) {
            this.output = output;
        }

        public HashMap getScaledOperatorCost() {
            return scaledOperatorCost;
        }

        public void setScaledOperatorCost(HashMap scaledOperatorCost) {
            this.scaledOperatorCost = scaledOperatorCost;
        }

        public Term getScaledOutput() {
            return scaledOutput;
        }

        public void setScaledOutput(Term scaledOutput) {
            this.scaledOutput = scaledOutput;
        }

        public HashSet<String> getSubTreeRelations() {
            return subTreeRelations;
        }

        public void setSubTreeRelations(HashSet<String> subTreeRelations) {
            this.subTreeRelations = subTreeRelations;
        }

     public static String getOutputList(String dbVendor, Hashtable attributes) {
        String cols = null;
        if (dbVendor.equals(DBConstants.DB2)) {
            cols = (String) attributes.get("Column Names");
        } else if (dbVendor.equals(DBConstants.ORACLE)) {
            cols = (String) attributes.get("Output List");
        } else if (dbVendor.equals(DBConstants.MSSQL)) {
            cols = (String) attributes.get("Column List");
        } else if (dbVendor.equals(DBConstants.SYBASE)) {
            cols = (String) attributes.get("Output List");
        } else if (dbVendor.equals(DBConstants.POSTGRES)) {
            cols = (String) attributes.get("Output List");
        }
        return cols;
    }

    public static String[] getOutputColumns(String dbVendor, Hashtable attributes) {
        String cols = getOutputList(dbVendor, attributes);
        String[] columns = null;
        if (dbVendor.equals(DBConstants.DB2)) {
            columns = cols.split(",");
        } else if (dbVendor.equals(DBConstants.ORACLE)) {
        } else if (dbVendor.equals(DBConstants.MSSQL)) {
        } else if (dbVendor.equals(DBConstants.SYBASE)) {
        } else if (dbVendor.equals(DBConstants.POSTGRES)) {
        }
        return columns;
    }

    private static String removeAlias(String string) {
        return string.replaceAll("^Q[\\d]+\\.", "").trim();
    }

    public static String[] getPredicates(String dbVendor, Hashtable attributes) {
        String[] predicates = null;
        if (dbVendor.equals(DBConstants.DB2)) {
            String preds = (String) attributes.get("Predicates List");
            if(preds != null) {
                predicates = preds.split(",");
                for(int i=0;i<predicates.length;i++) {
                    // Remove paranthesis, alias name
                    String predicate = predicates[i];
                    predicate = predicate.substring(predicate.indexOf("(")+1);
                    predicate = predicate.substring(0,predicate.indexOf(")"));
                    predicate = predicate.trim();
                    predicate = removeAlias(predicate);
                    predicates[i] = predicate;
                }
            }
        } else if (dbVendor.equals(DBConstants.ORACLE)) {
        } else if (dbVendor.equals(DBConstants.MSSQL)) {
        } else if (dbVendor.equals(DBConstants.SYBASE)) {
        } else if (dbVendor.equals(DBConstants.POSTGRES)) {
        }
        return predicates;
    }



}
