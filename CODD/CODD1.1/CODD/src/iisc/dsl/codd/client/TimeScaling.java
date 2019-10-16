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

import iisc.dsl.codd.DatalessException;
import iisc.dsl.codd.db.DBConstants;
import iisc.dsl.codd.db.Database;
import iisc.dsl.codd.ds.Constants;
import iisc.dsl.codd.ds.Term;
import iisc.dsl.codd.plan.Plan;
import iisc.dsl.codd.plan.TreeNode;
import java.rmi.RemoteException;
import javax.swing.JOptionPane;
import javax.swing.DefaultListModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import iisc.dsl.codd.client.gui.BaseJPanel;

/**
 * Implementation of Cost Scaling.
 * The frame asks the user for the required input (Queries, Probability Distribution), computes the cost function for query plan,
 * formulates the optimization problem, solves the problem using SuanShu library and scales the relation with the obtained solution.
 *
 * We use only the CPU cost in our modeling. Since we don't have CPU cost information from PostgreSQL, we use total operator cost.
 * @author dsladmin
 */
public class TimeScaling extends javax.swing.JFrame {

	/**
	 * Generated serialVersionUID
	 */
	private static final long serialVersionUID = 8381673480306173042L;
	String[] selectedRelation;
	DefaultListModel model1 = new DefaultListModel();
	Database database;
	// Stores the scale factore variable for each relation
	HashMap<String, String> scaleFactors;
	/**
	 * Stores the graph of dependency. Each relation has a list of all FK relations.
	 */
	HashMap<String, ArrayList<String>> dependencyGraph;
	/**
	 * Dimension
	 */
	public int dimension;
	/**
	 * Plan Cost Functions for the input queries.
	 * Plan Cost function is represented as a set of terms. The terms are summed up to get the cost function.
	 * Our representation keeps the terms in a map, where the key uniquely identifies a term.
	 */
	public HashMap<String, Term>[] planCostFunctions;
	/**
	 * Estimated Plan Cost for the input queries on the baseline metadata.
	 */
	public double[] originalPlanCost; //total cpu cost
	/**
	 * Scale Factor
	 */
	public int scaleFactor;
	/**
	 * Input : Query Probability
	 */
	public double[] prob;
	/**
	 * Lemma2 Conditions. <relation, Term[left,right]>
	 * Key identifies the relation on which the Lemma 2 constraint is posed.
	 */
	public HashMap<String, Term[]> lemma2Constraints;
	/**
	 * true for Srujana's Output model, False for Talha's Model (Lemma 2)
	 * true - Output of an operator is a function of only FK relation's scale factor in the subtree
	 * false - Output of an operator is a function all relation's scale factor in the subtree
	 */
	public static boolean scaledOutputOnFKRels = true;

	/** Creates new form CostScaling
	 * @param relations
	 * @param sf
	 * @param database
	 */
	public TimeScaling(String[] relations, String sf, Database database) {
		Constants.CPrintToConsole("Time Scaling.", Constants.DEBUG_FIRST_LEVEL_Information);
		this.database = database;
		selectedRelation = relations;
		initComponents();
		setLocationRelativeTo(null);
		setTitle("Metadata Time Scaling");
		getContentPane().setBackground(new java.awt.Color(255, 255, 204));
		jLabel8.setText(sf);
		scaleFactor = Integer.parseInt(sf);
		this.dimension = selectedRelation.length;
		scaleFactors = new HashMap();
		Constants.CPrintToConsole("Relation:UnknownVecotIndex Map", Constants.DEBUG_FIRST_LEVEL_Information);
		for (int i = 0; i < selectedRelation.length; i++) {
			String relation = selectedRelation[i].toLowerCase();
			String scaleFactor = "sf_" + relation + "_sf";
			//String scaleFactor = "x["+i+"]";
			Constants.CPrintToConsole(relation + ":" + scaleFactor + " ----- " + i, Constants.DEBUG_FIRST_LEVEL_Information);
			scaleFactors.put(relation, scaleFactor);
		}
		// Initialize the dependecy Graph
		dependencyGraph = new HashMap();
		for (int i = 0; i < selectedRelation.length; i++) {
			String relation = selectedRelation[i];
			ArrayList<String> depRels = null;
			try {
				ArrayList<String> dep = database.getDependentRelations(relation);
				depRels = new ArrayList();
				for (int k = 0; k < dep.size(); k++) {
					String rel = dep.get(k);
					depRels.add(rel.toLowerCase());
				}
			} catch (DatalessException ex) {
				Constants.CPrintErrToConsole(ex);
				JOptionPane.showMessageDialog(null, "Excpetion in initializing dependency graph.", "CODD - Exception", JOptionPane.WARNING_MESSAGE);
			}
			dependencyGraph.put(relation.toLowerCase(), depRels);
		}

		// Lemma 2 bound
		lemma2Constraints = new HashMap();
		for (int i = 0; i < selectedRelation.length; i++) {
			String relation = selectedRelation[i];
			try {
				String[] primaryKeys = database.getPrimaryKeyAttributes(relation);
				String[] foreignKeys = database.getForeignKeyAttributes(relation);

				if (foreignKeys != null) {
					ArrayList<String> fk = new ArrayList();
					for (int f = 0; f < foreignKeys.length; f++) {
						if (!foreignKeys[f].isEmpty()) {
							fk.add(foreignKeys[f]);
						}
					}

					// Is my PK's are a combination of FK's
					int PKCount = 0, PKFKCount = 0;
					for (int f = 0; primaryKeys != null && f < primaryKeys.length; f++) {
						if (!primaryKeys[f].isEmpty()) {
							PKCount++;
							if (fk.contains(primaryKeys[f])) {
								PKFKCount++;
							}
						}
					}

					boolean PKsAreFKs;
					if (PKCount > 0 && PKCount == PKFKCount) {
						PKsAreFKs = true;
					} else {
						PKsAreFKs = false;
					}
					if (PKsAreFKs) {
						// relation's PrimaryKeys are a combination of Foreign Keys. Lemma 2 constraint is applicable for this relation.
						// <FKColumnName, RefPKRelationName>
						Term left = new Term(scaleFactors.get(relation.toLowerCase()));
								Term right = new Term(new String());
								String PKRels = new String();
								boolean atlestOnePKRelIsSelected = false;
								TreeMap<String, String> FKColRefRel = (TreeMap) database.getFKColumnRefRelation(relation);
								for (int p = 0; primaryKeys != null && p < primaryKeys.length; p++) {
									String column = primaryKeys[p];
									// Is column a foreign key, then add it coressponding PKRel
									if (FKColRefRel.containsKey(column)) {
										String PKRel = FKColRefRel.get(column);
										if (scaleFactors.containsKey(PKRel.toLowerCase())) {
											atlestOnePKRelIsSelected = true;
											PKRels = PKRels + " " + PKRel;
											Term rightPart = new Term(scaleFactors.get(PKRel.toLowerCase()));
											right.include(rightPart);
										}
									}
								}
								if (atlestOnePKRelIsSelected) {
									// Atleast one of the FK relation must be chosen to apply the constraint
									Constants.CPrintToConsole(" Lemma 2 Bound:  " + relation + " <= " + PKRels, Constants.DEBUG_FIRST_LEVEL_Information);
									Constants.CPrintToConsole(" Lemma 2 Bound:  " + left + " <= " + right, Constants.DEBUG_FIRST_LEVEL_Information);
									Term[] lemma2 = new Term[2];
									lemma2[0] = left;
									lemma2[1] = right;
									lemma2Constraints.put(relation, lemma2);
								}
					}
				} // end-if (foreignKeys != null)
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, "Excpetion in finding primary, foreign keys of realtions.", "CODD - Exception", JOptionPane.WARNING_MESSAGE);
			}

		}
		/*
		 * Used for testing timeScaling.
        //String queries = "Select * from TEST1, TEST2 WHERE COL1=COL3 AND COL2 <=10 ORDER BY COL4; Select * from TEST1 WHERE COL2 <= 10";
        String queries = "select "
        + " l_extendedprice * (1 - l_discount) "
        + "from "
        + "DSLADMIN.LINEITEM,"
        + " DSLADMIN.PART1 "
        + "where"
        + "	l_partkey = p_partkey"
        + "	and l_shipdate <= date('1992-03-19'); select * from DSLADMIN.LINEITEM";
        // Q12, Q13
        //String queries = "select * from lineitem;select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate group by l_shipmode order by l_shipmode;select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";
        this.jTextAreaQueries.setText(queries);

        // select * from lineitem;
        String queries = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate group by l_shipmode order by l_shipmode;select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";
        this.jTextAreaQueries.setText(queries);
		 */


		//For testing
		//String queries = "select * from lineitem;select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate group by l_shipmode order by l_shipmode;select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";
		//String queries = "select l_shipmode, sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_commitdate < l_receiptdate and l_shipdate < l_commitdate group by l_shipmode order by l_shipmode;select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";
		//String queries = "select c_count, count(*) as custdist from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc, c_count desc;";

		// Testing with modifying Q13
		String queries = "select c_count from ( select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%:1%:2%' group by c_custkey ) as c_orders (c_custkey, c_count) order by  c_count desc;";  // Outer group by removed from q13


		this.jTextAreaQueries.setText(queries);

		// Remove Cost Scaling, if lib not found
		try {
			// Look for SuanShu library presense.
			Class suanShu = Class.forName("com.numericalmethod.suanshu.misc.license.License");
		} catch (ClassNotFoundException e) {
			JOptionPane.showMessageDialog(null, "SuanShu Optimization library not found in the CLASSPATH Environment.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
			jButton1.setEnabled(false);
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

		jPanel1 = new BaseJPanel("img/bg_net.png");
		jScrollPane1 = new javax.swing.JScrollPane();
		jTextAreaQueries = new javax.swing.JTextArea();
		jLabel2 = new javax.swing.JLabel();
		jLabel4 = new javax.swing.JLabel();
		jScrollPane2 = new javax.swing.JScrollPane();
		jTextAreaProbabilities = new javax.swing.JTextArea();
		jLabel7 = new javax.swing.JLabel();
		jButton4 = new javax.swing.JButton();
		jLabel8 = new javax.swing.JLabel();
		jButton1 = new javax.swing.JButton();

		setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
		setTitle("Metadata Time Scaling");
		setResizable(false);

		jTextAreaQueries.setColumns(20);
		jTextAreaQueries.setFont(new java.awt.Font("Courier New", 0, 14)); // NOI18N
		jTextAreaQueries.setRows(5);
		jScrollPane1.setViewportView(jTextAreaQueries);

		jLabel2.setFont(new java.awt.Font("Cambria", 0, 14)); // NOI18N
		jLabel2.setText("Enter the query workload invovling the selected relations ( Queries must be seperated by semi-colon ';' ) : ");

		jLabel4.setFont(new java.awt.Font("Cambria", 0, 14)); // NOI18N
		jLabel4.setText("Enter the query workload probabilities ( seperated by ';' ) :");

		jTextAreaProbabilities.setColumns(20);
		jTextAreaProbabilities.setFont(new java.awt.Font("Courier New", 0, 14)); // NOI18N
		jTextAreaProbabilities.setRows(5);
		jScrollPane2.setViewportView(jTextAreaProbabilities);

		jLabel7.setFont(new java.awt.Font("Cambria", 1, 14)); // NOI18N
		jLabel7.setText("Scale Factor:");

		jButton4.setText("<< Back");
		jButton4.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton4ActionPerformed(evt);
			}
		});

		jLabel8.setFont(new java.awt.Font("Cambria", 1, 14)); // NOI18N
		jLabel8.setText("0.0");

		jButton1.setText("Scale");
		jButton1.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(java.awt.event.ActionEvent evt) {
				jButton1ActionPerformed(evt);
			}
		});

		javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
		jPanel1.setLayout(jPanel1Layout);
		jPanel1Layout.setHorizontalGroup(
				jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
						.addContainerGap()
						.addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
								.addComponent(jScrollPane2)
								.addComponent(jScrollPane1)
								.addGroup(jPanel1Layout.createSequentialGroup()
										.addComponent(jButton4, javax.swing.GroupLayout.PREFERRED_SIZE, 120, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(jButton1))
										.addGroup(jPanel1Layout.createSequentialGroup()
												.addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
														.addComponent(jLabel2)
														.addComponent(jLabel4)
														.addGroup(jPanel1Layout.createSequentialGroup()
																.addComponent(jLabel7)
																.addGap(18, 18, 18)
																.addComponent(jLabel8)))
																.addGap(0, 140, Short.MAX_VALUE)))
																.addContainerGap())
				);

		jPanel1Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {jButton1, jButton4});

		jPanel1Layout.setVerticalGroup(
				jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
				.addGroup(jPanel1Layout.createSequentialGroup()
						.addContainerGap()
						.addComponent(jLabel2)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
						.addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 164, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addGap(18, 18, 18)
						.addComponent(jLabel4)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
						.addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
						.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
						.addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
								.addComponent(jLabel7)
								.addComponent(jLabel8, javax.swing.GroupLayout.PREFERRED_SIZE, 14, javax.swing.GroupLayout.PREFERRED_SIZE))
								.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
										.addComponent(jButton4, javax.swing.GroupLayout.PREFERRED_SIZE, 40, javax.swing.GroupLayout.PREFERRED_SIZE)
										.addComponent(jButton1))
										.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
				);

		jPanel1Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] {jButton1, jButton4});

		getContentPane().add(jPanel1, java.awt.BorderLayout.CENTER);

		pack();
	}// </editor-fold>//GEN-END:initComponents

	private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
		String allQueries = this.jTextAreaQueries.getText();
		String[] queries = allQueries.split(";");

		prob = new double[queries.length];
		String allProbabilities = this.jTextAreaProbabilities.getText();
		if (!allProbabilities.isEmpty()) {
			String[] probs = allProbabilities.split(";");

			if (probs.length != queries.length) {
				JOptionPane.showMessageDialog(null, "Number of queries and probability does not match.", "CODD - Validation Error", JOptionPane.INFORMATION_MESSAGE);
				return;
			}
			double total = 0;
			for (int i = 0; i < queries.length; i++) {
				try {
					prob[i] = Double.parseDouble(probs[i]);
				} catch (NumberFormatException e) {
					JOptionPane.showMessageDialog(null, (i + 1) + "th probability entry is not of Double type.", "CODD - Validation Error", JOptionPane.ERROR_MESSAGE);
					return;
				}
				total = total + prob[i];
			}
			if (total != 1.0) {
				JOptionPane.showMessageDialog(null, "The sum of probability of query must be 1.0.", "CODD - Validation Error", JOptionPane.INFORMATION_MESSAGE);
				return;
			}
		} else {
			// Default Values. Uniform Probability Distribution.
			for (int i = 0; i < queries.length; i++) {
				prob[i] = 1.0 / queries.length;
			}
		}

		Constants.CPrintToConsole("Probabilities: ", Constants.DEBUG_FIRST_LEVEL_Information);
		for (int i = 0; i < queries.length; i++) {
			Constants.CPrintToConsole(prob[i] + " ", Constants.DEBUG_FIRST_LEVEL_Information);
		}
		Constants.CPrintToConsole("", Constants.DEBUG_FIRST_LEVEL_Information);

		{
			double sf = Double.parseDouble(jLabel8.getText());
			boolean success = true;
			int[] costScaleFactor = null;
			costScaleFactor = costScale(queries, sf); // Do costScaling and get the scaling factors for each selected relation
			if (costScaleFactor != null) {
				for (int i = 0; i < selectedRelation.length; i++) {
					String relation = selectedRelation[i];
					try {
						int sf_rel = costScaleFactor[i];
						Constants.CPrintToConsole("Scaling Relation " + relation + " by " + sf_rel, Constants.DEBUG_FIRST_LEVEL_Information);

						String msg;
						if (database.scale(relation, sf_rel, false)) {
							msg = "Metadata Time Scaling on relation " + relation + " is successful.";
						} else {
							msg = "Metadata Time Scaling on relation " + relation + " has FAILED.";
							success = false;
						}
						Constants.CPrintToConsole(msg, Constants.DEBUG_FIRST_LEVEL_Information);

					} catch (Exception e) {
						Constants.CPrintErrToConsole(e);
						JOptionPane.showMessageDialog(null, "Exception Caught: Time Scaling of Relation failed for " + relation + ".", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
						success = false;
					}
				}
			} else {
				success = false;
			}

			if (success) {
				JOptionPane.showMessageDialog(null, "Metadata Time Scaling is Successful.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
			} else {
				JOptionPane.showMessageDialog(null, "Metadata Time Scaling has Failed.", "CODD - Message", JOptionPane.INFORMATION_MESSAGE);
			}
		}
		try{
			new GetRelationFrame(database).setVisible(true);
			this.dispose();							
		}catch(Exception e){
            Constants.CPrintErrToConsole(e);
            JOptionPane.showMessageDialog(null,e.getMessage(), "CODD - Exception",JOptionPane.ERROR_MESSAGE);
		}
	}//GEN-LAST:event_jButton1ActionPerformed

	private void jButton4ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton4ActionPerformed
		new ModeSelection(database, selectedRelation).setVisible(true);
		this.dispose();
	}//GEN-LAST:event_jButton4ActionPerformed
	/**
	 * @param args the command line arguments
	 */
	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton jButton1;
	private javax.swing.JButton jButton4;
	private javax.swing.JLabel jLabel2;
	private javax.swing.JLabel jLabel4;
	private javax.swing.JLabel jLabel7;
	private javax.swing.JLabel jLabel8;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JScrollPane jScrollPane1;
	private javax.swing.JScrollPane jScrollPane2;
	private javax.swing.JTextArea jTextAreaProbabilities;
	private javax.swing.JTextArea jTextAreaQueries;
	// End of variables declaration//GEN-END:variables

	/**
	 * Evaluates the parameter term with the given Point x
	 * @param term Term object
	 * @param x Point
	 * @return evaluated value
	 */
	private double evaluateTerm(Term term, double[] x) {
		double termValue = term.getValue();
		String termStr = term.getStr();
		for (int i = 0; i < selectedRelation.length; i++) {
			String relation = selectedRelation[i].toLowerCase();
			String scaleFactor = this.scaleFactors.get(relation);
			if (termStr.contains(scaleFactor)) {
				termValue = termValue * x[i];
			}
		}
		ArrayList<Term> logTerms = term.getLogTerms();
		for (int i = 0; i < logTerms.size(); i++) {
			double logTermVal = evaluateTerm(logTerms.get(i), x);
			double logValue = Math.log10(logTermVal);
			termValue = termValue * logValue;
		}
		Constants.CPrintToConsole(term + " : " + termValue, Constants.DEBUG_THIRD_LEVEL_Information);
		return termValue;
	}

	/**
	 * Evaluates the cost function of plan indicated by index with the given point x.
	 * @param index Plan Cost function index
	 * @param x Point
	 * @return Plan cost indicated by index at x
	 */
	private double evaluateCostFunction(int index, double[] x) {
		HashMap<String, Term> costFunction = planCostFunctions[index];
		double value = 0.0;

		Set<String> keys = costFunction.keySet();
		Iterator kiter = keys.iterator();
		while (kiter.hasNext()) {
			String key = (String) kiter.next();
			Term term = costFunction.get(key);
			value = value + evaluateTerm(term, x);
		}

		String str_x = new String();
		for (int i = 0; i < x.length; i++) {
			str_x = str_x + x[i] + " ";
		}
		Constants.CPrintToConsole("CostFunction Evaluated value : " + index + " at (" + str_x + ") - " + value, Constants.DEBUG_THIRD_LEVEL_Information);
		return value;
	}

	/**
	 * Evaluates the objective function of cost scaling optimization problem.
	 * @param x point
	 * @return Objective value at x
	 */
	public double evaluateObjective(double[] x) {
		// SUM ((Cost_scaled / Cost_originial) - alpha)^2
		double objValue = 0.0;
		for (int q = 0; q < this.planCostFunctions.length; q++) {
			double costScaled = this.evaluateCostFunction(q, x);
			objValue = objValue + Math.pow(((costScaled / originalPlanCost[q]) - scaleFactor), 2);
		}

		String str_x = new String();
		for (int i = 0; i < x.length; i++) {
			str_x = str_x + x[i] + " ";
		}
		Constants.CPrintToConsole("Objective Function Value at (" + str_x + ") : " + objValue, Constants.DEBUG_THIRD_LEVEL_Information);
		return objValue;
	}

	/**
	 * Evaluates the constraint of cost scaling optimization problem.
	 * @param x Point
	 * @return Value of constraint at x
	 */
	public double evaluateConstraint(double[] x) {
		// SUM Cost_scaled * prob = alpha * (SUM Cost_originial * prob)
		// left - right
		double left = 0.0;
		for (int q = 0; q < this.planCostFunctions.length; q++) {
			double costScaled = this.evaluateCostFunction(q, x);
			left = left + (costScaled * prob[q]);
		}

		double right = 0.0;
		for (int q = 0; q < this.planCostFunctions.length; q++) {
			right = right + (originalPlanCost[q] * prob[q]);
		}
		right = right * scaleFactor;

		String str_x = new String();
		for (int i = 0; i < x.length; i++) {
			str_x = str_x + x[i] + " ";
		}
		Constants.CPrintToConsole("Constraint Value at (" + str_x + ") left - " + left + " right - " + right, Constants.DEBUG_THIRD_LEVEL_Information);
		return left - right;
	}

	/**
	 * Evaluates Lemma2 constraint of given relation with the given point x and returns the value.
	 * @param relation Relation for which Lemma 2 constraint has to be evaluated.
	 * @param x Point
	 * @return Value of constraint at x
	 */
	public double evaluteLemma2Constraint(String relation, double[] x) {

		Term[] lemma2Bound = this.lemma2Constraints.get(relation);
		double left = evaluateTerm(lemma2Bound[0], x);
		double right = evaluateTerm(lemma2Bound[1], x);

		String str_x = new String();
		for (int i = 0; i < x.length; i++) {
			str_x = str_x + x[i] + " ";
		}
		Constants.CPrintToConsole("Lemma 2 Constraint Value at (" + str_x + ") left - " + left + " right - " + right, Constants.DEBUG_THIRD_LEVEL_Information);

		return left - right;
	}

	private double[] getBinaryRepresentation(int val, int dimension) {
		double[] d = new double[dimension];

		for (int i = 0; i < dimension; i++) {
			int temp = val / 2;
			d[i] = val % 2;
			val = temp;
		}
		return d;
	}

	/**
	 * Call the Optimizer with multiple points to solve the cost scaling optimization problem.
	 * @param queries Query Workload - set of input queries
	 * @param sf Scaling Factor
	 * @return Solution vector
	 */
	private int[] costScale(String[] queries, double sf) {
		System.out.println("CostScale.");
		int[] costScalingFactor = null;
		planCostFunctions = new HashMap[queries.length];
		originalPlanCost = new double[queries.length]; //total cpu cost
		for (int i = 0; i < queries.length; i++) {
			// Determine the equation
			Plan plan = null;
			try {
				plan = database.getPlan(queries[i]);
			} catch (Exception ex) {
				Constants.CPrintErrToConsole(ex);
				JOptionPane.showMessageDialog(null, "Exception in getting Estimated plan for the query.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
			}
			// SHOW PLAN FOR 0th NODE WHICH IS THE ROOT OF PLAN TREE, BUT IN SQLMX ROOT IS NOT NODE 0,
			int ind = plan.getNodeIndexByName("ROOT");
			if ((database.getSettings().getDbVendor().equals(DBConstants.NONSTOPSQL))) {
				plan.showPlan(ind, true);     // CORRECT SHOW PLAN LOGIC TO GET CORRECT DIAPLY OF PLAN
			} else {
				plan.showPlan(0);
			}
			TreeNode root = plan.createPlanTree();

			int retVal = determineCostFunctionForEachOperator(root);
			if (retVal != 0) {
				return null;
			}
			// returns the costFunction for Plan
			HashMap<String, Term> costFunction = determineCostFunctionForPlan(root);
			String costFunctionAsString = getCostFunctionAsString(costFunction);
			// Step 1 of Cost Scaling Algorithm
			planCostFunctions[i] = costFunction;
			// Step 2 of Cost Scaling Algorithm
			originalPlanCost[i] = root.getEstimatedCost();
			Constants.CPrintToConsole(queries[i] + " \n  --> " + costFunctionAsString + " \n -- Original Cost Function " + originalPlanCost[i], Constants.DEBUG_FIRST_LEVEL_Information);

		}
		// Lemma 2 bound - found in the constructor, use it in the solver

		// Formualte Optimization Problem - Cost Equations, Original Cost is known. evaluateObjective, evaluateConstraint
		// Use mulitple Points to solve and show the objective function, solution to the user and let him choose the option.

		// Form a n-dimension cube, and try out the corner points of the cube + size-scaling point as initial points.
		HashMap<String, Solution> solutions = new HashMap();
		int totalInitialPoints = (int) Math.pow(2, dimension) + 1;
		int min = 1;
		int max = this.scaleFactor * Constants.thresholdTimes;
		boolean atleastOneSolution = false;
		for (int pt = 0; pt < totalInitialPoints; pt++) {
			//for(int pt=0;pt<1;pt++) {
			double[] initialPoint = new double[dimension];
			String initialPtStr = new String();
			if (pt == 0) { // Try out the size-scaling vector
				for (int i = 0; i < dimension; i++) {
					initialPoint[i] = this.scaleFactor;
					initialPtStr = initialPtStr + initialPoint[i] + " ";
				}
			} else {
				double[] binaryValues = getBinaryRepresentation(pt - 1, dimension);
				for (int i = 0; i < dimension; i++) {
					if (binaryValues[i] == 0) {
						initialPoint[i] = min;
					} else {
						initialPoint[i] = max;
					}
					initialPtStr = initialPtStr + initialPoint[i] + " ";
				}
			}
			Optimizer opt = new Optimizer(this, initialPoint);
			try {
				Constants.CPrintToConsole("Solving Optimization problem.... with intial point as " + initialPtStr, Constants.DEBUG_FIRST_LEVEL_Information);
				Solution soln = opt.solveConstrainedOptimization();
				atleastOneSolution = true;
				// Add the solution to list of Solutions
				solutions.put(soln.getSolnString(), soln);
				System.out.println(" solutions : " + soln.getSolnString() + "  value : " + solutions.get(soln.getSolnString()));
			} catch (RuntimeException e) {
				// Unable to solve the problem for this initial point, Continue with the next point.
				// Constants.CPrintErrToConsole(e);
				Constants.CPrintToConsole(" RuntimeException: Unable to solve a optimization problem for the initial Point " + initialPtStr, Constants.DEBUG_FIRST_LEVEL_Information);
				Constants.CPrintErrToConsole(e);
			} catch (Exception e) {
				Constants.CPrintErrToConsole(e);
				JOptionPane.showMessageDialog(null, "Exception Caught: Error in solving Optimization problem.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				return null;
			}
		}
		String relationInOrder = new String();
		for (int r = 0; r < dimension; r++) {
			relationInOrder = relationInOrder + selectedRelation[r];
			if (r < dimension - 1) {
				relationInOrder = relationInOrder + ", ";
			}
		}
		if (atleastOneSolution && solutions.size() > 1) {
			Object[] possibilities = new Object[solutions.size()];
			Set<String> keys = solutions.keySet();
			Iterator kiter = keys.iterator();
			int p = 0;
			while (kiter.hasNext()) {
				String key = (String) kiter.next();
				Solution soln = solutions.get(key);
				possibilities[p] = "Soln: (" + relationInOrder + ") - " + soln.toString();
				p++;
			}

			String s = (String) JOptionPane.showInputDialog(
					this,
					"Choose the solution:\n",
					"Multiple Solutions are obtained for the optimization problem. Choose any one.",
					JOptionPane.PLAIN_MESSAGE,
					null,
					possibilities,
					"");


			// Code Block Added by Deepali
			s = s.substring(s.lastIndexOf("(") + 1, s.lastIndexOf(" )")).replace(", ", "-").replace("(", "").replace(" )", "").replace(" ", "");

			//If a string was returned, say so.
			if ((s != null) && (s.length() > 0)) {
				Solution chosenSoln = solutions.get(s);
				//                System.out.println(" s : " + s );     // This s is not correct... key is different
				//                System.out.println(" --- chosenSoln " + chosenSoln.getSolnString());// + " == " + chosenSoln.toString());
				costScalingFactor = chosenSoln.getSoln();
			} else {
				JOptionPane.showMessageDialog(null, "No Solution was chosen. Exiting the TimeScaling.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				costScalingFactor = null;
			}

		} else if (atleastOneSolution) {
			// There is only one solution.
			Set<String> keys = solutions.keySet();
			Solution soln = null;
			Iterator kiter = keys.iterator();
			while (kiter.hasNext()) {
				String key = (String) kiter.next();
				soln = solutions.get(key);
			}
			int r = JOptionPane.showConfirmDialog(null,
					"Optimization has returned an unique solution : (" + relationInOrder + ") - " + soln.toString() + ". Would you like to use this for TimeScaling?",
					"Continue to use the solution..",
					JOptionPane.YES_NO_OPTION);
			if (r == JOptionPane.YES_OPTION) {
				costScalingFactor = soln.getSoln();
			} else {
				JOptionPane.showMessageDialog(null, "No Solution was chosen. Exiting the TimeScaling.", "CODD - Exception", JOptionPane.ERROR_MESSAGE);
				costScalingFactor = null;
			}
		} else {
			JOptionPane.showMessageDialog(null, "Unable to solve the Optimization problem.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
			costScalingFactor = null;
		}

		return costScalingFactor;
	}

	/**
	 * Determines the costFunction for each operator. First four steps of Procedure in Technical Report
	 * @param root Node
	 * @return 0 on success, -1 on failure
	 */
	private int determineCostFunctionForEachOperator(TreeNode root) {
		/**
		 * Follows the Depth First Traversal / Bottom Up approach.
		 */
		Vector children = root.getChildren();
		double childTotalCost = 0;
		for (int i = 0; i < children.size(); i++) {
			TreeNode childNode = (TreeNode) children.get(i);
			int retVal = determineCostFunctionForEachOperator(childNode);

			if (retVal != 0) {
				return retVal;
			}

			childTotalCost = childTotalCost + childNode.getEstimatedCost();
		}

		/*
		 * Update subTreeRelations based on the operator.
		 */
		HashSet<String> subTreeRelations = new HashSet();
		if (children.isEmpty()) // Leaf Node
		{
			String relName = root.getNodeName().toLowerCase(); // For DB2, Oracle and Postgres and NonStopSQL
			subTreeRelations.add(relName);
		} else // For other nodes, combine the child's subTreeRelations
		{
			for (int i = 0; i < children.size(); i++) {
				TreeNode childNode = (TreeNode) children.get(i);
				subTreeRelations.addAll(childNode.getSubTreeRelations());
			}
		}
		root.setSubTreeRelations(subTreeRelations);

		/*
		 * 1) Determine Operator CPU cost
		 */
		double currNodeOperatorCost = root.getEstimatedCost() - childTotalCost;
		root.setOperatorCPUCost(currNodeOperatorCost);

		/*
		 * 2) Determine the original output of this operator
		 */
		root.setOutput(root.getCardinality());

		/*
		 * 3) Determine the Simple Cost Model Operator Cost wrt original input of this operator
		 *
		 * Changing this also requires to change Simple Cost Model for Scaled Output (Step 5)
		 */
		String operator = root.getNodeName().toLowerCase();
		double costOriginal = 0;
		if (operator.equalsIgnoreCase("HYBRID_HASH_JOIN") || (operator.contains("hash") && operator.contains("join")) || (operator.contains("merge") && operator.contains("join")) || operator.contains("hsjoin") || operator.contains("msjoin") || (operator.contains("index") && operator.contains("join"))) {
			//System.out.println("HASH OR MERGE JOIN OPERATOR SCALING..");
			// x + y
			TreeNode childNodeX = (TreeNode) children.get(0);
			double x = childNodeX.getCardinality();
			TreeNode childNodeY = (TreeNode) children.get(1);
			double y = childNodeY.getCardinality();
			costOriginal = x + y;
		} else if (operator.equalsIgnoreCase("NESTED_JOIN") || ((operator.contains("nested") || operator.contains("loop") || operator.contains("nljoin")) && operator.contains("join"))) {
			//System.out.println("NESTED LOOP JOIN SCLAING...");
			// x * y
			TreeNode childNodeX = (TreeNode) children.get(0);
			double x = childNodeX.getCardinality();
			TreeNode childNodeY = (TreeNode) children.get(1);
			double y = childNodeY.getCardinality();
			costOriginal = x * y;
		} else if (operator.contains("sort") || operator.equalsIgnoreCase("SORT")) {
			//System.out.println("SORT OPERATOR SCALING.");
			// x log x
			TreeNode childNode = (TreeNode) children.get(0);
			double x = childNode.getCardinality();
			costOriginal = x * Math.log10(x);
		} else // For other operators
		{
			//System.out.println("SCALING OTHER OPERATORS." + operator);
			// x
			double x;
			// Add other operator, if it is not in {Group by, filter, Table / Index Scan}
			if (operator.contains("return") || operator.contains("select") || operator.equalsIgnoreCase("ROOT")) // SELECT and RETURN are like ROOT in NSSQL, thats why I added ROOT here
			{
				x = 0;
			} else {
				// For leaf node, output is same as input.
				if (children.isEmpty()) // It is a Leaf Node
				{
					if (database.getSettings().getDbVendor().equals(DBConstants.DB2) || database.getSettings().getDbVendor().equals(DBConstants.ORACLE) || database.getSettings().getDbVendor().equals(DBConstants.POSTGRES) || database.getSettings().getDbVendor().equals(DBConstants.NONSTOPSQL)) {
						x = 0; // Leaf node represents the Relation name
					} else {
						x = root.getCardinality();
					}
				} else {
					TreeNode childNode = (TreeNode) children.get(0);
					if ((database.getSettings().getDbVendor().equals(DBConstants.POSTGRES) || database.getSettings().getDbVendor().equals(DBConstants.NONSTOPSQL)) && childNode.getChildren().isEmpty()) {
						// Node above leaf node. It must be any of the scan operators
						x = root.getCardinality();
					} else {
						x = childNode.getCardinality();
					}
				}
			}
			costOriginal = x;
		}
		root.setSCM_cost(costOriginal);

		/*
		 * 4) Determine the scaled output of this operator. Using Lemma 1
		 */
		Term scaledOutput = new Term(root.getCardinality()); // Initialize with original output
		// Determine the zero incoming nodes of depedeny graph from the subTreeRelations (edges are from fK to PK relation)
		Iterator iter = subTreeRelations.iterator();
		while (iter.hasNext()) {
			String rel = (String) iter.next();
			if (TimeScaling.scaledOutputOnFKRels) {
				ArrayList depRels = this.dependencyGraph.get(rel);

				if (depRels == null) {
					JOptionPane.showMessageDialog(null, "Error: Relation " + rel + " is not chosen but used in the query.", "CODD - Error", JOptionPane.ERROR_MESSAGE);
					return -1;
				}
				int newDepRelCnt = 0;
				Iterator iter1 = subTreeRelations.iterator();
				while (iter1.hasNext()) {
					String rel1 = (String) iter1.next();

					if (depRels.contains(rel1)) {
						newDepRelCnt++;
					}
				}
				if (newDepRelCnt == 0) {
					// No incoming edge on the newGraph, add it to the output scaling factor
					String sf = this.scaleFactors.get(rel);   // See from where it is seting
					Term term = new Term(sf);
					scaledOutput.include(term);
				}
			} else {
				// Talha's model
				String sf = this.scaleFactors.get(rel);
				Term term = new Term(sf);
				scaledOutput.include(term);
			}
		}
		root.setScaledOutput(scaledOutput);

		/**
		 * 5) Determine the Simple Cost Model Scaled Operator Cost wrt scaled input of this operator
		 */
		HashMap<String, Term> SCM_ScaledCost = new HashMap();
		String str_SCMScaledCost = new String();
		// ADD OPERATORS FOR NON STOP SQL HERE
		if ((operator.equalsIgnoreCase("HYBRID_HASH_JOIN")) || (operator.contains("hash") && operator.contains("join")) || (operator.contains("merge") && operator.contains("join")) || operator.contains("hsjoin") || operator.contains("msjoin") || (operator.contains("index") && operator.contains("join"))) {
			// x + y
			TreeNode childNodeX = (TreeNode) children.get(0);
			Term x = childNodeX.getScaledOutput();
			TreeNode childNodeY = (TreeNode) children.get(1);
			Term y = childNodeY.getScaledOutput();
			SCM_ScaledCost.put(x.getKey(), x);
			SCM_ScaledCost.put(y.getKey(), y);
			str_SCMScaledCost = str_SCMScaledCost + x + " + " + y;
		} // ADD OPERATORS FOR NON STOP SQL HERE
		else if (operator.equalsIgnoreCase("NESTED_JOIN") || ((operator.contains("nested") || operator.contains("loop") || operator.contains("nljoin")) && operator.contains("join"))) {
			//System.out.println("Scaling nested join operator.");
			// x * y
			TreeNode childNodeX = (TreeNode) children.get(0);
			Term x = childNodeX.getScaledOutput();
			TreeNode childNodeY = (TreeNode) children.get(1);
			Term y = childNodeY.getScaledOutput();
			x.include(y);
			SCM_ScaledCost.put(x.getKey(), x);
			str_SCMScaledCost = str_SCMScaledCost + x;
		} // ADD OPERATORS FOR NON STOP SQL HERE
		else if (operator.contains("sort") || operator.equalsIgnoreCase("SORT")) {
			//System.out.println("Scaling sort operator.");
			// x log x
			TreeNode childNode = (TreeNode) children.get(0);
			Term x = childNode.getScaledOutput();
			Term y = new Term(new String(), new Term(x));
			x.include(y);
			SCM_ScaledCost.put(x.getKey(), x);
			str_SCMScaledCost = str_SCMScaledCost + x;
		} // ADD OPERATORS FOR NON STOP SQL HERE
		else // For other operators
		{
			//System.out.println("Other operators.");
			// x
			Term x;
			// Add other operator, if it is not in {Group by, filter, Table / Index Scan}
			if (operator.contains("return") || operator.contains("select") || operator.equalsIgnoreCase("ROOT")) {
				x = new Term("zero", 0.0);
			} else {
				// For leaf node, output is same as input.
				if (children.isEmpty()) // It is a Leaf Node
				{
					if (database.getSettings().getDbVendor().equals(DBConstants.DB2) || database.getSettings().getDbVendor().equals(DBConstants.ORACLE) || database.getSettings().getDbVendor().equals(DBConstants.POSTGRES) || database.getSettings().getDbVendor().equals(DBConstants.NONSTOPSQL)) {
						x = new Term("zero", 0.0); // Leaf node represents the Relation name
					} else {
						x = new Term(root.getScaledOutput());
					}

				} else {
					TreeNode childNode = (TreeNode) children.get(0);
					if ((database.getSettings().getDbVendor().equals(DBConstants.POSTGRES) || database.getSettings().getDbVendor().equals(DBConstants.NONSTOPSQL)) && childNode.getChildren().isEmpty()) {
						// Node above leaf node. It must be any of the scan operators
						x = new Term(root.getScaledOutput());
					} else {
						x = new Term(childNode.getScaledOutput());
					}
				}
			}
			SCM_ScaledCost.put(x.getKey(), x);
			str_SCMScaledCost = str_SCMScaledCost + x;
		}
		root.setSCM_scaledCost(SCM_ScaledCost);

		/**
		 * 6) Determine the scaled operator cost
		 * scaledOperatorCost = operatorCPUCost *(SCM_scaledCost / SCM_cost)
		 */
		HashMap<String, Term> scaledOperatorCost = new HashMap();
		String str_scaledOperatorCost = new String();
		double costFunction = 0.0;
		if (root.getSCM_cost() != 0) {
			costFunction = root.getOperatorCPUCost() / root.getSCM_cost();
		}
		Set<String> keys = SCM_ScaledCost.keySet();
		Iterator kiter = keys.iterator();
		while (kiter.hasNext()) {
			String key = (String) kiter.next();
			Term term = SCM_ScaledCost.get(key);
			term.multiplyValue(costFunction);
			scaledOperatorCost.put(key, term);
			if (str_scaledOperatorCost.length() > 0) {
				str_scaledOperatorCost = str_scaledOperatorCost + " + " + term;
			} else {
				str_scaledOperatorCost = str_scaledOperatorCost + term;
			}

		}

		root.setScaledOperatorCost(scaledOperatorCost);

		Constants.CPrintToConsole(root.getNodeName() + " (Card-" + root.getCardinality() + ", Cost-" + root.getEstimatedCost() + "): cpuCost - " + root.getOperatorCPUCost(), Constants.DEBUG_SECOND_LEVEL_Information);
		Constants.CPrintToConsole(" Output -" + root.getOutput() + " SCM_Cost - " + root.getSCM_cost() + " ScaledOutput - " + root.getScaledOutput(), Constants.DEBUG_SECOND_LEVEL_Information);
		Constants.CPrintToConsole(" SCM_ScaledCost - " + str_SCMScaledCost, Constants.DEBUG_SECOND_LEVEL_Information);
		Constants.CPrintToConsole(" ScaledOperatorCost - " + str_scaledOperatorCost, Constants.DEBUG_SECOND_LEVEL_Information);

		/*
        Hashtable attr = root.getAttributes();
        Set<String> set = attr.keySet();
        String str;
        Iterator<String> itr = set.iterator();
        int retValue = 0;
        boolean firstAttr = false;
        while (itr.hasNext()) {
        str = itr.next();
        String strAttr = (String) attr.get(str);
        Constants.CPrintToConsole(strAttr + " - ", Constants.DEBUG_SECOND_LEVEL_Information);
        }
		 */
		Constants.CPrintToConsole("", Constants.DEBUG_SECOND_LEVEL_Information);
		return 0;
	}

	/**
	 * Sums up the cost function of current node and its children to determine the cost function for the plan
	 * @param root root node of the tree
	 * @return Cost Function obtained for the tree rooted at node root.
	 */
	private HashMap<String, Term> determineCostFunctionForPlan(TreeNode root) {
		HashMap<String, Term> costFunction = (HashMap<String, Term>) root.getScaledOperatorCost().clone();
		/**
		 * Follows the Depth First Traversal / Bottom Up approach.
		 */
		Vector children = root.getChildren();
		for (int i = 0; i < children.size(); i++) {
			TreeNode childNode = (TreeNode) children.get(i);
			HashMap<String, Term> child_CostFunction = determineCostFunctionForPlan(childNode);
			// Do Sum - Group by key value of the terms
			Set<String> keys = child_CostFunction.keySet();
			Iterator kiter = keys.iterator();
			while (kiter.hasNext()) {
				String key = (String) kiter.next();
				Term term = child_CostFunction.get(key);
				if (costFunction.containsKey(key)) { // Key already exists. just add the value to the existing.
					Term existingTerm = costFunction.get(key);
					existingTerm.addValue(term.getValue()); // Just add the double value
					costFunction.put(key, existingTerm);
				} else {
					costFunction.put(key, term);
				}
			}
		}
		costFunction.remove("zero::"); // remove the zero elements.
		return costFunction;
	}

	/**
	 * Given a cost function (a set of terms in a map), returns its string representation.
	 * @param costFunction costFunction as a set of terms in a map
	 * @return string representation of the cost function
	 */
	private String getCostFunctionAsString(HashMap<String, Term> costFunction) {
		String str_scaledOperatorCost = new String();
		Set<String> keys = costFunction.keySet();
		Iterator kiter = keys.iterator();
		while (kiter.hasNext()) {
			String key = (String) kiter.next();
			Term term = costFunction.get(key);
			str_scaledOperatorCost = str_scaledOperatorCost + term + " + ";
		}
		// remove last plus symbol
		str_scaledOperatorCost = str_scaledOperatorCost.substring(0, str_scaledOperatorCost.lastIndexOf("+"));
		return str_scaledOperatorCost;
	}
	/**
	 * Main function added to test CostScaling independently.
	 * @param args
	 */
}
