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

import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Vector;
import java.util.ListIterator;
import java.io.Serializable;

/**
 * Plan represents a execution plan tree.
 * @author dsladmin
 */

public class Plan {

	private Vector nodes;
        private int RootNodeIndex = -5;

	public Plan()
	{
		nodes = new Vector();
	}

	public double getCost()
	{
            //System.out.println("Cost of " + ((Node)nodes.get(0)).getName());
		return ((Node)nodes.get(0)).getCost();
	}

	public double getCard()
	{
		return ((Node)nodes.get(0)).getCard();
	}

	public Node getNodeById(int id)
	{
		Node node;
		ListIterator it = nodes.listIterator();
		while(it.hasNext()){
			node = (Node)it.next();
			if(node.getId() == id)
				return node;
		}
		return null;
	}
        
        // Added By Deepali :- For NonSTopSQL
        public int getNodeIndexByName(String name) {
            Node node;
            ListIterator it = nodes.listIterator();
            int index = 0;
            while (it.hasNext()) {
                node = (Node) it.next();
                if (node.getName().equalsIgnoreCase(name)) {
                    RootNodeIndex = index;
                    return index;
                }
                index++;
            }
            return -10;
        }

	public Node getNode(int index)
	{
		if(index >=0 && index<nodes.size())
			return (Node)nodes.get(index);
		return null;
	}

	public void setNode(Node node, int index)
	{
		nodes.add(index,node);
	}

	public int getSize()
	{
		return nodes.size();
	}

	public void showPlan(int index)
	{
            /*
             * Assumption : Nodes are stored in top-down fashion
             */
		if(index>= nodes.size())
			return;
		boolean firstCall = false;
		if(index==0)
			firstCall=true;
		Node root = ((Node)nodes.elementAt(index));
		if(!firstCall && root.getId()==-1){   // Id = -1 is assigned to Relation Names
			//System.out.print(root.getId() + " :: " + root.getName());
                    System.out.println(" (" + root.getName() + ") ");
			return;
		}
		ListIterator it = nodes.listIterator(index);
		System.out.print("(");
                Node left = null;
		while(it.hasNext()){
			left = (Node)it.next();
			if(left.getParentId() == root.getId()){
				showPlan(index);
				break;
			}
			index++;
		}
                //System.out.println("index : " + index);
		System.out.print(" "+root.getName()+" ");
		while(it.hasNext()){
			Node right = (Node)it.next();
			if((right.getParentId() == root.getId()) && ((right.getId() != left.getId()) || (left == null)))   // Right Child should not be same as right child
				showPlan(index);
			index++;
		}
		System.out.print(")");
		if(firstCall==true)
			System.out.println();
	}
        
        public void showPlan(int index, boolean firstCall)
	{
            /*
             * Assumption : Nodes are stored in top-down fashion
             */
            
            
		if(index>= nodes.size())
			return;
             
		//boolean firstCall = false;
		//if(index==0)
		//	firstCall=true;
		Node root = ((Node)nodes.elementAt(index));
		if(!firstCall && root.getId()==-1){   // Id = -1 is assigned to Relation Names
			System.out.print(root.getId() + " :: " + root.getName());
			return;
		}
		ListIterator it = nodes.listIterator(index);
		System.out.print("(");
		while(it.hasNext()){
			Node left = (Node)it.next();
			if(left.getParentId() == root.getId()){
				showPlan(index, false);
				break;
			}
			index++;
		}
                //System.out.println("index : " + index);
		System.out.print(" "+root.getName()+" ");
		while(it.hasNext()){
			Node right = (Node)it.next();
			if(right.getParentId() == root.getId())
				showPlan(index, false);
			index++;      // Comment added by Deepali : Earlier it was index++ every where ... I changed it to index++ since root is having max index and we are moving top to bottom
		}
		System.out.print(")");
		if(firstCall==true)
			System.out.println();
	}

	public boolean isIdPresent(int id)
	{
		ListIterator it = nodes.listIterator();
		while(it.hasNext()){
			if(((Node)it.next()).getId() == id)
				return true;
		}
		return false;
	}


	public TreeNode createPlanTree()
	{
            /*
            if(RootNodeIndex != -5){
                return createSubTree(null,0,this,RootNodeIndex);   // For NonStop sQL
            }
             * 
             */
                // For NonStopSQL also root node index = 0 
		return createSubTree(null,0,this,0);   // Index of root node = 0
	}
	private TreeNode createSubTree(TreeNode parent, int depth, Plan plan, int index)
	{
                //System.out.println("CreateSubTree.");
                TreeNode root;
                root = new TreeNode(depth, parent);          // See what is depth
                //System.out.println("SubTreecreated.");
		Node node = plan.getNode(index);
                System.out.println("Node : " + node.getName() + "   index : " + index);
		if(node == null)
			return null;
		node.populateTreeNode(root);
		int id = node.getId();
                //System.out.println("id = " + id);
		if(id == -1){   // For leaf level tables
                   // System.out.println("leaf.");
			root.setChildren(new Vector());
			return root;
		}
		Vector children = new Vector();
		for(int i=0;i<plan.getSize();i++){
			node = plan.getNode(i);
			if(node.getParentId() == id){
                            //System.out.println("child node name : " + node.getName());
				children.add(createSubTree(root, depth+1, plan,i));    // For other Engines
                            //children.add(createSubTree(root, depth+1, plan,node.getId()));   // For NonStop SQL
			}
		}
		root.setChildren(children);
		return root;
	}
//apexp
//	got from PGraph.java and simplified.
	static  String getAttributeStr2(TreeNode tree) {
		        java.util.Hashtable table = tree.getAttributes();

		        if ( table == null ) {
		            table = new java.util.Hashtable();
		        }

		        Object[] keys = table.keySet().toArray();
		        if ( keys.length == 0 )
		            return "";

		        String str = " ";
		        int length = 150;
		        for (int i=0; i < keys.length; i++) {
		            str += (keys[i] + "=" + table.get(keys[i]) + "; ");
		        }

		        str = str.replace('"','\'');

		        return str;
		    }


	public static void showSubTree(TreeNode roo)
	{
		System.out.print(roo.getNodeName());

		Vector chd = roo.getChildren();
		if(chd.size()>0)
		{
			System.out.print("(");
			showSubTree((TreeNode)chd.elementAt(0));
			System.out.print(")");
		}

		if(chd.size()>1)
		{
			System.out.print(",");
			System.out.print("(");
			showSubTree((TreeNode)chd.elementAt(1));
			System.out.print(")");
		}

		if(chd.size()>2)
				System.out.print("More than 2 children!");
	}
//end apexp

}