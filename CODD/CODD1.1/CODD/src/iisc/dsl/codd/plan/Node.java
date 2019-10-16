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

import java.util.Vector;
import java.util.ListIterator;
import java.sql.Statement;
import java.sql.SQLException;

/**
 * Node represents a node in the plan tree.
 * @author dsladmin
 */
public class Node {
	private int 	id, parentId, type;
	private String 	name;
	private double 	cost;
	private double card;
	private Vector argType, argValue;

	public Node()
	{
		argType = new Vector();
		argValue = new Vector();
	}
	public int getId()
	{
		return id;
	}
	public void setId(int id)
	{
		this.id = id;
	}

	public int getParentId()
	{
		return parentId;
	}
	public void setParentId(int id)
	{
		this.parentId = id;
	}

	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		if(name != null)
			this.name = name.trim();
		else
			this.name = "";
	}

	public double getCost()
	{
		return cost;
	}
	public void setCost(double cost)
	{
		this.cost = cost;
	}

	public double getCard()
	{
		return card;
	}
	public void setCard(double card)
	{
		this.card = card;
	}

	public void populateTreeNode(TreeNode node)
	{
		node.setNodeValues(id, name, type, cost, cost, card, argType, argValue);
	}

	private String escapeQuotes(String str)
	{
		return str.replaceAll("'","''");
	}

	public boolean isArgTypePresent(String arg)
	{
		return argType.contains(arg);
	}
	public void addArgType(String arg)
	{
		argType.add(arg);
	}
	public void addArgValue(String arg)
	{
		argValue.add(arg);
	}
	public Vector getArgType()
	{
		return argType;
	}
	public Vector getArgValue()
	{
		return argValue;
	}
}
