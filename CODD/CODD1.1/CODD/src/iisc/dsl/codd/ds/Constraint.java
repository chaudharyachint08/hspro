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
package iisc.dsl.codd.ds;

import java.io.Serializable;

/**
 * Implementation of Database Integrity Constraints.
 * Some columns of a relation is associated with a constraints (Primary Key, Foreign Key,.. ).
 * This class represents the Constraint. An object of this class is associated with each column
 * statistics, to identify the constraint present on the column.
 *
 * The tool "supports" the following states of the column constraint. i.e each column will belong to
 * "any one of the constraint" listed below.
 * 1) PRIMARYKEY
 *     Constraint for Primary Key Columns. It includes UNIQUE, NOTNULL constraints.
 *     Note that, It should not be used for the composite primary key columns.
 *     TPC-H Example Column: N_NATIONKEY of NATION relation.
 * 2) FOREIGNKEY
 *     Constraint for Foreign Key Columns.
 *     Note that, It should not be used for the foreign key columns, which is part of primary key.
 *     TPC-H Example Column: N_REGIONKEY of NATION relation.
 * 3) COMPOSITE_PK
 *     Constraint for Composite Primary Key Columns. It includes NOTNULL constraints.
 *     TPC-H Example Column: L_LINENUMBER of LINEITEM relation.
 * 4) COMPOSITE_PK_FK
 *     Constraint for Composite Primary Key Columns, which is also a Foreign Key. It includes NOTNULL constraints.
 *     TPC-H Example Column: L_ORDERKEY of LINEITEM relation. PS_PARTKEY, PS_SUPPKEY of PARTSUPP relation.
 * 5) UNIQUE
 *     Constraint for Unique Columns, which is not a Primary Key Column.
 * 6) NOTNULL
 *     Constraint for NOT NULL Columns, which is not a Primary Key Column.
 *     TPC-H Example Column: N_NATIONNAME of NATION relation.
 * 7) NONE
 *     This is used for the columns, which does not fall into any of the above categories.
 *     i.e There is no constraint on this columns.
 * @author dsladmin
 */
public class Constraint implements Serializable {
	
	private static final long serialVersionUID = -5091518436968336565L;
	
	/**
     * Primary Key Constraint.
     */
    public final static String PRIMARYKEY = "PRIMARYKEY";
    public final static String PRIMARY_KEY = "PK";
    
    /**
     * Foreign Key Constraint.
     */
    public final static String FOREIGNKEY = "FOREIGNKEY";
    public final static String FOREIGN_KEY = "RC";
    
    /**
     * Primary Key Constraint. Column is a "part of" Composite Primary Key.
     */
    public final static String COMPOSITE_PK = "COMPOSITE_PK";
    
    /**
     * Primary Key Constraint, Foreign Key Constraint. Column is a "part of" Composite Primary Key and a Foreign Key.
     */
    public final static String COMPOSITE_PK_FK = "COMPOSITE_PK_FK";
    
    /**
     * Unique Constraint. Column is not a part of Primary Key Columns.
     */
    public final static String UNIQUE = "UNIQUE";
    public final static String UNIQUE_NS = "UC";
    
    /**
     * NOT NULL Constraint. Column is not a part of Primary Key Columns.
     */
    public final static String NOTNULL = "NOTNULL";
    public final static String NOT_NULL = "NN";
    
    /**
     * No Constraints on the column.
     */
    public final static String NONE = "NONE";

    /**
     * Represents the type of constraint present in the column.
     */
    private String type;

    /**
     * Constructor for Constraint class. Values are initialized from a Constraint object.
     * @param c - Constraint object
     */
    public Constraint(Constraint c)
    {
        this.type = c.type;
    }

    /**
     * Constructor for Constraint class.
     * @param constraintval  - String representation of the constraint. Must be equal
     * to any one of the "supporting" constraint.
     */
    public Constraint(String constraintval)
    {
        this.type = constraintval;
    }

    /**
     * Returns true, if a constraint specified by 'type' attribute is a Primary Key column.
     * @return true, if the column is a Primary Key.
     *         false, otherwise.
     */
     public boolean isPK() {
        if(type.equals(Constraint.PRIMARYKEY) || type.equals(Constraint.PRIMARY_KEY)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if a constraint specified by 'type' attribute is a part of  / a Primary Key column.
     * @return true, if the column is a Primary Key or a part of Primary Key.
     *         false, otherwise.
     */
    public boolean isPartOfPK() {
        return isPK() || isCompositePK();
    }

    /**
     * Returns true, if a constraint specified by 'type' attribute is part a Foreign Key column.
     * @return true, if the column is a Foreign Key.
     *         false, otherwise.
     */
    public boolean isFK() {
        if(type.equals(Constraint.FOREIGNKEY) || type.equals(Constraint.COMPOSITE_PK_FK) || type.equals(Constraint.FOREIGN_KEY)) {
            return true;
        }
        return false;
    }

    /**
     *
     * Returns true, if a constraint specified by 'type' attribute is a Composite Primary Key column.
     * @return true, if the column is a part of Composite Primary Key.
     *         false, otherwise.
     */
   public boolean isCompositePK() {
        if(type.equals(Constraint.COMPOSITE_PK) || type.equals(Constraint.COMPOSITE_PK_FK)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if a constraint specified by 'type' attribute is a Unique column.
     * @return true, if the column has Unique constraint.
     *         false, otherwise.
     */
    public boolean isUnique() {
        if(type.equals(Constraint.UNIQUE) || type.equals(Constraint.PRIMARYKEY) || type.equals(Constraint.UNIQUE_NS) || type.equals(Constraint.PRIMARY_KEY)) {
            return true;
        }
        return false;
    }

    /**
     * Returns true, if a constraint specified by 'type' attribute is a NOTNULL column.
     * @return true, if the column has NOTNULL constraint.
     *         false, otherwise.
     */
    public boolean isNotNULL() {
        if(type.equals(Constraint.NOTNULL) || this.isPartOfPK() || this.isFK() || type.equals(Constraint.NOT_NULL)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return this.type;
    }
}