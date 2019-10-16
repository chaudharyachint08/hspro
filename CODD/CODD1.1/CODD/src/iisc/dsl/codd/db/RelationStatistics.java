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
package iisc.dsl.codd.db;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Relation Statistics of a relation is represented with this class.
 * Each of the derived subclass should have scale and mapForPorting function
 * and call the super class function.
 * @author dsladmin
 */
public class RelationStatistics implements Serializable {

	private static final long serialVersionUID = 8001479597224359534L;
	
	protected String relationName; // Relation name (store in CAPS always and do transformation as required)
    protected String schemaName; // Schema name (store in CAPS always and do transformation as required)
    protected BigDecimal relCard; // Number of rows in the relation
    protected BigDecimal relPages; // Number of pages / blocks in which the relation tuples exists

    /**
     * Constructs a RelationStatistics for the specified relation with the default values.
     * @param relation Relation name
     * @param schema Schema name
     */
    public RelationStatistics(String relation, String schema)
    {
        this.relationName = relation.toUpperCase();
        this.schemaName = schema.toUpperCase();
        this.relCard = BigDecimal.ZERO;
        this.relPages = BigDecimal.ZERO;
    }

    public String getRelationName()
    {
        return this.relationName;
    }

    public void setRelationName(String relation)
    {
        this.relationName = relation;
    }

    public String getSchema()
    {
        return this.schemaName;
    }

    public void setSchema(String schema)
    {
        this.schemaName = schema;
    }

    public BigDecimal getCardinality()
    {
        return this.relCard;
    }

    public void setCardinality(BigDecimal card)
    {
        this.relCard = card;
    }

    public BigDecimal getPages()
    {
        return this.relPages;
    }

    public void setPages(BigDecimal pages)
    {
        this.relPages = pages;
    }

    /**
     * Initializes the super class RelationStatistics metadata fields with the specified RelationStatistics object.
     * This is used in case of porting metadata.
     * At Non-Engine Specific relation level, all metadata fields are portable.
     * @param relStat RelationStatistics
     */
    public void mapForPorting(RelationStatistics relStat)
    {
        //this.setSchema(relStat.getSchema());
        //this.setRelationName(relStat.getRelationName());
        if(relStat!=null && relStat.getCardinality()!=null && relStat.getCardinality().compareTo(BigDecimal.ZERO) > 0) {
            this.setCardinality(relStat.getCardinality());
        } // else deafult value
        if(relStat!=null && relStat.getPages()!=null && relStat.getPages().compareTo(BigDecimal.ZERO) > 0) {
            this.setPages(relStat.getPages());
        } // else deafult value
    }

    /**
     * Scale (size based) the RelationStatistics attributes by the scale factor.
     * @param sf scale factor
     */
    public void scale(int sf)
    {
        BigDecimal scalefactor = BigDecimal.valueOf((long) sf);
        if(this.relCard.compareTo(BigDecimal.ZERO) > 0) {
            this.relCard = this.relCard.multiply(scalefactor);
        }
        if(this.relPages.compareTo(BigDecimal.ZERO) > 0) {
            this.relPages = this.relPages.multiply(scalefactor);
        }
    }
}