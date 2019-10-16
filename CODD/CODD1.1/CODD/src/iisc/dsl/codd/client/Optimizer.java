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

import com.numericalmethod.suanshu.analysis.function.rn2r1.RealScalarFunction;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.general.sqp.activeset.SQPActiveSetMinimizer;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.problem.ConstrainedOptimProblemImpl1;
import com.numericalmethod.suanshu.algebra.linear.vector.doubles.Vector;
import com.numericalmethod.suanshu.algebra.linear.vector.doubles.dense.DenseVector;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.constraint.general.GeneralEqualityConstraints;
import com.numericalmethod.suanshu.optimization.multivariate.constrained.constraint.general.GeneralLessThanConstraints;
import iisc.dsl.codd.ds.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Implementation of the Optimization Solving part with the help of SuanShu library. 
 * @author dsladmin
 */
public class Optimizer {

    /**
     * Dimension of the Optimization problem (no. of Unknowns).
     */
    public static int dimension;
    /**
     * Initial points for the Optimization problem. Length must be of Dimension
     */
    public double[] initialPoint;
    /**
     * SQP Solver Argument2
     */
    public double[] SQPArg2;
    /**
     * SQP Solver Argument3
     */
    public double[] SQPArg3;
    /**
     * Objective function tolerance
     */
    public double tolerance;
    /**
     * Number of iterations for the optimization problem
     */
    public int iterations;

    /**
     * Mapping for the InEqualityConstraints - lowerbound
     */
    public static HashMap<RealScalarFunction, Integer> map;

    /**
     * Mapping for the InEqualityConstraints - lemma2
     */
    public static HashMap<RealScalarFunction, String> mapLemma2;

    /**
     * CostScaling object to evaluate the objective function and equality constraint.
     */
    public static TimeScaling costScaling;

    /**
     * Construction for Optimization Solver
     * @param costScaling - CostScaling object, so as to evaluate Objective functions and Constraints
     * @param initialPoint - Initial point to start the optimization algorithm.
     */
    public Optimizer(TimeScaling costScaling, double[] initialPoint) {
        this.costScaling = costScaling;
        this.dimension = initialPoint.length;
        this.initialPoint = initialPoint;
        map = new HashMap();
        mapLemma2 = new HashMap();
        iterations = Constants.iterations;
        tolerance = Constants.tolerance;
    }

    /**
     * Returns the dimension of the Optimization problem.
     * @return dimension
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * Sets dimension for the Optimization problem.
     * @param dimension dimension for the Optimization problem.
     */
    public void setDimesion(int dimension) {
        this.dimension = dimension;
    }

    /**
     * Returns the initial Point used in solving the Optimization problem.
     * @return Initial Point
     */
    public double[] getInitialPoint() {
        return this.initialPoint;
    }

    /**
     * Sets the initial Point used in solving the Optimization problem.
     * @param initialVector Initial Point
     */
    public void setInitialPoint(double[] initialVector) {
        this.initialPoint = initialVector;
    }

    /**
     * Returns the index of unknown array for the specified lower bound constraint function.
     * @param function Constraint Function
     * @return index of the array x
     */
    public static Integer getIndex(RealScalarFunction function) {
        return map.get(function);
    }

    /**
     * Returns the lemma2 Constraint as a string for the specified constraint function.
     * @param function Constraint Function
     * @return string format of lemma 2 constraint function
     */
    public static String getLemma2Relation(RealScalarFunction function) {
        return mapLemma2.get(function);
    }

    /**
     * Solves the optimization problem and returns the solution.
     * @return solution solution of the Optimization problem
     * @throws Exception
     */
    public Solution solveConstrainedOptimization() throws Exception {

        // Objective Function
        RealScalarFunction f = new RealScalarFunction() {
            public int dimensionOfDomain() {
                //return 2;
                return Optimizer.dimension;
            }

            public int dimensionOfRange() {
                return 1;
            }

			@Override
			public Double evaluate(Vector x) {		
				return Optimizer.costScaling.evaluateObjective(x.toArray());
			}

        };

        // Equality Constraint
        RealScalarFunction equalityConstraints = new RealScalarFunction() {
            public int dimensionOfDomain() {
                return Optimizer.dimension;
            }

            public int dimensionOfRange() {
                return 1;
            }

			public Double evaluate(Vector x) {
                return Optimizer.costScaling.evaluateConstraint(x.toArray());
            }

        };

        HashSet inEqualityConstraints = new HashSet();
        // Create dimension number of in-equality constraint for the lower bound.
        for(int d=0;d<dimension;d++) {
            RealScalarFunction lowerBound = new RealScalarFunction() {
                public int dimensionOfDomain() {
                    return Optimizer.dimension;
                }

                public int dimensionOfRange() {
                    return 1;
                }

				@Override
				public Double evaluate(Vector x) {
					double[] y = x.toArray();
					return 1 - y[Optimizer.getIndex(this)];
                }

            };
            map.put(lowerBound, d);
            inEqualityConstraints.add(lowerBound);
        }

        // Add for Lemma 2 Constraints, if any.
        Set<String> keys = Optimizer.costScaling.lemma2Constraints.keySet();
        Iterator kiter = keys.iterator();
        while(kiter.hasNext())
        {
            String relation = (String)kiter.next();
            RealScalarFunction lemma2Bound = new RealScalarFunction() {
                public int dimensionOfDomain() {
                    return Optimizer.dimension;
                }

                public int dimensionOfRange() {
                    return 1;
                }

				@Override
				public Double evaluate(Vector x) {
					return Optimizer.costScaling.evaluteLemma2Constraint(Optimizer.getLemma2Relation(this), x.toArray());
                }
            };
            mapLemma2.put(lemma2Bound, relation);
            inEqualityConstraints.add(lemma2Bound);
        }

        ConstrainedOptimProblemImpl1 problem = new ConstrainedOptimProblemImpl1(f,
                new GeneralEqualityConstraints(equalityConstraints),
                new GeneralLessThanConstraints(inEqualityConstraints)
                );

        SQPActiveSetMinimizer optim = new SQPActiveSetMinimizer(tolerance, iterations);

        //We have a class named Solution. So have to use entire path instead of importing this.
        com.numericalmethod.suanshu.optimization.multivariate.constrained.general.sqp.activeset.SQPActiveSetMinimizer.Solution sol = optim.solve(problem); // problem, tolerance, iterations

        // SQPArg2 - #Equality  Constraints : 1
        SQPArg2 = new double[1]; SQPArg2[0] = Constants.SQLArg2_ElementValue;
        // SQPArg2 - # In Equality  Constraints : Dimension + lemma2Bound Size
        SQPArg3 = new double[inEqualityConstraints.size()];
        for(int i=0;i<SQPArg3.length;i++) {
            SQPArg3[i] = Constants.SQLArg3_ElementValue;
        }

        long start = System.currentTimeMillis();
        Vector xmin = sol.search(new DenseVector(initialPoint), new DenseVector(SQPArg2), new DenseVector(SQPArg3));

         // Vector xmin = optim.search(new DenseVector(initialPoint), new DenseVector(-1., -1.), new DenseVector(1., 1., 1., 1.));
        /*
        PenaltyMethodMinimizer optim = new PenaltyMethodMinimizer(PenaltyMethodMinimizer.DEFAULT_PENALTY_FUNCTION_FACTORY, 1e30, new BFGS());
        optim.solve(problem, tolerance, iterations); // problem, tolerance, iterations
        Vector xmin = optim.search(new DenseVector(initialPoint));
         *
         */

        // Following argument commented by deepali
        double fxmin = f.evaluate(xmin);
        long end = System.currentTimeMillis();
        String solnAndFnValue = String.format("f(%s) = %f", xmin.toArray().toString(), fxmin);
        long elapsedtime = ((end-start) / 1000); // milliseconds to seconds
        String time;
        if(elapsedtime < 1000 ) {
            time = " [in "+elapsedtime+" seconds]";
        } else {
            elapsedtime = (elapsedtime / 1000); // seconds to mins
            time = " [in "+elapsedtime+" mins]";
        }
        Constants.CPrintToConsole(solnAndFnValue+time, Constants.DEBUG_FIRST_LEVEL_Information);
        return new Solution(xmin.toArray(),fxmin);
    }
}

/**
 * Solution of Optimization problem is represented by this class.
 * @author dsladmin
 */
class Solution {
    private int dimension;
    private int[] soln;
    private double objectiveFunctionValue;

    public Solution(double[] soln, double fnValue) {
        //System.out.println("solution class");
        this.dimension = soln.length;
        this.soln = new int[dimension];
        for (int i = 0; i < dimension; i++) {
            int sf_rel = (int) Math.round(soln[i]);
            this.soln[i] = sf_rel;
        }
        this.objectiveFunctionValue = fnValue;
    }

    public String getSolnString() {
        String retString = new String();
        for(int i=0; i<dimension;i++) {
            retString = retString+soln[i];
            if( i < dimension-1) {
                retString = retString+"-";
            }
        }
        return retString;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public double getObjectiveFunctionValue() {
        return objectiveFunctionValue;
    }

    public void setObjectiveFunctionValue(double objectiveFunctionValue) {
        this.objectiveFunctionValue = objectiveFunctionValue;
    }

    public int[] getSoln() {
        return soln;
    }

    public void setSoln(int[] soln) {
        this.soln = soln;
    }

    @Override
    public String toString() {
        String retString = new String();
        retString = retString+" (";
        for(int i=0; i<dimension;i++) {
            retString = retString+soln[i];
            if( i < dimension-1) {
                retString = retString+", ";
            }
        }
        retString = retString+" ) ObjVal: "+this.objectiveFunctionValue;
        return retString;
    }
}