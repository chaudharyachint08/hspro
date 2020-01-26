# hspro


## To-Do List
<pre>
#### ANOREXIC REDUCTION ####
Anorexic Reduction not as mapping, a plan-optimal regions can be jointly swallowed by two plans
Seperate cost threshold for each execution, as anorexic reduction might have happended or not.
Cost value of contour not always shifted by (1+Anorexic_Lambda)*cost[IC_id]


#### COVERING SEQUENCE IDENTIFICATION ####
CSI is just mapping from one execution to another which is cover of what of keyed execution
On Demand CSI invocation if Randomization 2 is deployed, for which is random_p_d CSI are there, into joint mapping
During CSI cost thresholds


#### Randomization 2 ####
Different CSI Invocation will be needed, each with m+1 for others, & m IC-contours for no-left shift placement of contours
each will be on-demand execution with saving of cover into maps of form (Execution -> Execution)

#### Randomization 1 ####
Shuffle of executions on each contour

#### PERFORMANCE METRICS ####
Below maps to be added for plan choice decision making
(Scale -> POSP)
For MSO or Any-performance metric, RED**|ESS| points would be needed
Plan_Bouquet compilation can be done at higher resolution
While MSO & Other peformance metrics should be done on lower dimesions, as exponential bombing of number of points to be evaluated

#### SIMULATION ####
Boolean Zagged array for only once execution during simulation
MSO_k also evaluated at same time for Bouquet Execution (m or m+1 contours, depending upon R2 variable)
</pre>


## Basic Implementation Idea
<pre>
	0. BEGIN : Preparatory ground for nexus exeution
	1. NEXUS : Get All Plans on Each of Iso-cost surfaces
	2. Anorexic Reduction : Plan swallowing for reducing plan density on each surface
	3. Covering sequence : Further Reduction of plan density on each surface, (check if impacts ASO)
	4. Execution of bouquet, or simulation of execution under ideal cost model assumption
		a. Execution will return list of plan executions, last of which will discover correct selectivity
		b. Sec 4.1 Randomization, by appropriately randomizing & slicing list of last IC-surface on which selectivity is discovered
		c. Sec 4.2 Randomization, by Additional data-structure in map for randomized contour placement of IC-surfaces is needed

	Also, values of selectivities of ESS grid in all papers are in Geometric progression 

During implementation also, have you considered GP, or A.P. instead, and how should I decide upon minimum selectivity value to consider, as if I have to take selectivity values in G.P., minimum value will have some impact
And, have you also considered absolute 0 selectivity of all EPP in implementation
</pre>

