# hspro


<pre>
	0. BEGIN : Preparatory ground for nexus exeution
	1. NEXUS : Get All Plans on Each of Iso-cost surfaces (multi-threaded without GIL lock)
	2. Anorexic Reduction : Plan swallowing for reducing plan density on each surface
	3. Covering sequence : Further Reduction of plan density on each surface, (check if impacts ASO)
	4. Execution of bouquet, or simulation of execution under ideal cost model assumption

	Also, values of selectivities of ESS grid in all papers are in Geometric progression 

During implementation also, have you considered GP, or A.P. instead, and how should I decide upon minimum selectivity value to consider, as if I have to take selectivity values in G.P., minimum value will have some impact
And, have you also considered absolute 0 selectivity of all EPP in implementation
</pre>