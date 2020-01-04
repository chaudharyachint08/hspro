'Basic Bouquet Implementation - Achint Chaudhary'

'''
Syntax for Seletivity Injection
explain sel (' and '.join(PREDICATE_LIST))(' , '.join(SELECTIVITY_LIST)) SQL_QUERY

Syntax for Plan Enforcing
SQL_QUERY fpc XML_PLAN_PATH
'''


######## STANDARD & THIRD PARTY LIBRARIES IMPORTS BEGINS ########

# Importing Standard Python Libraries
ls = ['math','os','sys','argparse','datetime','shutil','itertools','struct','gc','warnings','pickle']
for i in ls:
    exec('import {0}'.format(i))
    #exec('print("imported {0}")'.format(i))

from datetime import datetime
warnings.filterwarnings("ignore")
os.environ['KERAS_BACKEND'] = 'tensorflow'

# Importing Standard Data Science & Deep Learning Libraries
ls = [('matplotlib','mpl'),('matplotlib.pyplot','plt'),('seaborn','sns'),
    ('numpy','np'),'scipy',    'sklearn',
    ('tensorflow','tf'),'h5py','keras']
for i in ls:
    if not isinstance(i,tuple):
        i = (i,)
    exec('import {0} as {1}'.format(i[0],i[-1]))
    exec('print("Version of {0}",{1}.__version__)'.format(i[0],i[-1]))

######## STANDARD & THIRD PARTY LIBRARIES IMPORTS ENDS ########



######## COMMAND LINE ARGUMENTS BEGINS ########

parser = argparse.ArgumentParser()

# Bool Type Arguments
parser.add_argument("--zero_sel" , type=eval , dest='zero_sel' , default=False)
parser.add_argument("--new_plan" , type=eval , dest='new_plan' , default=False)
parser.add_argument("--anorexic" , type=eval , dest='anorexic' , default=False)
parser.add_argument("--covering" , type=eval , dest='covering' , default=False)

# Int Type Arguments
parser.add_argument("--resolution" , type=eval , dest='resolution' , default=1000)
parser.add_argument("--base_scale" , type=eval , dest='base_scale' , default=1)
parser.add_argument("--sel_round"  , type=eval , dest='sel_round'  , default=None)
# Float Type Arguments
parser.add_argument("--r_ratio"         , type=eval , dest='r_ratio'         , default=2.0) # IC cost ratio for bouquet
parser.add_argument("--min_sel"         , type=eval , dest='min_sel'         , default=0.0001) # Least sel out of 1
parser.add_argument("--max_sel"         , type=eval , dest='max_sel'         , default=1.0) # Maximum sel of 1
parser.add_argument("--anorexic_lambda" , type=eval , dest='anorexic_lambda' , default=0.2) # Cost Slack
parser.add_argument("--nexus_tolerance" , type=eval , dest='nexus_tolerance' , default=0.05) # fir q-points in discretized planes
# String Type Arguments
parser.add_argument("--progression" , type=str  , dest='progression' , default='AP')
parser.add_argument("--benchmark"   , type=str  , dest='benchmark'   , default='tpcds')
parser.add_argument("--master_dir"  , type=str  , dest='master_dir'  , default=os.path.join('.','..','experiments','bouquet_master' ))
parser.add_argument("--plots_dir"   , type=str  , dest='plots_dir'   , default=os.path.join('.','..','experiments','bouquet_plots'))
# Tuple Type Arguments
parser.add_argument("--db_scales" , type=eval , dest='db_scales' , default=(1,2,5,10,20,30,40,50,75,100,125,150,200,250))

args, unknown = parser.parse_known_args()
globals().update(args.__dict__)

######## COMMAND LINE ARGUMENTS ENDS ########


######## GLOBAL DATA-STRUCTURES BEGINS ########

pwd = os.getcwd()

######## GLOBAL DATA-STRUCTURES ENDS ########


eval( json["QUERY PLAN"][0]["Plan"]["Startup-Cost"] )
eval( json["QUERY PLAN"][0]["Plan"]["Total-Cost"] )


class ScaleVariablePlanBouquet:
	''
	'''
	'''

	def __init__(self,benchmark,query,base_scale,db_scales):
		self.benchmark, self.query, self.base_scale, self.db_scales = benchmark, query, base_scale, db_scales
		self.struct2file_map = {} # (serial_plan) : Plan_Structure
		self.file2plan_map   = {} # (plan_file_name)   : provides plan_if for given plan file name, without extension
		self.plan2file_map   = {} # (plan_id)     : provides plan_if for given plan path

		self.sel2plan_map    = {} # (sel) : Getting ID of optimal plan at given selectivity
		self.fpc_map  = {} # (plan, sel) : COST_VAL of some plan at some selectivity

		IC2plan_map = {} # (benchmark,query,IC)   : LIST_PLANS , list of plans on iso-cost surface
		plan2IC_map = {} # (benchmark,query,plan) : CONTOUR_ID , iso-cost contour id of any plan
		IC2cost_map = {} # (benchmark,query,IC)   : Cost budget of iso-cost surface

	def plan_serial(plan_path):
		"Function to convert info providing plan ID to a string"
		pass

	######## PERFORMANCE METRICS BEGINS ########

	def cost(plan, sel):
	    "Costs plan at some selectivity value"
	    if (query, plan, sel) not in fpc_map:
	        '# Code for FPC call to cost plan'
	        pass
	    return fpc_map[ (query, plan, sel) ]

	def SubOpt(benchmark, query, est_sel, act_sel, bouquet=False):
	    "Ratio of plan on estimated sel to plan on actual sel"
	    act_plan = plan_map[ (query, act_sel) ]
	    if not bouquet:
	        'Classic Optimizer Style Metric'
	        est_plan = plan_map[ (query, est_sel) ]
	        return cost(query, est_plan, act_sel) / cost(query, act_plan, act_sel)
	    else:
	        'Bouquet Style Metric'
	        plan_list = ExecPlanBouquet(query, act_sel)
	        bouquet_cost = 0
	        for plan in plan_list[:-1]: # Since up-to second last plan, budget based execution
	            bouquet_cost += IC2cost_map[ (query, plan2IC_map[ (query,plan) ] ) ]
	        bouquet_cost += cost(query, est_plan, act_sel)
	        return bouquet_cost / cost(query, act_plan, act_sel)
	        pass

	def WorstSubOpt(query, act_sel):
	    "SubOpt for all possible est_selectivities"
	    return max( SubOpt(query, est_sel, act_sel) for est_sel in sel_range)

	def MaxSubOpt(query):
	    "Global worst case"
	    return max( WorstSubOpt(query, act_sel) for act_sel in sel_range)

	def AvgSubOpt(query):
	    "Average suboptimality over ESS under uniformity assumption"
	    return sum( (SubOpt(query, est_sel, act_sel) for est_sel in sel_range for act_sel in sel_range) ) / (len(sel_range)**2)

	def MaxHarm(query):
	    "Harm using Plan Bouquet, over Classic Optimizer"
	    return max( (SubOpt(query,est_sel=-1,act_sel,bouquet=True)/WorstSubOpt(query, act_sel)) for act_sel in sel_range ) - 1

	def cover(sel_1, sel_2):
	    "Check if one region covers other"
	    ls_1, ls_2 = np.array(sel_1), np.array(sel_2)
	    if   (ls_1 <= ls_2).all(): # ls_1 is covered by ls_2
	        return 1
	    elif (ls_1 >= ls_2).all(): # ls_1 covers ls_2
	        return -1
	    else:
	        return 0

	def anorexic_reduction(query):
	    pass

	def covering_sequence(query):
	    pass


######## PLAN BOUQUET ENDS ########

if not os.path.isdir(master_dir):
    os.makedirs(master_dir)
if not os.path.isdir(plots_dir):
    os.makedirs(plots_dir)



if __name__=='__fuck__':

    # Creating values of selectivities on each axis to be varied
    if progression=='GP':
        if zero_sel:
            resolution -= 1
        sel_ratio = np.exp( np.log(max_sel/min_sel) / (resolution-1) )
        sel_range = [(min_sel*sel_ratio**i) for i in range(resolution)]
        if zero_sel:
            sel_range.insert( 0, 0 )
    else:
        sel_diff = max_sel / (resolution-1)
        sel_range = [ i*sel_diff for i in range(resolution)]
    sel_range = np.array( sel_range )
    if sel_round is not None:
        sel_range = np.round( sel_range, sel_round )

    'Step 0: Find cost values for each iso-cost surface'
    '''
    '''

    'Step 1: Find Plans on each Iso-cost surface'
    '''
    '''
    nexus()

    'Step 2: Reduing overall number of plans, effectively reducing plan density on each iso-cost surface'
    '''
    '''
    if anorexic:
        anorexic_reduction()

    'Step 3: Find Plans on each Iso-cost surface'
    '''
    '''
    if covering:
        covering_sequence()

    'Step 4: Simulating Plan-Bouquet Execution under Idea Cost model assumption'
    '''
    '''
    simulate()



