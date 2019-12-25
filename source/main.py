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
	('numpy','np'),'scipy',	'sklearn',
	('tensorflow','tf'),'h5py','keras']
for i in ls:
	if not isinstance(i,tuple):
		i = (i,)
    exec('import {0} as {1}'.format(i[0],i[-1]))
    exec('print("Version of {0}",{1}.__version__)'.format(i[0],i[-1]))

######## STANDARD & THIRD PARTY LIBRARIES IMPORTS ENDS ########



######## GLOBAL INITIAL VARIABLES BEGINS ########

parser = argparse.ArgumentParser()

# Bool Type Arguments
parser.add_argument("--zero_sel"   , type=eval , dest='zero_sel'   , default=False)
parser.add_argument("--new_plans"  , type=eval , dest='new_plans'  , default=False)
# Int Type Arguments
parser.add_argument("--resolution" , type=eval , dest='resolution' , default=10)
parser.add_argument("--sel_round"  , type=eval , dest='sel_round'  , default=None)
# Float Type Arguments
parser.add_argument("--min_sel"         , type=eval , dest='min_sel'         , default=0.0001) # Least sel out of 1
parser.add_argument("--max_sel"         , type=eval , dest='max_sel'         , default=1.0) # Maximum sel of 1
parser.add_argument("--anorexic_lambda" , type=eval , dest='anorexic_lambda' , default=0.2) # Cost Slack
# String Type Arguments
parser.add_argument("--benchmark"  , type=str  , dest='benchmark'  , default='TPC-DS')
parser.add_argument("--master_dir" , type=str  , dest='master_dir' , default=os.path.join('.','..','experiments','bouquet_master' ))
parser.add_argument("--plots_dir"  , type=str  , dest='plots_dir'  , default=os.path.join('.','..','experiments','bouquet_plots'))

# Tuple Type Arguments
parser.add_argument("--channel_indx" , type=eval , dest='channel_indx' , default=(0,1,2))

args, unknown = parser.parse_known_args()
globals().update(args.__dict__)



plan_map = {} # (query, sel)       : PLAN_ID  , Getting ID of optimal plan at given Selectivity
fpc_map  = {} # (query, plan, sel) : COST_VAL , FPC outputs, Evaluates a Plan at Selectivity value

IC2plan_map = {} # (query,IC)   : LIST_PLANS , list of plans on iso-cost surface
plan2IC_map = {} # (query,plan) : CONTOUR_ID , iso-cost contour id of any plan
IC2cost_map = {} # (query,IC)   : Cost budget of iso-cost surface

######## GLOBAL INITIAL VARIABLES ENDS ########




######## PERFORMANCE METRICS BEGINS ########

def cost(query, plan, sel):
	"Costs plan at some selectivity value"
	if (query, plan, sel) not in fpc_map:
		'# Code for FPC call to cost plan'
		pass
	return fpc_map[ (query, plan, sel) ]

def SubOpt(query, est_sel, act_sel, bouquet=False):
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

######## PERFORMANCE METRICS BEGINS ########




######## PLAN BOUQUET BEGINS ########

def cover(sel_1, sel_2):
	"Check if one region covers other":
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



if __name__=='__main__':
	sel_ratio = np.exp( np.log(max_sel/min_sel) / (resolution-1) )
	sel_range = [(min_sel*sel_ratio**i) for i in range(resolution)]
	if zero_sel:
		sel_range.insert( 0, 0 )
	sel_range = np.array( sel_range )
	if sel_round is not None:
		sel_range = np.round( sel_range, sel_round )
