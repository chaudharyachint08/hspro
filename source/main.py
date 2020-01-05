'Basic Bouquet Implementation - Achint Chaudhary'

'''
Syntax for Seletivity Injection
explain sel (' and '.join(PREDICATE_LIST))(' , '.join(SELECTIVITY_LIST)) SQL_QUERY

Syntax for Plan Enforcing
SQL_QUERY fpc XML_PLAN_PATH



.
├── bouquet_master
│   ├── tpcds
│   │   ├── sql
│   │   │   ├── 1.sql
│   │   │   ├── 2.sql
│   │   │   └── 3.sql
│   │   ├── epp
│   │   │   ├── 1.epp
│   │   │   ├── 2.epp
│   │   │   └── 3.epp
│   │   └── plans
│   │       ├── json
│   │       │   ├── 1
│   │       │   │   ├── 1.json
│   │       │   │   └── 2.json
│   │       │   ├── 2
│   │       │   │   ├── 1.json
│   │       │   │   ├── 2.json
│   │       │   │   └── 3.json
│   │       │   └── 3
│   │       │       └── 1.json
│   │       └── xml
│   │           ├── 1
│   │           │   ├── 1.xml
│   │           │   └── 2.xml
│   │           ├── 2
│   │           │   ├── 1.xml
│   │           │   ├── 2.xml
│   │           │   └── 3.xml
│   │           └── 3
│   │               └── 1.xml
│   └── tpch
│       │
│       *
└── bouquet_plots
│   │
│   *
└── source
    ├── main.py
    └── plan_format.py
'''


######## STANDARD & THIRD PARTY LIBRARIES IMPORTS BEGINS ########

# Importing Standard Python Libraries
ls = ['math','os','sys','argparse','datetime','shutil','itertools','struct','gc','warnings','pickle','multiprocessing','json']
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

# Importing DBMS libraries, and custom libraries
import psycopg2
import plan_format as pf

######## STANDARD & THIRD PARTY LIBRARIES IMPORTS ENDS ########


######## COMMAND LINE ARGUMENTS BEGINS ########

parser = argparse.ArgumentParser()

# Bool Type Arguments
parser.add_argument("--zero_sel" , type=eval , dest='zero_sel' , default=False)
parser.add_argument("--new_plan" , type=eval , dest='new_plan' , default=False)
parser.add_argument("--anorexic" , type=eval , dest='anorexic' , default=False)
parser.add_argument("--covering" , type=eval , dest='covering' , default=False)
parser.add_argument("--random_p" , type=eval , dest='random_p' , default=False)
parser.add_argument("--random_c" , type=eval , dest='random_c' , default=False)
# Int Type Arguments
parser.add_argument("--CPU"        , type=eval , dest='CPU'        , default=10)
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
parser.add_argument("--master_dir"  , type=str  , dest='master_dir'  , default=os.path.join('.','..','bouquet_master' ))
parser.add_argument("--plots_dir"   , type=str  , dest='plots_dir'   , default=os.path.join('.','..','bouquet_plots'  ))
# Tuple Type Arguments
parser.add_argument("--db_scales" , type=eval , dest='db_scales' , default=(1,2,5,10,20,30,40,50,75,100,125,150,200,250))

args, unknown = parser.parse_known_args()
globals().update(args.__dict__)

######## COMMAND LINE ARGUMENTS ENDS ########


######## GLOBAL DATA-STRUCTURES BEGINS ########

pwd = os.getcwd()
# eval( json["QUERY PLAN"][0]["Plan"]["Total-Cost"] )

######## GLOBAL DATA-STRUCTURES ENDS ########


class ScaleVariablePlanBouquet:
    ""
    def save_dict(self, dict_obj, file_name):
		"Serialize data into file"
		json.dump( dict_obj, open(file_name,'w') )
	def load_dict(self, file_name)
		"Read data from file"
		return json.load( open(file_name) )

	def plan_serial_helper(self,json_obj):
		"Recursive helper function for plan_serial"
		# CHECKPOINT : write code here for JSON to serial string conversion
		return 'PLAN_STRING_REPRESENTATION'
    def plan_serial(self,xml_plan_string):
        "parsing function to convert plan object (XML/JSON) in to a string"
        json_obj = pf.xml2json(res,mode='string')
        plan_string = self.plan_serial_helper( json_obj["QUERY PLAN"][0] )        
        return plan_string

    def __init__(self,benchmark,query_id,base_scale,db_scales,stderr):
        "query is nothing but an integer, which is same as ID of epp file also"
        self.stderr, self.benchmark, self.query_id, self.base_scale, self.db_scales = stderr, benchmark, query_id, base_scale, db_scales
        with open(os.path.join(master_dir,self.benchmark,'sql','{}.sql'.format(query_id))) as f:
            self.query = f.read().strip()
        with open(os.path.join(master_dir,self.benchmark,'epp','{}.epp'.format(query_id))) as f:
            self.epp = []
            for line in f.readlines():
                line = line.strip()
                if line: # Using 1st part, second part imply nature of cost with (increasing/decreasing/none) selectivity
                    self.epp.append( line.split('|')[0].strip() )
        
        'Naming conventions for maps'
        # [c,d,f,i,p,r,s] are [cost, base_scale, file_name(without_extension), IC_id, plan_id, representation_plan(serial string), selectivity]
        self.r2f_m = {} # (plan_string)     : file_name(without extension)
        self.f2p_m = {} # (file_name)       : plan_id
        self.p2r_m = {} # (plan_id)         : representation_plan(serial string)
        # Above 3 have cyclic, and any of them can be used to derive any other, using either one or two maps
        self.sd2p_m   = {} # (sel,scale)      : plan_id , (returns optimal plan_id at sel value for given scale of benchmark)
        self.spd2c_m  = {} # (sel,plan,scale) : abstract_cost value of plan at some selectivity for given scale of benchmark (FPC mapping)
        # Maps for Iso-cost surfaces
        self.id2p_m     = {} # (IC_id, scale)   : list of plans on iso-cost surface for given scale
        self.pd2i_m     = {} # (plan_id, scale) : IC_id , iso-cost contour id of any plan at some given scale
        self.id2c_m     = {} # (IC_id,scale)    : abstract_cost, cost budget of IC_id surface for given scale

    def get_plan(self, sel, scale):
        "finding optimal plan at some seletivity value"
        try:
            connection = psycopg2.connect(user = "sa",
                                          password = "database",
                                          host = "127.0.0.1",
                                          port = "5432",
                                          database = "{}-{}".format(self.benchmark,scale))
            cursor = connection.cursor()
            epp_list = ' and '.join(str(x) for x in self.epp)
            sel_list = ' , '.join(str(x) for x in sel)
            cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {};'.format(epp_list,sel_list,self.query)  )
            res = cursor.fetchone()[0]
        except (Exception, psycopg2.DatabaseError) as error :
            # MultiThreaded Logging of Exception
            print ("Error while connecting to PostgreSQL", error,file=self.stderr,flush=True)
            res = None
        else:
            pass
        finally:
            if connection:
                cursor.close()
                connection.close()
        return res

    def get_cost(self, sel, plan, scale):
        "FPC of plan at some other selectivity value"
        try:
            connection = psycopg2.connect(user = "sa",
                                          password = "database",
                                          host = "127.0.0.1",
                                          port = "5432",
                                          database = "{}-{}".format(self.benchmark,scale))
            cursor = connection.cursor()
            epp_list  = ' and '.join(str(x) for x in self.epp)
            sel_list  = ' , '.join(str(x) for x in sel)
            sep = os.path.sep
            plan_path = os.path.join( *pwd.split(sep)[:-1],master_dir.split(sep)[-1],self.benchmark,'plans','xml', self.query_id, '{}.xml'.format(self.r2f_m[self.p2r_m[plan]]) )
            cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {} FPC {};'.format(epp_list,sel_list,self.query,plan_path)  )
            res = cursor.fetchone()[0]
        except (Exception, psycopg2.DatabaseError) as error :
            # MultiThreaded Logging of Exception
            print ("Error while connecting to PostgreSQL", error,file=self.stderr,flush=True)
            res = None
        else:
            json_obj = pf.xml2json(res,mode='string')
            res = json_obj["QUERY PLAN"][0]["Plan"]["Total-Cost"]
        finally:
            if connection:
                cursor.close()
                connection.close()
        return res

    ######## PERFORMANCE METRICS BEGINS ########

    def cost(self, sel, plan, scale=None):
    	"Costs plan at some selectivity value"
        scale = scale if (scale is not None) else self.base_scale
        if (sel, plan, scale) not in self.spd2c_m:
            '# Code for FPC call to cost plan'
            self.spd2c_m[ (sel, plan, scale) ] = self.get_cost(sel, plan, scale)
        return self.spd2c_m[ (sel, plan, scale) ]
    def SubOpt(self, act_sel, est_sel, scale=None, bouquet=False):
        "Ratio of plan on estimated sel to plan on actual sel"
        scale = scale if (scale is not None) else self.base_scale
        act_plan = self.sd2p_m[ (act_sel) ]
        if not bouquet:
            'Classic Optimizer Style Metric'
            est_plan = self.sd2p_m[ (est_sel, scale) ]
            return self.cost(act_sel, est_plan, scale) / self.cost(act_sel, act_plan, scale)
        else:
            'Bouquet Style Metric'
            exec_list = self.get_bouquet_exec_list(act_sel, scale)
            bouquet_cost = 0
            for plan in exec_list[:-1]: # Since up-to second last plan, budget based execution
            	bouquet_cost += self.id2c_m[ (self.pd2i_m[(plan,scale)], scale) ]
            bouquet_cost += self.cost(act_sel, exec_list[-1], scale)
            return bouquet_cost / self.cost(act_sel, act_plan, scale)
    def WorstSubOpt(self, act_sel, scale=None):
        "SubOpt for all possible est_selectivities"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        return max( self.SubOpt(act_sel, est_sel, scale) for est_sel in epp_iterator )
    def MaxSubOpt(self, scale=None, bouquet=False):
        "Global worst case"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        if not bouquet:
            'Classic Optimizer Style Metric'
            return max( self.WorstSubOpt(act_sel, scale) for act_sel in epp_iterator )
        else:
            'Bouquet Style Metric'
            return max( self.SubOpt(act_sel, -1, scale, bouquet=True) for act_sel in epp_iterator )
    def AvgSubOpt(self, scale=None, bouquet=False):
        "Average suboptimality over ESS under uniformity assumption"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        if not bouquet:
            'Classic Optimizer Style Metric'
            epp_iterator2 = itertools.product(*([sel_range,]*len(self.epp)))
            return sum( (self.SubOpt(act_sel, est_sel) for est_sel in epp_iterator for act_sel in epp_iterator) ) / ((len(sel_range)**len(self.epp))**2)
        else:
            'Bouquet Style Metric'
            return sum( (self.SubOpt(act_sel, est_sel) for act_sel in epp_iterator) ) / (len(sel_range)**len(self.epp))
    def MaxHarm(self, scale=None):
        "Harm using Plan Bouquet, over Classic Optimizer"
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        return max(  ( self.SubOpt(act_sel,-1,scale,bouquet=True) / self.WorstSubOpt(act_sel,scale) ) for act_sel in epp_iterator  ) - 1

    ######## PERFORMANCE METRICS ENDS ########


    def base_gen(self):
        "Step 0: Find cost values for each iso-cost surface"
        pass

    def nexus(self):
        "Step 1: Find Plans on each Iso-cost surface"
        pass

    def anorexic_reduction(self):
        "Reduing overall number of plans, effectively reducing plan density on each iso-cost surface"
        pass

    def cover(self,sel_1, sel_2):
        "Check if one region covers other"
        ls_1, ls_2 = np.array(sel_1), np.array(sel_2)
        if   (ls_1 <= ls_2).all(): # ls_1 is covered by ls_2
            return 1
        elif (ls_1 >= ls_2).all(): # ls_1 covers ls_2
            return -1
        else:
            return 0

    def covering_sequence(self):
        "Step 3: Find Plans on each Iso-cost surface"
        pass

    def simulate(self):
        "Simulating Plan-Bouquet Execution under Idea Cost model assumption"
        pass

    def run(self):
        ""
        self.base_gen()
        self.nexus()
        if anorexic:
            self.anorexic_reduction()
        if covering:
            self.covering_sequence()
        self.simulate()



######## PLAN BOUQUET ENDS ########


if __name__=='__main2__':

    def run(object):
        "Simple Execution for PlanBouquet objects"
        object.run()

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

    obj_ls, stderr = [], fopen(os.path.join(master_dir,'stderr.txt'),'w')
    for query_name in os.listdir( os.path.join(master_dir,benchmark,'sql') ):
        query_id = query_name.split('.')[0].strip()
        obj_ls.append( ScaleVariablePlanBouquet(benchmark,query_id,base_scale,db_scales,stderr) )


    with multiprocessing.Pool(processes=CPU) as pool:
        for i in pool.imap_unordered(run,obj_ls):
            continue

    stderr.close()