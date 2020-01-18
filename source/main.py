'Basic Bouquet Implementation - Achint Chaudhary'

'''
Syntax for Seletivity Injection
explain sel (' and '.join(PREDICATE_LIST))(' , '.join(SELECTIVITY_LIST)) SQL_QUERY

Syntax for Plan Enforcing
SQL_QUERY fpc XML_PLAN_PATH


hspro (Home Directory of Project) # CHECKPOINT, next directory should be r_ratio value, in which all maps for re-excution can be stored
.
├── bouquet_master
│   ├── tpcds
│   │   ├── maps
│   │   │   ├── 1
│   │   │   ├── 2
│   │   │   └── 3
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
├── bouquet_plots
│   │
│   *
└── source
    ├── main.py
    └── plan_format.py
'''


######## STANDARD & THIRD PARTY LIBRARIES IMPORTS BEGINS ########

# Importing Standard Python Libraries
ls = ['math','os','sys','argparse','copy','datetime','shutil','itertools','struct','gc','warnings','pickle','json','multiprocessing','threading']
for i in ls:
    exec('import {0}'.format(i))
    #exec('print("imported {0}")'.format(i))

from datetime import datetime
warnings.filterwarnings("ignore")

# Importing DBMS libraries, and custom made libraries
import psycopg2
import plan_format as pf


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
parser.add_argument("--random_s" , type=eval , dest='random_s' , default=False) # Flag for Sec 4.1 Randomized Sequence of Iso-Contour Plans
parser.add_argument("--random_p" , type=eval , dest='random_p' , default=False) # Flag for Sec 4.2 Randomized Placement of Iso-Contours (with discretization)
# Int Type Arguments
parser.add_argument("--CPU"        , type=eval , dest='CPU'        , default=10)
parser.add_argument("--resolution" , type=eval , dest='resolution' , default=100)
parser.add_argument("--base_scale" , type=eval , dest='base_scale' , default=1)
parser.add_argument("--random_p_d" , type=eval , dest='random_p_d' , default=2) # Discretization parameter for shifting of Iso-cost contours, (always power of 2)
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
sep = os.path.sep
val = pwd.split(sep)
if 'source' in val:
    home_path = val[:-1]
else:
    home_path = val
    master_ls = master_dir.split(sep)
    plots_ls  = plots_dir.split(sep)
    try:
        master_ls.remove('..')
        plots_ls.remove('..')
    except:
        pass
    master_dir = os.path.join(master_ls)    
    plots_dir  = os.path.join(plots_ls)
# creating a lock for os_operations
os_lock = threading.Lock()
# eval( json["QUERY PLAN"][0]["Plan"]["Total-Cost"] )

######## GLOBAL DATA-STRUCTURES ENDS ########

######## GLOBAL FUNCTIONS BEGINS ########

def my_listdir(dir_path='.'):
    "Synchronization construct based listing of files in directory"
    os_lock.acquire()
    file_ls = os.listdir(dir_path)
    os_lock.release()
    return file_ls

def my_open(file_path,mode='r'):
    "Synchronization construct based opening of a file"
    os_lock.acquire()
    file_handle = my_open(file_path,mode)
    os_lock.release()
    return file_handle

def run(object):
    "Simple Execution for PlanBouquet objects"
    object.run()

######## GLOBAL FUNCTIONS ENDS ########


class ScaleVariablePlanBouquet:
    ""
    def __init__(self,benchmark,query_id,base_scale,db_scales,stderr):
        "Instance initializer: query is nothing but an integer, which is same as ID of epp file also"
        self.stderr, self.benchmark, self.query_id, self.base_scale, self.db_scales = stderr, benchmark, query_id, base_scale, db_scales
        self.maps_dir = os.path.join(master_dir,self.benchmark,'maps','{}'.format(query_id))
        with my_open(os.path.join(master_dir,self.benchmark,'sql','{}.sql'.format(query_id))) as f:
            self.query = f.read().strip()
        try:
            with my_open(os.path.join(master_dir,self.benchmark,'epp','{}.epp'.format(query_id))) as f:
                self.epp = []
                for line in f.readlines():
                    line = line.strip()
                    if line: # Using 1st part, second part imply nature of cost with (increasing/decreasing/none) selectivity
                        self.epp.append( line.split('|')[0].strip() )
        except:
            self.bouquet_runnable = False
        else:
            self.bouquet_runnable = True if len(self.epp) else False

        self.exec_specific['random_p_d'] = random_p_d
        self.anorexic_lambda = anorexic_lambda if anorexic else 0.0
        'Naming conventions for maps'
        # [a,c,d,f,i,p,r,s] are [anorexic_lambda, cost, base_scale, file_name(without_extension), IC_id, plan_id, representation_plan(serial string), selectivity]
        # Below 3 have cyclic, and any of them can be used to derive any other, using either one or two maps
        self.r2f_m = {} # (plan_string)     : file_name(without extension)
        self.f2p_m = {} # (file_name)       : plan_id
        self.p2r_m = {} # (plan_id)         : representation_plan(serial string)
        # Maps for Iso-cost surfaces, indexed from 0 as opposite from paper convention (EXPONENTIAL SPACE MAPS, SHOULD BE OPTIONAL)
        self.sd2p_m   = {} # (sel,scale)      : plan_id , (returns optimal plan_id at sel value for given scale of benchmark)
        self.spd2c_m  = {} # (sel,plan,scale) : abstract_cost value of plan at some selectivity for given scale of benchmark (FPC mapping)
        # map related to Iso-Cost contours
        self.id2c_m  = {} # (IC_id, scale) : abstract_cost, cost budget of IC_id surface for given scale
        self.iad2p_m = {} # (IC_id, anorexic_lambda, scale) : set of plans on iso-cost surface for given scale
        # Below map will need exponential space, will be used to send to Anorexic reduction, with diff-IC contour synchronization
        self.ipd2s_m = {} # (IC_id, plan, scale)   : set of selectivity values of each plan on IC surface
        # If anorexic_reduction is disabled, anorexic_lambda is 0.0 and reduction is not called

    ######## DATA-BASE CONNECTION METHODS ########
    
    def get_plan(self, sel, scale=base_scale):
        "finding optimal plan at some seletivity value"
        while True:
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
            except (Exception, psycopg2.DatabaseError) as error : # MultiThreaded Logging of Exception
                print ("Error while connecting to PostgreSQL", error,file=self.stderr,flush=True)
                break_flag = False
            else:
                break_flag = True
            finally:
                if connection:
                    cursor.close()
                    connection.close()
                if break_flag:
                    break
        return res

    def get_cost(self, sel, plan='', scale=base_scale):
        "FPC of plan at some other selectivity value"
        while True:
            try:
                connection = psycopg2.connect(user = "sa",
                                              password = "database",
                                              host = "127.0.0.1",
                                              port = "5432",
                                              database = "{}-{}".format(self.benchmark,scale))
                cursor = connection.cursor()
                epp_list  = ' and '.join(str(x) for x in self.epp)
                sel_list  = ' , '.join(str(x) for x in sel)
                if plan:
                    plan_path = os.path.join( *home_path,master_dir.split(sep)[-1],self.benchmark,'plans','xml', self.query_id, '{}.xml'.format(self.r2f_m[self.p2r_m[plan]]) )
                    cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {} FPC {};'.format(epp_list,sel_list,self.query,plan_path)  )
                else:
                    cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {};'.format(epp_list,sel_list,self.query)  )
                res = cursor.fetchone()[0]
            except (Exception, psycopg2.DatabaseError) as error : # MultiThreaded Logging of Exception
                print ("Error while connecting to PostgreSQL", error,file=self.stderr,flush=True)
                break_flag = False
            else:
                json_obj = pf.xml2json(res,mode='string')
                res = json_obj["QUERY PLAN"][0]["Plan"]["Total-Cost"]
                break_flag = True
            finally:
                if connection:
                    cursor.close()
                    connection.close()
                if break_flag:
                    break
        return res


    ######## PERFORMANCE METRICS METHODS ########

    def cost(self, sel, plan, scale=None):
        "Costs plan at some selectivity value"
        scale = scale if (scale is not None) else self.base_scale
        if (sel, plan, scale) not in self.spd2c_m:
            self.spd2c_m[ (sel, plan, scale) ] = self.get_cost(sel, plan, scale)
        return self.spd2c_m[ (sel, plan, scale) ]
    def SubOpt(self, act_sel, est_sel, scale=None, bouquet=False):
        "Ratio of plan on estimated sel to plan on actual sel"
        scale = scale if (scale is not None) else self.base_scale
        act_plan = self.sd2p_m[ (act_sel, scale) ]
        if not bouquet: # Classic Optimizer Style Metric
            est_plan = self.sd2p_m[ (est_sel, scale) ]
            return self.cost(act_sel, est_plan, scale) / self.cost(act_sel, act_plan, scale)
        else: # Bouquet Style Metric
            bouquet_cost = 0
            exec_list = self.simulate(act_sel, scale)
            for plan in exec_list[:-1]: # Since up-to second last plan, budget based execution
                bouquet_cost += self.id2c_m[ (self.pd2i_m[(plan,scale)], scale) ]
            bouquet_cost += self.cost(act_sel, exec_list[-1], scale)
            return bouquet_cost / self.cost(act_sel, act_plan, scale)
    def WorstSubOpt(self, act_sel, scale=None):
        "SubOpt for all possible est_selectivities"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        return max( self.SubOpt(act_sel, est_sel, scale, bouquet=False) for est_sel in epp_iterator )
    def MaxSubOpt(self, scale=None, bouquet=False):
        "Global worst case"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        if not bouquet: # Classic Optimizer Style Metric
            return max( self.WorstSubOpt(act_sel, scale) for act_sel in epp_iterator )
        else: # Bouquet Style Metric
            return max( self.SubOpt(act_sel, -1, scale, bouquet=True) for act_sel in epp_iterator )
    def AvgSubOpt(self, scale=None, bouquet=False):
        "Average suboptimality over ESS under uniformity assumption"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        if not bouquet: # Classic Optimizer Style Metric
            epp_iterator2 = itertools.product(*([sel_range,]*len(self.epp)))
            return sum( (self.SubOpt(act_sel, est_sel) for est_sel in epp_iterator for act_sel in epp_iterator) ) / ((len(sel_range)**len(self.epp))**2)
        else: # Bouquet Style Metric
            return sum( (self.SubOpt(act_sel, est_sel) for act_sel in epp_iterator) ) / (len(sel_range)**len(self.epp))
    def MaxHarm(self, scale=None):
        "Harm using Plan Bouquet, over Classic Optimizer"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*([sel_range,]*len(self.epp)))
        return max(  ( self.SubOpt(act_sel,-1,scale,bouquet=True) / self.WorstSubOpt(act_sel,scale) ) for act_sel in epp_iterator  ) - 1


    ######## PLAN PROCESSING, SAVING & LOADING METHODS ########

    def plan_serial_helper(self,json_obj):
        "Recursive helper function for plan_serial"
        # CHECKPOINT : write code here for JSON to serial string conversion
        return 'PLAN_STRING_REPRESENTATION'
    def plan_serial(self,xml_plan_string):
        "parsing function to convert plan object (XML/JSON) in to a string"
        json_obj = pf.xml2json(res,mode='string')
        plan_string = self.plan_serial_helper( json_obj["QUERY PLAN"][0] )        
        return plan_string

    def store_plan(self, xml_string):
        "method to store XML & JSON variants on plans in respective directories, and Creating entries in "
        sep = os.path.sep
        # Writing XML plan into suitable plan directory & file name conventions
        xml_plan_path = os.path.join( *pwd.split(sep)[:-1],master_dir.split(sep)[-1],self.benchmark,'plans','xml', self.query_id)
        if not is os.path.dir(xml_plan_path):
            os.makedirs(xml_plan_path)
        with my_open( os.path.join(xml_plan_path,'{}.xml'.format(len(my_listdir(xml_plan_path)))) ,'w') as f:
            f.write( xml_string )
        # Writing JSON plan into suitable plan directory & file name conventions
        json_plan_path = os.path.join( *pwd.split(sep)[:-1],master_dir.split(sep)[-1],self.benchmark,'plans','json', self.query_id)
        if not is os.path.dir(json_plan_path):
            os.makedirs(json_plan_path)
        json_obj = pf.xml2json(xml_string,mode='string')
        len_dir = len(my_listdir(json_plan_path))
        self.save_dict(json_obj, os.path.join(json_plan_path,'{}.json'.format(len_dir)) )
        return len_dir

    ######## MAPS SAVING & RE-LOADING METHODS FOR RECOMPUTATION AVOIDANCE ON DIFFERENT INVOCATIONS ########    

    def save_dict(self, dict_obj, file_name):
        "Serialize data into file"
        json.dump( dict_obj, my_open(file_name,'w') )
    def load_dict(self, file_name)
        "Read data from file"
        return json.load( my_open(file_name) )
    def save_maps(self):
        "Save present maps values into objects for repeatable execution over difference simulation"
        if not os.path.isdir(self.maps_dir):
            os.makedirs(self.maps_dir)
        self.save_dict( self.exec_specific , os.path.join( self.maps_dir,'exec_specific' ) )
        self.save_dict( self.r2f_m         , os.path.join( self.maps_dir,'r2f_m'   ) )
        self.save_dict( self.f2p_m         , os.path.join( self.maps_dir,'f2p_m'   ) )
        self.save_dict( self.p2r_m         , os.path.join( self.maps_dir,'p2r_m'   ) )
        self.save_dict( self.sd2p_m        , os.path.join( self.maps_dir,'sd2p_m'  ) )
        self.save_dict( self.spd2c_m       , os.path.join( self.maps_dir,'spd2c_m' ) )
        self.save_dict( self.iad2p_m       , os.path.join( self.maps_dir,'iad2p_m' ) )
        self.save_dict( self.id2c_m        , os.path.join( self.maps_dir,'id2c_m'  ) )
        self.save_dict( self.ipd2s_m       , os.path.join( self.maps_dir,'ipd2s_m' ) )

    def reindex(self, old_random_p_d, new_random_p_d):
    	"Fucntion for changing Indexes for Iso-cost contours, after different loading"
        incr_ratio = (new_random_p_d//old_random_p_d)
        remap = { x:((x+1)*incr_ratio-1) for x in range(old_random_p_d*self.IC_count) }

    def remap(self, old_map, reindex_m, re_ix=0):
        "Changing map keys, for re-indexing of Iso-cost contours"
        new_map = {}
        tuple_mode = True if type(list(old_map.keys())[0])==tuple else False
        for key in old_map:
            new_map[ ((*key[:re_ix],reindex_m[key],*key[re_ix+1:]) if tuple_mode else reindex_m[key]) ] = old_map[key]
        return new_map

    def load_maps(self):
        "Loads previous maps values into objects for repeatable execution over difference simulation"
        if os.path.isdir(self.maps_dir):
            self.load_dict( self.r2f_m         , os.path.join( self.maps_dir,'r2f_m'   ) )
            self.load_dict( self.f2p_m         , os.path.join( self.maps_dir,'f2p_m'   ) )
            self.load_dict( self.p2r_m         , os.path.join( self.maps_dir,'p2r_m'   ) )
            self.load_dict( self.sd2p_m        , os.path.join( self.maps_dir,'sd2p_m'  ) )
            self.load_dict( self.spd2c_m       , os.path.join( self.maps_dir,'spd2c_m' ) )

            self.load_dict( self.iad2p_m       , os.path.join( self.maps_dir,'iad2p_m' ) )
            self.load_dict( self.id2c_m        , os.path.join( self.maps_dir,'id2c_m'  ) )
            self.load_dict( self.ipd2s_m       , os.path.join( self.maps_dir,'ipd2s_m' ) )

            exec_specific = self.load_dict( os.path.join( self.maps_dir,'exec_specific' ) )
            if self.exec_specific['random_p_d'] != exec_specific['random_p_d']:
                old_val = exec_specific['random_p_d']
                new_val = (self.exec_specific['random_p_d']*exec_specific['random_p_d']) // math.gcd(self.exec_specific['random_p_d'],exec_specific['random_p_d'])
                iad2p_m, id2c_m, ipd2s_m = self.iad2p_m, self.id2c_m, self.ipd2s_m
                reindex_m = self.reindex(self, old_val, new_val)
                self.iad2p_m, self.id2c_m, self.ipd2s_m = self.remap(self.iad2p_m,reindex_m,0), self.remap(self.id2c_m,reindex_m,0), self.remap(self.ipd2s_m,reindex_m,0)
                self.exec_specific['random_p_d'] = new_val

    ######## BOUQUET EXECUTION METHODS ########

    def base_gen(self, scale=None):
        "Step 0: Find cost values for each iso-cost surface"
        scale = scale if (scale is not None) else self.base_scale
        sel_min, sel_max = (sel_range[0],)*len(self.epp), (sel_range[-1],)*len(self.epp)
        # Getting plans at extremas, storing them and serializing them
        plan_min_xml, plan_max_xml = self.get_plan(sel_min, scale), self.get_plan(sel_max, scale)
        plan_min, plan_max = self.store_plan(plan_min_xml), self.store_plan(plan_max_xml)
        plan_min_serial, plan_max_serial = self.plan_serial(plan_min_xml), self.plan_serial(plan_max_xml)
        # Setting map entries for plan_id, plan_file and plan_string representations
        self.r2f_m[plan_min_serial], self.r2f_m[plan_max_serial] = plan_min, plan_max
        self.f2p_m[plan_min], self.f2p_m[plan_max] = plan_min, plan_max
        self.p2r_m[plan_min], self.p2r_m[plan_max] = plan_min_serial, plan_max_serial
        # Setting plan entries for optimal plan at locations and cost values at those location
        self.sd2p_m[(sel_min,scale)], self.sd2p_m[(sel_max,scale)] = plan_min, plan_max
        self.C_min, self.C_max = self.cost(sel_min, plan_min, scale), self.cost(sel_max, plan_max, scale)
        self.spd2c_m[(sel_min,plan_min,scale)], self.spd2c_m[(sel_max,plan_max,scale)] = self.C_min, self.C_max
        # Maps for Iso-cost surfaces, indexed from 0 as opposite from paper convention
        # Also for now only last IC is known and have one optimal plan at it, we can create its entries
        self.IC_count = int( np.floor(  np.log(self.C_max/self.C_min) / np.log(r_ratio)  )+1 )
        for ix in range(self.IC_count):
            self.id2c_m[(ix,scale)] = self.C_max / (r_ratio**(self.IC_count-(ix+1)))
        # Below steps are to be done using NEXUS for other than last contours
        self.iad2p_m[(self.IC_count-1,0.0,scale)]      = {plan_max}
        self.ipd2s_m[(self.IC_count-1,plan_max,scale)] = {sel_max}

    def nexus_2d():
        "2 dimensional"
        pass

    def nexus_helper():
        pass
    def nexus(self):
        "Step 1: Find Plans on each Iso-cost surface"
        '''
        # Locating Initial Seed, Binary Search based edges selection
        Location L(x, y) is included in the contour C if it satisfies the following conditions:
            (a) C ≤ C_opt[L] ≤ (1 + α)C and
            (b) if C_opt[L(x−1)] > C and C_opt[L(y−1) ) > C then c opt (L(−1) ) < C
        # Neighborhood EXploration Using Seed (NEXUS)
        If C_opt[(S(y−1)] < C, then set S = S(x+1) else S = S(y−1)
        The end of this recursive routine is marked by the non-existence of both S(x+1) and S(y−1) in the ESS grid.
        '''
        pass
        if self.anorexic_lambda and self.anorexic_lambda!=0.0:
            self.anorexic_reduction()


    def anorexic_reduction(self):
        "Reduing overall number of plans, effectively reducing plan density on each iso-cost surface"
        pass



    def product_cover(self, sel_1, sel_2, dual=False):
        "Check if either of points in ESS covers each other, +ve if in ascending order"
        ls_1, ls_2 = np.array(sel_1), np.array(sel_2)
        if   (ls_1 <= ls_2).all(): # ls_1 is covered by ls_2
            return 1
        elif dual and (ls_1 >= ls_2).all(): # ls_1 covers ls_2
            return -1
        else:
            return 0

    def region_cover(self, points_1, points_2, dual=False):
        "Checks if two execution's points given, will cover each other, +ve if in ascending order"
        grid_1, grid_2 = np.array(points_1), np.array(points_2)
        min_1, min_2, max_1, max_2 = grid_1.min(axis=0), grid_2.min(axis=0), grid_1.max(axis=0), grid_2.min(axis=0)
        # Check 1 : Computationally easy
        if self.product_cover(max_1, min_2):
            return 1
        if dual and self.product_cover(max_2, min_1):
            return -1
        # Check 2 : Computationally easy
        if not self.product_cover(max_1, max_2):
            return 0
        # Check 3 : Computationally expensive
        # There must exist a point in points_2 for each point in points_1, that cover, which makes point_2 a cover of points_1
        for point_1 in grid_1:
            if not (point_1 <= grid_2).all(axis=1).any():
                break
        else:
            return 1
        if dual:
            for point_2 in grid_2:
                if not (point_2 <= grid_2).all(axis=1).any():
                    break
            else:
                return -1
        return 0

    def build_hasse(self):
        "To build Hasse diagram for CSI algorithm to work"
        '''
        Pruning cover relation determination in all pairs of BS. Few inference test:
            1. If both on same contour, reject as cover cannot exist
            2. If one of element is E_terminal, it will cover other element
            3. If execution lies more than contour apart, explicit check only if transitive is non-existent
        '''
        pass

    def covering_sequence(self):
        "Step 3: Find Plans on each Iso-cost surface"
        '''
        Generate hasse diagram of execution covering
        Boolean Data structure & execution maintenance for CSI algorithm
        '''
        pass

    def simulate(self, act_sel, scale=None):
        "Simulating Plan-Bouquet Execution under Idea Cost model assumption"
        scale = scale if (scale is not None) else self.base_scale
        '''
        Anorexic reduction based execution, with options for both randomization and covering sequence
        '''
        pass

    def run(self):
        "Method to"
        if self.bouquet_runnable:
            self.load_maps()
            self.base_gen()
            self.nexus()
            else: # Identity mapping if Anorexic_reduction is switched off
                for plan in self.p2r_m:
                    self.anorexic_m[ (plan) ] = plan
            if covering:
                self.covering_sequence()
            # exec_list = self.simulate(act_sel, scale) # This function is indeed called from metrices, and call be later from anywhere to get list of execution
            self.save_maps()





'######## MAIN EXECUTION ########'

if __name__=='__main2__':

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

    obj_ls, stderr = [], my_open(os.path.join(master_dir,'stderr.txt'),'w')
    for query_name in my_listdir( os.path.join(master_dir,benchmark,'sql') ):
        query_id = query_name.split('.')[0].strip()
        obj_ls.append( ScaleVariablePlanBouquet(benchmark,query_id,base_scale,db_scales,stderr) )

    with multiprocessing.Pool(processes=CPU) as pool:
        for i in pool.imap_unordered(run,obj_ls):
            continue

    stderr.close()