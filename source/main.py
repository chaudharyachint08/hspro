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
│   │   ├── maps    # Store Maps, which will be further used to avoid re-computation & aid Repeated execution
│   │   │   ├── 1
│   │   │   ├── 2
│   │   │   └── 3
│   │   ├── plots   # Store plots of contours, MSO, ASO or anything relevant neeeded to be visualized
│   │   │   ├── 1
│   │   │   ├── 2
│   │   │   └── 3
│   │   ├── sql     # Store SQL Queries on which experiments are need to be done
│   │   │   ├── 1.sql
│   │   │   ├── 2.sql
│   │   │   └── 3.sql
│   │   ├── epp     # Store EPPs of corresponding SQL Queries
│   │   │   ├── 1.epp
│   │   │   ├── 2.epp
│   │   │   └── 3.epp
│   │   └── plans   # Store PLANs in both JSON & XML format for SQL Queries
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
└── source
    ├── main.py
    └── plan_format.py




├── bouquet_plots
│   │
│   *

'''


######## STANDARD & THIRD PARTY LIBRARIES IMPORTS BEGINS ########

# Importing Standard Python Libraries
ls = ['math','os','sys','argparse','copy','datetime','shutil','itertools','struct','gc','warnings','pickle','json','multiprocessing','threading','inspect']
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

ls = [('matplotlib','mpl'),('matplotlib.pyplot','plt'),('matplotlib.colors','mcolors'),('seaborn','sns'),
    ('numpy','np'), ('pandas','pd'), 'scipy',    'sklearn',
    ('tensorflow','tf'),'h5py','keras']
for i in ls:
    if not isinstance(i,tuple):
        i = (i,)
    exec('import {0} as {1}'.format(i[0],i[-1]))
    try:
        exec('print("Version of {0}",{1}.__version__)'.format(i[0],i[-1]))
    except:
        pass
# This import registers the 3D projection, but is otherwise unused.
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter



######## COMMAND LINE ARGUMENTS ########

parser = argparse.ArgumentParser()

def set_cmd_arguments():
    "set command-line arguments"
    # Bool Type Arguments
    parser.add_argument("--zero_sel" , type=eval , dest='zero_sel' , default=False) # Whether to include point of zero-selectivity, for mathematical convenience 
    parser.add_argument("--new_info" , type=eval , dest='new_info' , default=True) # To generate new information, like plans, contours, points
    parser.add_argument("--anorexic" , type=eval , dest='anorexic' , default=True) # If to use Anorexic Reduction Heuristic
    parser.add_argument("--covering" , type=eval , dest='covering' , default=False) # If to use Covering Sequence Identificationb
    parser.add_argument("--random_s" , type=eval , dest='random_s' , default=False) # Flag for Sec 4.1 Randomized Sequence of Iso-Contour Plans
    parser.add_argument("--random_p" , type=eval , dest='random_p' , default=False) # Flag for Sec 4.2 Randomized Placement of Iso-Contours (with discretization)
    # Int Type Arguments
    parser.add_argument("--CPU"        , type=eval , dest='CPU'        , default=10)
    parser.add_argument("--base_scale" , type=eval , dest='base_scale' , default=1)
    parser.add_argument("--exec_scale" , type=eval , dest='exec_scale' , default=1)
    parser.add_argument("--random_p_d" , type=eval , dest='random_p_d' , default=2) # Discretization parameter for shifting of Iso-cost contours, (always power of 2)
    parser.add_argument("--sel_round"  , type=eval , dest='sel_round'  , default=6) # If have to round of selectivity values during computation
    parser.add_argument("--font_size"  , type=eval , dest='font_size'  , default=None) # If have to round of selectivity values during computation
    # Float Type Arguments
    parser.add_argument("--r_ratio"         , type=eval , dest='r_ratio'         , default=2.0)    # IC cost ratio for bouquet
    parser.add_argument("--epsilon"         , type=eval , dest='epsilon'         , default=1e-9)    # minimal change value in floating calculation
    parser.add_argument("--min_sel"         , type=eval , dest='min_sel'         , default=0.0001) # Least sel out of 1.0, treated as epsilon in theory
    parser.add_argument("--max_sel"         , type=eval , dest='max_sel'         , default=1.0)    # Maximum sel of 1.0
    parser.add_argument("--anorexic_lambda" , type=eval , dest='anorexic_lambda' , default=0.2) # Cost Slack, for ANOREXIC Red. Heuristic
    parser.add_argument("--nexus_tolerance" , type=eval , dest='nexus_tolerance' , default=0.05) # for q-points in discretized planes, results in surface thickening
    # String Type Arguments
    parser.add_argument("--progression" , type=str  , dest='progression' , default='GP')
    parser.add_argument("--benchmark"   , type=str  , dest='benchmark'   , default='tpcds')
    parser.add_argument("--master_dir"  , type=str  , dest='master_dir'  , default=os.path.join('.','..','bouquet_master' ))
    parser.add_argument("--plots_dir"   , type=str  , dest='plots_dir'   , default=os.path.join('.','..','bouquet_plots'  ))
    # Tuple Type Arguments
    parser.add_argument("--resolution_o" , type=eval , dest='resolution_o' , default=(100,  300,  50,  20, 10) ) # Used for MSO evaluation, exponential in EPPs always, hence kept low Dimension-wise
    parser.add_argument("--resolution_p" , type=eval , dest='resolution_p' , default=(1000,  300,  50,  20, 10) ) # Used for Plan Bouquet, should be sufficient for smoothness, worst case exponential
    parser.add_argument("--db_scales"    , type=eval , dest='db_scales'    , default=(1,2,5,10,12,14,16,18,20,30,40,50,75,100,102,105,109,114,119,125,150,200,250))
    # Adding global vairables from received or default value
    args, unknown = parser.parse_known_args()
    globals().update(args.__dict__)

set_cmd_arguments()

if font_size is not None:
    plt.rcParams.update({'font.size': font_size})



######## GLOBAL DATA-STRUCTURES BEGINS ########

def global_path_var():
    "initial path variables"
    global pwd, sep, val, home_dir, master_dir, plots_dir, os_lock
    pwd = os.getcwd()
    sep = os.path.sep
    val = pwd.split(sep)
    if 'source' in val:
        home_dir = os.path.join(*val[:-1])
    else:
        home_dir = os.path.join(*val)
        master_ls = master_dir.split(sep)
        plots_ls  = plots_dir.split(sep)
        try:
            master_ls.remove('..')
            plots_ls.remove('..')
        except:
            pass
        master_dir = os.path.join(*master_ls)
        plots_dir  = os.path.join(*plots_ls)
    if sep=='/':
        home_dir = sep+home_dir
    # creating a lock for os_operations
    os_lock = multiprocessing.Lock()
    # eval( json["QUERY PLAN"][0]["Plan"]["Total-Cost"] )

global_path_var()



######## GLOBAL FUNCTIONS BEGINS ########

def my_print( *args, sep=' ', end='\n', file=sys.stdout, flush=False):
    os_lock.acquire()
    print( *args, sep=sep, end=end, file=file, flush=flush)
    os_lock.release()

def my_listdir(dir_path='.'):
    "Synchronization construct based listing of files in directory"
    # print(os_lock.__dict__['_semlock']._count())
    os_lock.acquire()
    try:
        file_ls = os.listdir(dir_path)
    except Exception as e:
        error = True
        error_msg = str(e)
    else:
        error = False
    finally:
        os_lock.release()
    if error:
        raise FileNotFoundError(error_msg)
    return file_ls

def my_open(file_path, mode='r'):
    "Synchronization construct based opening of a file"
    os_lock.acquire()
    try:
        file_handle = open(file_path, mode)
    except Exception as e:
        error = True
        error_msg = str(e)
    else:
        error = False
    finally:
        os_lock.release()
    if error:
        raise FileNotFoundError(error_msg)
    return file_handle

def run(object):
    "Simple Execution for PlanBouquet objects"
    object.run()



######## OPERATOR & PLAN DESCRIPTION CLASS ########

class OperatorNode:
    "Node of plan operator class, from which tree can be searched"

    def __init__(self, json_obj, level=0, child_ix=0, root2prv_path=[]):
        "Handles feeding of detail of operator node, and recursive construction of plan tree"
        self.level, self.child_ix, self.root2cur_path, self.child_obj_ls, self.node_detail = level, child_ix, root2prv_path+[(level,child_ix),], [], {}
        self.skip_keys = { 'Plans', 'Startup-Cost', 'Total-Cost', 'Plan-Rows' }
        self.node_detail.update({ key:json_obj[key] for key in json_obj if (key not in self.skip_keys)  })
        if ('Plans' in json_obj) and ('Plan' in json_obj['Plans']) :
            if type(json_obj['Plans']['Plan']) not in (list, tuple):
                json_obj['Plans']['Plan'] = [ json_obj['Plans']['Plan'] ]
            for sub_ix, sub_json_obj in enumerate(json_obj['Plans']['Plan']):
                self.child_obj_ls.append( OperatorNode(sub_json_obj, self.level+1, sub_ix, self.root2cur_path) )

    def __eq__(self, obj):
        "Overloading == operator for plan tree object"
        if set(self.node_detail.keys()) != set(self.node_detail.keys()):
            return False
        else:
            for key in self.node_detail:
                if self.node_detail[key] != obj.node_detail[key]:
                    return False
            if len(self.child_obj_ls) != len(obj.child_obj_ls):
                return False
            for child_self, child_obj in zip(self.child_obj_ls, obj.child_obj_ls):
                if child_self != child_obj :
                    return False
            else:
                True

    def node_to_str(self):
        "Converting detail of operator node into string"
        node_string = str({ key:self.node_detail[key] for key in sorted(self.node_detail) })
        return '\t'*self.level + str(self.root2cur_path) + '\n' + '\t'*self.level + node_string

    def tree_to_str(self):
        "Converting detail of plan tree, operator wise into string, which can be pretty printed"
        if len(self.child_obj_ls):
            return self.node_to_str() + '\n'+ '\t'*self.level +'-->[\n' + '\n'.join((child_obj.tree_to_str() for child_obj in self.child_obj_ls)) + '\n' + '\t'*self.level +'<--]'
        else:
            return self.node_to_str()



######## SCALABLE PLAN BOUQUET CLASS ########

class ScaleVariablePlanBouquet:
    "Plan Bouquet class"

    def __init__(self, benchmark, query_id, base_scale, exec_scale, db_scales, stderr):
        "Instance initializer: query_id is name of query file, which is same as ID of epp file also"
        self.benchmark, self.query_id, self.base_scale, self.exec_scale, self.db_scales, self.stderr = benchmark, query_id, base_scale, exec_scale, db_scales, stderr
        self.maps_dir  = os.path.join( master_dir,self.benchmark,'maps',  '{}'.format(self.query_id) )
        self.plots_dir = os.path.join( master_dir,self.benchmark,'plots', '{}'.format(self.query_id) )
        with my_open(os.path.join(home_dir,master_dir,self.benchmark,'sql','{}.sql'.format(query_id))) as f:
            self.query = f.read().strip()
        try:
            with my_open(os.path.join(home_dir,master_dir,self.benchmark,'epp','{}.epp'.format(query_id))) as f:
                self.epp, self.epp_dir = [], []
                for line in f.readlines():
                    line = line.strip()
                    if line: # Using 1st part, second part imply nature of cost with (increasing/decreasing/none) selectivity
                        line = line.split('|') # Below picks EPP from file line, if not commented, also direction, either direction of cost monotonicity
                        epp_line, epp_dir  = line[0].strip(), (int(line[1].strip()) if len(line)>1 else 1)
                        if not epp_line.startswith('--'):
                            self.epp.append( epp_line )
                            self.epp_dir.append( epp_dir )
                self.Dim = len(self.epp)
                self.resolution_o, self.resolution_p = resolution_o[self.Dim-1], resolution_p[self.Dim-1]
                self.sel_range_o_inc, self.sel_range_p_inc = sel_range_o[self.Dim][::+1], sel_range_p[self.Dim][::+1]
                self.sel_range_o_dec, self.sel_range_p_dec = sel_range_o[self.Dim][::-1], sel_range_p[self.Dim][::-1]
        except:
            self.bouquet_runnable = False
        else:
            self.bouquet_runnable = True if len(self.epp) else False
        self.exec_specific = {}
        self.exec_specific['random_p_d'] = random_p_d
        self.anorexic_lambda = anorexic_lambda if anorexic else 0.0
        self.obj_lock = threading.Lock() # Threading lock to provide exclusive write access of object-wide variables
        'Naming conventions of maps for saving compilation and other computation over multiple invocation'
        # [a,c,d,e,i,m,o,s,t] are [ anorexic_lambda, cost_val, database_scale, execution(IC_id,plan_id), IC_id, cost_multiplicity due to anorexic_red., POSP set, selectivity, (number of occurence) ]
        # [f,p,r] are [ file_name(without_extension), plan_id, representation_plan(serial string) ]
        # Below 3 have cyclic, and any of them can be used to derive any other, using either one or two maps
        self.p2f_m = {} # (plan_id)        : file_name (without extension)
        self.f2r_m = {} # (plan_file_name) : representation_plan(serial string)
        self.r2p_m = {} # (plan_serial)    : plan_id
        # Maps for Iso-cost surfaces, indexed from 0 as opposite from paper convention (EXPONENTIAL SPACE MAPS, SHOULD BE OPTIONAL)
        self.sd2p_m   = {} # (sel,scale)      : plan_id , (returns optimal plan_id at sel value for given scale of benchmark)
        self.spd2c_m  = {} # (sel,plan,scale) : abstract_cost value of plan at some selectivity for given scale of benchmark (FPC mapping)
        # Map related to Iso-Cost contours
        self.id2c_m  = {} # (IC_id, scale) : abstract_cost, cost budget of IC_id surface for given scale
        self.iad2p_m = {} # (IC_id, anorexic_lambda, scale) : set of plans on iso-cost surface for given scale
        # Below map will need exponential space, will be used to send to Anorexic reduction, with diff-IC contour synchronization
        self.iapd2s_m = {} # (IC_id, anorexic_lambda, plan, scale)  : set of selectivity values of each plan on IC surface
        # If anorexic_reduction is disabled, anorexic_lambda is 0.0 and reduction is not called
        self.d2o_m   = {} # (scale)           : POSP set of plan_id, used for MSO & other computation
        self.dp2t_m  = {} # (scale, plan_id)   : Multiplicity of each plan in optimizer grid, used for ASO computation of Native optimizer
        self.aed2aed_m = {} # (anorexic_lambda, IC_id, plan_id, scale) useful for CSI Algorithms storage, checking if that execution has already occured in lower IC-contours
        self.aed2m_m   = {} # (anorexic_lambda, IC_id, plan_id, scale) : multiplicity introduced due to anorexic reduction


    ######## DATA-BASE CONNECTION METHODS ########
    
    def get_cost_and_plan(self, sel, plan_id=None, scale=None): # return (result_cost, result_plan)
        "FPC of plan at some other selectivity value if plan is suplied, else optimal plan and its cost at supplied selectivity"
        scale = scale if (scale is not None) else self.base_scale
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
                if plan_id is None:
                    cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {};'.format(epp_list,sel_list,self.query)  )
                else: # Plan supplied as XML string
                    plan_path = os.path.join( home_dir,master_dir,self.benchmark,'plans','xml', self.query_id, '{}.xml'.format(self.p2f_m[plan_id]) )
                    cursor.execute(  'explain (costs, verbose, format xml) selectivity ({})({}) {} FPC {};'.format(epp_list,sel_list,self.query,plan_path)  )
                result_plan = cursor.fetchone()[0]
            except (Exception, psycopg2.DatabaseError) as error : # MultiThreaded Logging of Exception
                my_print("Error while connecting to PostgreSQL", error,file=self.stderr,flush=True)
                break_flag = False
            else:
                json_obj = pf.xml2json(result_plan,mode='string')
                result_cost = float( json_obj["QUERY PLAN"][0]["Plan"]["Total-Cost"] )
                break_flag = True
            finally:
                if connection:
                    cursor.close()
                    connection.close()
                if break_flag:
                    break
        return (result_cost, result_plan)


    ######## ABSTRACTION OVER DATA-BASE CONNECTION ########   

    def plan(self, sel, scale=None): # return plan_val
        "Costs plan at some selectivity value"
        scale = scale if (scale is not None) else self.base_scale
        _, plan_val = self.get_cost_and_plan(sel, scale=scale)
        return plan_val

    def cost(self, sel, plan_id=None, scale=None): # return cost_val
        "Costs plan at some selectivity value"
        scale = scale if (scale is not None) else self.base_scale
        if (sel, plan_id, scale) not in self.spd2c_m:
            cost_val, _ = self.get_cost_and_plan(sel, plan_id=plan_id, scale=scale)
            # self.spd2c_m[ (sel, plan_id, scale) ] = cost_val         # Exponential Storage of |POSP|*RED**dim(EPP)
        else:
            cost_val = self.spd2c_m[ (sel, plan_id, scale) ]
        return cost_val


    ######## PLAN PROCESSING, SAVING & LOADING METHOD USING OperatorNode CLASS ########

    def store_plan(self, xml_string): # return self.r2p_m[plan_serial]
        "Method to store XML & JSON variants on plans in respective directories, return plan_id"
        json_obj = pf.xml2json(xml_string, mode='string')
        plan_serial = OperatorNode(json_obj['QUERY PLAN'][0]['Plan']).tree_to_str()
        if plan_serial not in self.r2p_m:
            xml_plan_path  = os.path.join( home_dir,master_dir,self.benchmark,'plans','xml',  self.query_id)
            json_plan_path = os.path.join( home_dir,master_dir,self.benchmark,'plans','json', self.query_id)
            if not os.path.isdir(xml_plan_path):
                os.makedirs(xml_plan_path,exist_ok=True)
            if not os.path.isdir(json_plan_path):
                os.makedirs(json_plan_path,exist_ok=True)
            plan_id, json_obj = len(my_listdir(xml_plan_path)), pf.xml2json(xml_string,mode='string')
            with my_open( os.path.join(xml_plan_path,'{}.xml'.format(plan_id)) ,'w') as f:
                f.write( xml_string )
            self.save_dict(json_obj, os.path.join(json_plan_path,'{}.json'.format(plan_id)) )
            self.p2f_m[plan_id], self.f2r_m[plan_id], self.r2p_m[plan_serial] = plan_id, plan_serial, plan_id
        return self.r2p_m[plan_serial]


    ######## PERFORMANCE METRICS METHODS ########

    def SubOpt(self, act_sel, est_sel, scale=None, bouquet=False): # SubOpt
        "Ratio of plan on estimated sel to plan on actual sel"
        scale = scale if (scale is not None) else self.base_scale
        if not bouquet: # Classic Optimizer Style Metric
            return self.cost(act_sel, plan_id=self.store_plan( self.plan(est_sel, scale=scale) ), scale=scale) / self.cost(act_sel, scale=scale)
        else: # Bouquet Style Metric
            return (self.simulate(act_sel, scale=scale)['termination-cost']) / self.cost(act_sel, scale=scale)

    def WorstSubOpt(self, act_sel, scale=None): # WorstSubOpt (|POSP|)
        "SubOpt for all possible est_selectivities"
        scale = scale if (scale is not None) else self.base_scale
        cost_ls = [ self.cost(act_sel, plan_id=plan_id, scale=scale) for plan_id in self.d2o_m[scale] ]
        return max(cost_ls, default=np.inf) / min(cost_ls, default=epsilon)

    def MaxSubOpt(self, scale=None, bouquet=False): # MSO (|POSP|*RES**DIM)
        "Global worst case of suboptimality"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*[ sel_range_o[self.Dim] ]*self.Dim)
        if not bouquet: # Classic Optimizer Style Metric (computed over all plans either in Optimizer grid or Bouquet Exploration)
            return max( self.WorstSubOpt(act_sel, scale) for act_sel in epp_iterator )
        else: # Bouquet Style Metric
            return max( self.SubOpt(act_sel, -1, scale=scale, bouquet=True) for act_sel in epp_iterator )

    def AvgSubOpt(self, scale=None, bouquet=False):
        "Average suboptimality over ESS under uniformity assumption"
        scale = scale if (scale is not None) else self.base_scale
        posp_ls = list(self.d2o_m[scale]) if scale in self.d2o_m else []
        epp_iterator = itertools.product(*[ sel_range_o[self.Dim] ]*self.Dim)
        if not bouquet: # Classic Optimizer Style Metric (computed only over Optimizer grid as weightage of plan occurence is needed)
            result = 0
            for act_sel in epp_iterator:
                cost_ls = [ self.cost(act_sel,plan_id=plan_id,scale=scale)                             for plan_id in posp_ls ]
                freq_ls = [ (self.dp2t_m[(scale,plan_id)] if (scale,plan_id) in self.dp2t_m else 0)    for plan_id in posp_ls ]
                average_cost = ( float( np.dot(cost_ls, freq_ls) ) / (len(sel_range_o[len(self.epp)])**len(self.epp)) )
                result += ( average_cost / self.cost(act_sel, scale=scale) )
            return result / (len(sel_range_o[self.Dim])**self.Dim)
        else: # Bouquet Style Metric
            return sum( self.SubOpt(act_sel, -1, scale=scale, bouquet=True) for act_sel in epp_iterator ) / (len(sel_range_o[self.Dim])**self.Dim)

    def MaxHarm(self, scale=None):
        "Harm using Plan Bouquet, over Classic Optimizer"
        scale = scale if (scale is not None) else self.base_scale
        epp_iterator = itertools.product(*[ sel_range_o[self.Dim] ]*self.Dim)
        return max(  ( self.SubOpt(act_sel, -1, scale=scale, bouquet=True) / self.WorstSubOpt(act_sel, scale=scale) ) for act_sel in epp_iterator  ) - 1


    ######## MAPS SAVING & RE-LOADING METHODS FOR RECOMPUTATION AVOIDANCE ON DIFFERENT INVOCATIONS ########    

    def save_dict(self, dict_obj, file_name):
        "Serialize json into file"
        json.dump( dict_obj, my_open(file_name,'w') )

    def load_dict(self, file_name):
        "Read json from file"
        return json.load( my_open(file_name) )

    def save_obj(self, obj, file_name):
        "Serialize object into file"
        # print('save_obj', file_name)
        with my_open(file_name,'wb') as fp:
            pickle.dump(obj, fp)
            # pickle.dump(obj, fp, pickle.HIGHEST_PROTOCOL)

    def load_obj(self, file_name):
        "Read object from file"
        # print('load_obj', file_name)
        with my_open(file_name, 'rb') as fp:
            return pickle.load(fp)

    def save_points(self,IC_id,anorexic_lambda,plan_id,scale):
        "Save selectivity points onto disk, cobining with previous points, and clears memory"
        # print('save_points', (IC_id,anorexic_lambda,plan_id,scale))
        if os.path.isfile( os.path.join( self.maps_dir, str((IC_id,anorexic_lambda,plan_id,scale))  ) ):
            prev_val = self.load_obj( os.path.join( self.maps_dir, str((IC_id,anorexic_lambda,plan_id,scale))  ) )
        else:
            prev_val = set()
        cur_val = self.iapd2s_m[(IC_id,anorexic_lambda,plan_id,scale)]
        cur_val.update(prev_val)
        # print((IC_id,anorexic_lambda,plan_id,scale))
        # print(cur_val)
        self.save_obj(   cur_val , os.path.join( self.maps_dir, ','.join(tuple(str(x) for x in (IC_id,anorexic_lambda,plan_id,scale)))  )  )
        self.iapd2s_m[(IC_id,anorexic_lambda,plan_id,scale)] = set()

    def load_points(self,IC_id,anorexic_lambda,plan_id,scale):
        # print('load_points', (IC_id,anorexic_lambda,plan_id,scale))
        if os.path.isfile( os.path.join( self.maps_dir, ','.join(tuple(str(x) for x in (IC_id,anorexic_lambda,plan_id,scale)))  ) ):
            old_val = self.load_obj( os.path.join( self.maps_dir, ','.join(tuple(str(x) for x in (IC_id,anorexic_lambda,plan_id,scale)))  ) )
        else:
            old_val = set()
        return old_val

    def save_maps(self):
        "Save present maps values into objects for repeatable execution over difference simulation"
        if not os.path.isdir(self.maps_dir):
            os.makedirs(self.maps_dir)
        self.save_obj( self.exec_specific , os.path.join(self.maps_dir, 'exec_specific' ) )
        self.save_obj( self.p2f_m         , os.path.join(self.maps_dir, 'p2f_m'     ) )
        self.save_obj( self.f2r_m         , os.path.join(self.maps_dir, 'f2r_m'     ) )
        self.save_obj( self.r2p_m         , os.path.join(self.maps_dir, 'r2p_m'     ) )
        self.save_obj( self.sd2p_m        , os.path.join(self.maps_dir, 'sd2p_m'    ) )
        self.save_obj( self.spd2c_m       , os.path.join(self.maps_dir, 'spd2c_m'   ) )
        self.save_obj( self.id2c_m        , os.path.join(self.maps_dir, 'id2c_m'    ) )
        self.save_obj( self.iad2p_m       , os.path.join(self.maps_dir, 'iad2p_m'   ) )
        # self.save_obj( self.iapd2s_m      , os.path.join(self.maps_dir, 'iapd2s_m'  ) )
        self.save_obj( self.d2o_m         , os.path.join(self.maps_dir, 'd2o_m'     ) )
        self.save_obj( self.dp2t_m        , os.path.join(self.maps_dir, 'dp2t_m'    ) )
        self.save_obj( self.aed2aed_m     , os.path.join(self.maps_dir, 'aed2aed_m' ) )
        self.save_obj( self.aed2m_m       , os.path.join(self.maps_dir, 'aed2m_m'   ) )

    def reindex(self, old_random_p_d, new_random_p_d):
        "Fucntion for changing Indexes for Iso-cost contours, after different loading"
        incr_ratio = (new_random_p_d//old_random_p_d)
        remap = { x:((x+1)*incr_ratio-1) for x in range(old_random_p_d*self.IC_count) }

    def remap(self, old_map, reindex_m, re_ix=0, both_side=False):
        "Changing map keys, for re-indexing of Iso-cost contours"
        new_map = {}
        tuple_mode_k = True if type(list(old_map.keys())[0])==tuple else False
        for key in old_map:
            if not both_side:
                old_val = old_map[key]
            else: # This section work upto value in old_map in single level iterable, useful for aed2aed_m reindexing
                tuple_mode_v = True if type(list(old_map.values())[0])==tuple else False
                old_val = ((*old_map[key][:re_ix],reindex_m[old_map[key]],*old_map[key][re_ix+1:]) if tuple_mode_v else reindex_m[old_map[key]])
            new_map[ ((*key[:re_ix],reindex_m[key],*key[re_ix+1:]) if tuple_mode_k else reindex_m[key]) ] = old_val
        return new_map

    def load_maps(self):
        "Loads previous maps values into objects for repeatable execution over difference simulation"
        if new_info:
            try:
                shutil.rmtree(self.maps_dir)
                shutil.rmtree( os.path.join( home_dir,master_dir,self.benchmark,'plans','xml',  self.query_id) )
                shutil.rmtree( os.path.join( home_dir,master_dir,self.benchmark,'plans','json', self.query_id) )
            except:
                pass
        if not os.path.isdir( self.maps_dir ):
            os.makedirs( self.maps_dir )
        else: # if os.path.isdir(self.maps_dir):
            self.p2f_m     = self.load_obj( os.path.join( self.maps_dir,'p2f_m'     ) )
            self.f2r_m     = self.load_obj( os.path.join( self.maps_dir,'f2r_m'     ) )
            self.r2p_m     = self.load_obj( os.path.join( self.maps_dir,'r2p_m'     ) )
            self.sd2p_m    = self.load_obj( os.path.join( self.maps_dir,'sd2p_m'    ) )
            self.spd2c_m   = self.load_obj( os.path.join( self.maps_dir,'spd2c_m'   ) )
            self.id2c_m    = self.load_obj( os.path.join( self.maps_dir,'id2c_m'    ) )
            self.iad2p_m   = self.load_obj( os.path.join( self.maps_dir,'iad2p_m'   ) )
            # self.iapd2s_m  = self.load_obj( os.path.join( self.maps_dir,'iapd2s_m'  ) )
            self.d2o_m     = self.load_obj( os.path.join( self.maps_dir,'d2o_m'     ) )
            self.dp2t_m    = self.load_obj( os.path.join( self.maps_dir,'dp2t_m'    ) )
            self.aed2aed_m = self.load_obj( os.path.join( self.maps_dir,'aed2aed_m' ) )
            self.aed2m_m   = self.load_obj( os.path.join( self.maps_dir,'aed2m_m'   ) )

            exec_specific = self.load_obj( os.path.join( self.maps_dir,'exec_specific' ) )
            if self.exec_specific['random_p_d'] != exec_specific['random_p_d']:
                old_val = exec_specific['random_p_d']
                new_val = (self.exec_specific['random_p_d']*exec_specific['random_p_d']) // math.gcd(self.exec_specific['random_p_d'],exec_specific['random_p_d'])
                reindex_m = self.reindex(self, old_val, new_val)
                self.iad2p_m, self.id2c_m, self.iapd2s_m = self.remap(self.iad2p_m,reindex_m,0), self.remap(self.id2c_m,reindex_m,0), self.remap(self.iapd2s_m,reindex_m,0)
                self.aed2aed_m, self.aed2m_m = self.remap(self.aed2aed_m,reindex_m,1,both_side=True), self.remap(self.aed2aed_m,reindex_m,1)
                self.exec_specific['random_p_d'] = new_val


    ######## BOUQUET EXECUTION METHODS ########

    def build_posp(self, scale=None):
        "Optimal plans over ESS, exponential time, bouquet plans also added during compilation"
        scale = scale if (scale is not None) else self.base_scale
        if scale not in self.d2o_m:
            self.d2o_m[scale] = set()
        # count = 0
        for sel in itertools.product(*[ sel_range_o[self.Dim] ]*self.Dim):
            # print('Pre-store')
            plan_id = self.store_plan( self.plan(sel, scale=scale) )
            # print('Post-store')
            self.d2o_m[scale].add( plan_id )
            if (scale, plan_id) not in self.dp2t_m:
                self.dp2t_m[(scale, plan_id)] = 0
            self.dp2t_m[(scale, plan_id)] += 1
            # count += 1
            # print(count)
        self.exec_specific['build_posp'] = True

    def build_sel(self, sel_ix_ls, mode='p'):
        "builds selectivity point in ESS from index of points in ESS, this should be made inline later"
        # print(sel_ix_ls)
        if   mode=='p':
            return tuple( (self.sel_range_p_inc[sel_ix] if self.epp_dir[ix]>0 else self.sel_range_p_dec[sel_ix]) for ix,sel_ix in enumerate(sel_ix_ls) )
        elif mode=='o':
            return tuple( (self.sel_range_o_inc[sel_ix] if self.epp_dir[ix]>0 else self.sel_range_o_dec[sel_ix]) for ix,sel_ix in enumerate(sel_ix_ls) )

    def base_gen(self, scale=None):
        "Find cost values for each iso-cost surface"
        scale = scale if (scale is not None) else self.base_scale
        if scale not in self.d2o_m:
            self.d2o_m[scale] = set()
        sel_min_ix, sel_max_ix = (0,)*self.Dim, (self.resolution_p-1,)*self.Dim
        sel_min,    sel_max    = self.build_sel(sel_min_ix), self.build_sel(sel_max_ix)
        # print(sel_min,    sel_max)
        # Getting plans at extremas, storing them and serializing them
        plan_min_id, plan_max_id = self.store_plan( self.plan(sel_min, scale=scale) ), self.store_plan( self.plan(sel_max, scale=scale) )
        self.d2o_m[scale].update( {plan_min_id,plan_max_id} )
        if (scale, plan_min_id) not in self.dp2t_m:
            self.dp2t_m[(scale, plan_min_id)] = 0
        if (scale, plan_max_id) not in self.dp2t_m:
            self.dp2t_m[(scale, plan_max_id)] = 0
        # Setting plan entries for optimal plan at locations and cost values at those location
        self.C_min, self.C_max = self.cost(sel_min, plan_id=plan_min_id, scale=scale), self.cost(sel_max, plan_id=plan_max_id, scale=scale)
        self.sd2p_m[(sel_min,scale)], self.sd2p_m[(sel_max,scale)] = plan_min_id, plan_max_id
        self.spd2c_m[(sel_min,plan_min_id,scale)], self.spd2c_m[(sel_max,plan_max_id,scale)] = self.C_min, self.C_max
        # Maps for Iso-cost surfaces, indexed from 0 as opposite from paper convention
        # Also for now only last IC is known and have one optimal plan at it, we can create its entries
        self.IC_count = int( np.floor(  np.log(self.C_max/self.C_min) / np.log(r_ratio)  )+1 )
        self.random_p_IC_count, self.random_p_r_ratio = self.IC_count*self.exec_specific['random_p_d'], r_ratio**(1/self.exec_specific['random_p_d'])
        for ix in range(self.random_p_IC_count):
            self.id2c_m[(ix,scale)] = self.C_max / ( self.random_p_r_ratio**(self.random_p_IC_count-(ix+1)) )
        # Below steps are to be done using NEXUS for other than last contours
        self.iad2p_m[(self.random_p_IC_count-1,0.0,scale)]          = { plan_max_id }
        self.iapd2s_m[(self.random_p_IC_count-1,0.0,plan_max_id,scale)] = { sel_max }
        self.exec_specific['base_gen'] = True

    def anorexic_reduction(self, IC_id, scale=None):
        "Reduing overall number of plans, effectively reducing plan density on each iso-cost surface"
        scale = scale if (scale is not None) else self.base_scale
        org_plans = self.iad2p_m[(IC_id, 0.0, scale)]
        # Identity mapping for CSI with ANOREXIC reduction
        self.obj_lock.acquire()
        for plan_id in org_plans: 
            self.aed2aed_m[(self.anorexic_lambda,IC_id,plan_id,scale)] = (self.anorexic_lambda,IC_id,plan_id,scale)
        self.obj_lock.release()
        # Finding all points the are on present contour
        contour_points = set().union( *(self.iapd2s_m[(IC_id,0.0,plan_id,scale)] for plan_id in org_plans) )
        # Finding inital eating capacity with Anorexic reduction threshold (1+self.anorexic_lambda)
        eating_capacity = {}
        for plan_id in org_plans:
            non_optimal_points = contour_points - self.iapd2s_m[(IC_id,0.0,plan_id,scale)]
            eating_capacity[plan_id] = {}
            for sel in non_optimal_points:
                cost_val = self.cost(sel, plan_id=plan_id, scale=scale)
                if cost_val <= self.id2c_m[(IC_id,scale)]*(1+self.anorexic_lambda):
                    eating_capacity[plan_id][sel] = cost_val
        # Greedy Anorexic reduction algorithm as per Thesis of C.Rajmohan
        reduced_plan_set = set()
        while contour_points:
            max_eating_plan_id = max( eating_capacity, key=lambda plan_id:len(eating_capacity[plan_id]) )
            reduced_plan_set.add( max_eating_plan_id )
            anorexic_max_cost = max( eating_capacity[max_eating_plan_id].values(), default=self.id2c_m[(IC_id,scale)] )
            points_gone = set().union( *( self.iapd2s_m[(IC_id,0.0,max_eating_plan_id,scale)], eating_capacity[max_eating_plan_id].keys() ) )
            contour_points.difference_update( points_gone )
            for plan_id in eating_capacity:
                for sel in points_gone:
                    eating_capacity[plan_id].pop(sel,None)
            self.obj_lock.acquire()
            self.aed2m_m[(self.anorexic_lambda,IC_id,max_eating_plan_id,scale)]  = anorexic_max_cost / self.id2c_m[(IC_id,scale)]
            self.iapd2s_m[(IC_id,self.anorexic_lambda,max_eating_plan_id,scale)] = points_gone
            self.save_points(IC_id,0.0,                 max_eating_plan_id,scale)
            self.save_points(IC_id,self.anorexic_lambda,max_eating_plan_id,scale)
            self.obj_lock.release()
        self.obj_lock.acquire()
        self.iad2p_m[(IC_id, self.anorexic_lambda, scale)] = reduced_plan_set
        for plan_id in org_plans- reduced_plan_set:
            self.save_points(IC_id,0.0,plan_id,scale)
        self.obj_lock.release()

    def nexus(self, IC_id, scale=None):
        "Step 1: Find Plans on each Iso-cost surface"
        # print('NEXUS CALLED',IC_id,len(inspect.stack(0)),threading.current_thread())
        scale = scale if (scale is not None) else self.base_scale
        '''
        # Locating Initial Seed, Binary Search based edges selection
        Location L(x, y) is included in the contour C if it satisfies the following conditions:
            (a) C ≤ C_opt[L] ≤ (1 + α)C ; and
            (b) if C_opt[L(x−1)] ≥ C and C_opt[L(y−1) ) ≥ C then C_opt[L(−1)] < C
        # Neighborhood EXploration Using Seed (NEXUS)
        If C_opt[(S(y−1)] < C, then set S = S(x+1) else S = S(y−1)
        The end of this recursive routine is marked by the non-existence of either S(x+1) or S(y−1) in the ESS grid.
        '''
        # Locating initial seed boundary (0^s, v, (RES-1)^t) such that 0<=v<(RES-1), limit of index is [0,RES-1]
        if IC_id != (self.random_p_IC_count-1): # Last contour is specially built during base_gen
            lower_cost, upper_cost, contour_cost = None, None, self.id2c_m[(IC_id,scale)]
            for dim_count in range(self.Dim): # line of initial seed (dim_count, dim_count+1)
                s, t = self.Dim-(dim_count+1), dim_count
                low_end_ix,  upr_end_ix  = ( (0,)*(s+1) + (self.resolution_p-1,)*(t) ), ( (0,)*(s) + (self.resolution_p-1,)*(t+1) )
                low_end_sel, upr_end_sel = self.build_sel(low_end_ix), self.build_sel(upr_end_ix)
                if lower_cost is None:
                    lower_cost = self.cost(low_end_sel, scale=scale)
                upper_cost = self.cost(upr_end_sel, scale=scale)
                if lower_cost<=contour_cost and contour_cost<upper_cost:
                    break
                lower_cost = upper_cost
            # Binary search for finding value within interval [C,(1+α)C]
            l_ix, u_ix = 0, (self.resolution_p - 1)-1 # -1 is done twice, as limit are from 0<=v<(RES-1)
            while True:
                m_ix = (l_ix+u_ix)//2
                mid_sel_ix = ( (0,)*s + (m_ix,) + (self.resolution_p-1,)*t )
                mid_sel    = self.build_sel(mid_sel_ix)
                mid_cost   = self.cost(mid_sel, scale=scale)
                if contour_cost<=mid_cost and mid_cost<=(1+nexus_tolerance)*contour_cost:
                    break
                elif mid_cost < contour_cost:
                    l_ix = m_ix+1
                else:
                    u_ix = m_ix-1
            # Repeated Exponential search to find leftmost point inside [C,(1+α)C]
            v_ix = m_ix
            while True:
                continue_exp, exp_step = False, 1
                while v_ix >= exp_step:
                    e_ix = v_ix-exp_step
                    exp_sel_ix = ( (0,)*s + (e_ix,) + (self.resolution_p-1,)*t )
                    exp_sel    = self.build_sel(exp_sel_ix)
                    exp_cost   = self.cost(exp_sel, scale=scale)
                    if contour_cost <= exp_cost:
                        v_ix = e_ix
                        exp_step *= 2 # Increase step size by 2 each time
                        continue_exp = True # Future attempt of Exponential search
                    else:
                        break                
                if not continue_exp:
                    break
            # Initial seed value, which will explore into D-dimensional surface
            initial_seed_ix  = ( (0,)*s + (v_ix,) + (self.resolution_p-1,)*t )
            initial_seed_sel = self.build_sel(initial_seed_ix)
            initial_seed_plan_id = self.store_plan( self.plan(initial_seed_sel, scale=scale) )
            # Lock & local data-structures of outer function to be used
            nexus_lock = threading.Lock()
            iad2p_m, iapd2s_m = {}, {} # Local Data Structures will be merged at end of entire contour exploration
            iad2p_m[(IC_id,0.0,scale)] = { initial_seed_plan_id }
            iapd2s_m[(IC_id,0.0,initial_seed_plan_id,scale)] = { initial_seed_sel }
            # Exploration using Initial seed in Dim dimensional space
            def exploration(org_seed_ix, total_dim):
                "Nested function for exploration using seed and contour generation"
                nonlocal IC_id, contour_cost, scale, iad2p_m, iapd2s_m, nexus_lock
                if total_dim >= 1 :
                    dim_h = total_dim-1
                    cur_ix, exploration_thread_ls = list(org_seed_ix[:]), []
                    for dim_l in range(total_dim-1):
                        # (dim_l, dim_h) is dimension pair to be explored
                        p2s_m, seed_ix_ls= {}, []
                        # 2D exploration using initial seed
                        seed_ix_ls.append( tuple(cur_ix) )
                        while True:
                            x, y = cur_ix[dim_l], cur_ix[dim_h]
                            next_ix  = cur_ix[:]
                            next_ix[dim_h] -= 1
                            next_sel = self.build_sel(next_ix)
                            cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                            if cost_val < contour_cost:  # C_opt[(S(y−1)] < C
                                # S = S(x+1)
                                x += 1
                                next_ix  = cur_ix[:]
                                next_ix[dim_l] += 1
                                next_sel = self.build_sel(next_ix)
                                cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                            else:
                                # S = S(y−1)
                                y -= 1
                            next_plan_id = self.store_plan( plan_xml )
                            if next_plan_id in p2s_m:
                                p2s_m[next_plan_id].add(next_sel)
                            else:
                                p2s_m[next_plan_id] = {next_sel}
                            cur_ix[dim_l], cur_ix[dim_h] = x, y
                            seed_ix_ls.append( tuple(cur_ix) )
                            if (not (0<=x+1 and x+1<=self.resolution_p-1)) or (not (0<=y-1 and y-1<=self.resolution_p-1)) : # non-existence of either S(x+1) or S(y−1)
                                break
                        # First search include both ends of 2D exploration, rest will not include first end
                        if dim_l+1 != dim_h:
                            del seed_ix_ls[0]
                        nexus_lock.acquire()
                        iad2p_m[(IC_id,0.0,scale)].update(p2s_m.keys())
                        for plan_id in p2s_m:
                            if (IC_id,0.0,plan_id,scale) in iapd2s_m:
                                iapd2s_m[(IC_id,0.0,plan_id,scale)].update(p2s_m[plan_id])
                            else:
                                iapd2s_m[(IC_id,0.0,plan_id,scale)] = p2s_m[plan_id]
                        nexus_lock.release()
                        # For each seed generated, call (Dim-1) dimensional subproblem
                        d2_exploration_thread_ls = [ threading.Thread(target=exploration,args=(seed_ix,total_dim-1,)) for seed_ix in seed_ix_ls ]
                        # All exploration are collected and waited to end outside dim_l forloop
                        exploration_thread_ls.extend( d2_exploration_thread_ls )
                        # Launching construction of all Ico-cost contours
                        for d2_explore_thread in d2_exploration_thread_ls:
                            d2_explore_thread.start()
                    # Waiting for construction of all Ico-cost contours
                    for explore_thread in exploration_thread_ls:
                        explore_thread.join()

            # Calling generic exploration part of NEXUS algorithm, if EPP has two or more dim, search will continue
            exploration(initial_seed_ix, self.Dim)
            # Merging to Object Data-structures after exploration is complete
            self.obj_lock.acquire()
            for key in iad2p_m:
                self.iad2p_m[key] = iad2p_m[key]
            for key in iapd2s_m:
                self.iapd2s_m[key] = iapd2s_m[key]
            for plan_id_ls in iad2p_m.values():
                self.d2o_m[scale].update( plan_id_ls )
                for plan_id in plan_id_ls:
                    if (scale, plan_id) not in self.dp2t_m:
                        self.dp2t_m[(scale, plan_id)] = 0
            self.obj_lock.release()
        # Map generation for CSI & Anorexic Reduction cost-multiplicity
        self.obj_lock.acquire()
        org_plans = self.iad2p_m[(IC_id, 0.0, scale)]
        for plan_id in org_plans: # Identity mapping for cost-multiplicity without ANOREXIC reduction  & CSI
            self.aed2m_m[(0.0,IC_id,plan_id,scale)], self.aed2aed_m[(0.0,IC_id,plan_id,scale)] = 1.0, (0.0,IC_id,plan_id,scale)
        self.obj_lock.release()
        # Calling anorexic reduction, and associated map generation
        if self.anorexic_lambda:
            self.anorexic_reduction(IC_id, scale=scale)
            reduced_plans = self.iad2p_m[(IC_id, self.anorexic_lambda, scale)]
            self.obj_lock.acquire()
            for plan_id in reduced_plans: # Identity mapping for CSI
                self.aed2aed_m[(self.anorexic_lambda,IC_id,plan_id,scale)] = (self.anorexic_lambda,IC_id,plan_id,scale)
            self.obj_lock.release()
        else:
            self.obj_lock.acquire()
            for plan_id in self.iad2p_m[(IC_id, 0.0, scale)]:
                self.save_points(IC_id,0.0,plan_id,scale)
            self.obj_lock.release()

    def simulate(self, act_sel, scale=None):
        "Simulating Plan-Bouquet Execution under Idea Cost model assumption"
        scale = scale if (scale is not None) else self.base_scale
        # Building main iso-cost contours. as they are needed for plotting, also will execute if random_p is False
        random_p_val = self.exec_specific['random_p_d']-1
        IC_indices = sorted( set(range(random_p_val, self.random_p_IC_count, self.exec_specific['random_p_d'])).union({(self.random_p_IC_count-1)}) )
        # Executing NEXUS Algorithm for multi-dimensions
        # print(IC_indices)
        nexus_thread_ls = [ threading.Thread(target=self.nexus,args=(IC_ix,scale,)) for IC_ix in IC_indices ]
        # Boolean indecxing to check if previous contour is explored in any past invocation
        if 'nexus' not in self.exec_specific:
            self.exec_specific['nexus'] = {}
        if scale not in self.exec_specific['nexus']:
            self.exec_specific['nexus'][scale] = {}
        for IC_ix in IC_indices:
            if IC_ix not in self.exec_specific['nexus'][scale]:
                self.exec_specific['nexus'][scale][IC_ix] = False
        # Launching construction of all Ico-cost contours
        for ix, nexus_thread in enumerate(nexus_thread_ls):
            if not self.exec_specific['nexus'][scale][IC_indices[ix]]:
                nexus_thread.start()
        # Waiting for construction of all Ico-cost contours
        for ix, nexus_thread in enumerate(nexus_thread_ls):
            if not self.exec_specific['nexus'][scale][IC_indices[ix]]:
                nexus_thread.join()
                self.exec_specific['nexus'][scale][IC_indices[ix]] = True
        # Randomly or Deterministicly selecting set of contours to be considered in plan bouquet
        if random_p:
            random_p_val = np.random.randint(self.exec_specific['random_p_d'])
        else:
            random_p_val = self.exec_specific['random_p_d']-1
        IC_indices = sorted( set(range(random_p_val, self.random_p_IC_count, self.exec_specific['random_p_d'])).union({(self.random_p_IC_count-1)}) )
        # Executing NEXUS Algorithm for multi-dimensions
        nexus_thread_ls = [ threading.Thread(target=self.nexus,args=(IC_ix,scale,)) for IC_ix in IC_indices ]
        # Boolean indecxing to check if previous contour is explored in any past invocation
        for IC_ix in IC_indices:
            if IC_ix not in self.exec_specific['nexus'][scale]:
                self.exec_specific['nexus'][scale][IC_ix] = False
        # Launching construction of all Ico-cost contours
        for ix, nexus_thread in enumerate(nexus_thread_ls):
            if not self.exec_specific['nexus'][scale][IC_indices[ix]]:
                nexus_thread.start()
        # Waiting for construction of all Ico-cost contours
        for ix, nexus_thread in enumerate(nexus_thread_ls):
            if not self.exec_specific['nexus'][scale][IC_indices[ix]]:
                nexus_thread.join()
                self.exec_specific['nexus'][scale][IC_indices[ix]] = True
        # CSI algorithm, for further reducing effective plan density on each contour
        if covering:
            self.covering_sequence(scale=scale)
        # Building Boolean array for each execution only once, in case of covering
        zagged_bool = {}
        for IC_ix in IC_indices:
            zagged_bool[IC_ix] = {}
            for plan_id in self.iad2p_m[(IC_ix,self.anorexic_lambda,scale)]:
                zagged_bool[IC_ix][plan_id] = False
        simulation_result = {}
        # Simulating Execution using Plan_bouquet
        total_cost  = 0
        simulation_result['termination-cost'], simulation_result['done'], simulation_result['MSO_k'] = 0, False, {}
        for IC_ix in IC_indices:
            curr_IC_cost_ls, curr_IC_total_cost_ls = [], []
            plan_ls = list(self.iad2p_m[(IC_ix,self.anorexic_lambda,scale)])
            if random_s:
                np.random.shuffle(plan_ls)
            for plan_id in plan_ls:
                _, cover_IC_ix, cover_plan_id, _ = self.aed2aed_m[(self.anorexic_lambda,IC_ix,plan_id,scale)]
                if zagged_bool[cover_IC_ix][cover_plan_id] is False:
                    cost_val, cost_threshold = self.cost(act_sel, plan_id=cover_plan_id, scale=scale), self.id2c_m[(cover_IC_ix,scale)]*self.aed2m_m[(self.anorexic_lambda, cover_IC_ix, cover_plan_id, scale)]
                    # print('IC_ix = {} , plan_id = {}'.format(cover_IC_ix,cover_plan_id))
                    # print(cost_val, cost_threshold)
                    if cost_val <= cost_threshold:
                        if simulation_result['done'] is False:
                            simulation_result['done'] = True
                            simulation_result['termination-cost'] += cost_val
                    else:
                        if simulation_result['done'] is False:
                            simulation_result['termination-cost'] += cost_threshold
                    zagged_bool[cover_IC_ix][cover_plan_id] = True
                    total_cost += cost_threshold
                    curr_IC_cost_ls.append(cost_threshold)
                    curr_IC_total_cost_ls.append(total_cost)
            if (IC_ix==IC_indices[0]):
                simulation_result['MSO_k'][IC_ix] = max(curr_IC_total_cost_ls) / (min(curr_IC_cost_ls)/r_ratio)
            else:
                simulation_result['MSO_k'][IC_ix] = max(curr_IC_total_cost_ls) / min(prev_IC_cost_ls)
            prev_IC_cost_ls, prev_IC_total_cost_ls = curr_IC_cost_ls, curr_IC_total_cost_ls
            simulation_result['MSO'] = max( simulation_result['MSO_k'].values() )
        return simulation_result

    def evaluate(self, mode='p', scale=None):
        "Evaluates various metrics of Native optimizer & Plan Bouquet"
        scale = scale if (scale is not None) else self.base_scale
        if   mode=='p': # Evaluating Plan Bouquet
            MSO, ASO, MH = self.MaxSubOpt(scale,bouquet=True), self.AvgSubOpt(scale,bouquet=True), self.MaxHarm(scale)
        elif mode=='o': # Evaluating Native Optimizer
            if ('build_posp' not in self.exec_specific) or (not self.exec_specific['build_posp']):
                self.exec_specific['build_posp'] = False
                self.build_posp(scale)
            MSO, ASO, MH = self.MaxSubOpt(scale,bouquet=False), self.AvgSubOpt(scale,bouquet=False), None
        print('Evaluation of {} is'.format('Plan Bouquet' if mode=='p' else 'Native Optimizer'))
        print(('MSO',MSO),('ASO',ASO),sep='\n')
        if MH is not None:
            print('MH',MH)

    def plot_contours(self, do_posp=False, scale=None):
        "Plotting iso-cost contours up to 3 dimensions"
        scale = scale if (scale is not None) else self.base_scale
        if not os.path.isdir( self.plots_dir ):
            os.makedirs( self.plots_dir )
        if self.Dim <=3: # More than 3 dimensional contours cannot be visualized
            random_p_val = self.exec_specific['random_p_d']-1
            IC_indices = sorted( set(range(random_p_val, self.random_p_IC_count, self.exec_specific['random_p_d'])).union({(self.random_p_IC_count-1)}) )
            if   self.Dim == 1:
                fig = plt.figure()
                ax = fig.add_subplot(111)
                plt.axvline( 1.0 ,color='k',linestyle='-') # Black vertical line at 1.0 selectivity
                p2h_m = {} # Plan_id to plot_handle mapping
                X_tck = self.sel_range_o_inc if self.epp_dir[0]>0 else self.sel_range_o_dec
                cmin_handle = plt.axhline( self.C_min ,xmin=0.0, xmax=1.0, color='y', linestyle='--' )
                for IC_ix in IC_indices:
                    contours_handle = plt.axhline( self.id2c_m[(IC_ix, scale)] ,xmin=0.0, xmax=1.0,color='k', linestyle='--' )
                    for plan_id in self.iad2p_m[(IC_ix, self.anorexic_lambda, scale)]:
                        self.iapd2s_m[(IC_ix, self.anorexic_lambda, plan_id, scale)] = self.load_points(IC_ix, self.anorexic_lambda, plan_id, scale)
                        X_ls = ( list(self.iapd2s_m[(IC_ix, self.anorexic_lambda, plan_id, scale)]) [0][0] , )
                        Y_ls = ( self.id2c_m[(IC_ix, scale)] , )
                        self.iapd2s_m[(IC_ix, self.anorexic_lambda, plan_id, scale)] = set()
                        plan_handle = plt.scatter( X_ls , Y_ls, c = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] , s=None )
                        p2h_m[plan_id] = plan_handle
                if do_posp: # Merging Cost daigram of each plan Bouqet diagram
                    if ('build_posp' not in self.exec_specific) or (not self.exec_specific['build_posp']):
                        self.exec_specific['build_posp'] = False
                        self.build_posp(scale)
                    p2cl_m = {} # Cost list for entire selectivity space for each plan stored here
                    # Finding Plan Diagram of optimal plans at each location in ESS
                    ess_plan_diagram, ess_cost_diagram = np.zeros((self.resolution_o,)*self.Dim), np.ones((self.resolution_o,)*self.Dim)*np.inf
                    for plan_id in self.d2o_m[scale]:
                        p2cl_m[plan_id] = np.zeros((self.resolution_o,)*self.Dim)
                        epp_ix_iterator = itertools.product(*[ list(range(self.resolution_o)) ]*self.Dim)
                        for sel_ix_ls in epp_ix_iterator:
                            sel = self.build_sel(sel_ix_ls, mode='o')
                            cost_val = self.cost(sel, plan_id=plan_id, scale=scale)
                            p2cl_m[plan_id][sel_ix_ls] = cost_val
                            if cost_val < ess_cost_diagram[sel_ix_ls]:
                                ess_plan_diagram[sel_ix_ls] = plan_id
                                ess_cost_diagram[sel_ix_ls] = cost_val
                        plan_handle = plt.plot( X_tck , p2cl_m[plan_id] , color = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] )[0]
                        p2h_m[plan_id]  = plan_handle
                        del p2cl_m[plan_id]
                # plt.xticks(X_tck) # ; plt.yticks(Y_tck)
                plt.xscale('log')
                plt.yscale('log')
                plt.legend( [cmin_handle,contours_handle,*p2h_m.values()] , [ r'$C_{%s}$'%('min'), r'$IC_{%s}$'%('contour'), *[ r'$P_{%s}$'%(str(plan_id)) for plan_id in p2h_m.keys() ] ] , loc='upper right', bbox_to_anchor=(1.25, 1.0) )
                plt.xlim(0.0, 1.0) # ; plt.ylim(0.0, 1.0)
                plt.xlabel( '\n'.join(('Selectivity',self.epp[0]))  )
                plt.ylabel( 'Cost')
                # plt.title(r'$POSP$'+' '+r'$Performance$')
                # plt.title(' '.join((r'$POSP$',r'$Performance$'))))
                plt.title( ' '.join((   r'$POSP$' , r'$Performance$' , r'$%s$'%(self.benchmark) , r'$%sGB$'%(str(scale)) , r'$Q_{%s}$'%(self.query_id)   )) )
                plt.grid(True, which='both')
                # ax.set_xscale('log') ; # ax.set_yscale('log')
                plt.savefig( os.path.join( self.plots_dir, '1D-ESS {}GB {}.PNG'.format(scale, ('posp' if do_posp else 'regular')) ) , format='PNG' , dpi=600 , bbox_inches='tight' )
                # plt.show()
            elif self.Dim == 2:
                X_tck = self.sel_range_o_inc if self.epp_dir[0]>0 else self.sel_range_o_dec
                Y_tck = self.sel_range_o_inc if self.epp_dir[1]>0 else self.sel_range_o_dec
                if do_posp: # Created 3D plot of cost & 2D plan diagram
                    # Drawing 3D Cost Diagram
                    fig = plt.figure()
                    ax = fig.add_subplot(111, projection='3d')
                    p2h_m = {} # Plan_id to plot_handle mapping
                    mX, mY = np.meshgrid(X_tck, Y_tck)
                    cmin_hand = ax.plot_surface(mX, mY, (np.log(self.C_min)/np.log(r_ratio))*np.ones(mX.shape), color = 'y', linewidth=0, antialiased=False)
                    for IC_ix in IC_indices:
                        contours_handle = ax.plot_surface(mX, mY, (np.log(self.id2c_m[(IC_ix, scale)])/np.log(r_ratio))*np.ones(mX.shape), color = 'k', linewidth=0, antialiased=False)
                    if ('build_posp' not in self.exec_specific) or (not self.exec_specific['build_posp']):
                        self.exec_specific['build_posp'] = False
                        self.build_posp(scale)
                    # Finding Plan Diagram of optimal plans at each location in ESS
                    ess_plan_diagram, ess_cost_diagram = np.zeros((self.resolution_o,)*self.Dim), np.ones((self.resolution_o,)*self.Dim)*np.inf
                    for plan_id in self.d2o_m[scale]:
                        epp_ix_iterator = itertools.product(*[ list(range(self.resolution_o)) ]*self.Dim)
                        for sel_ix_ls in epp_iterator:
                            sel = self.build_sel(sel_ix_ls, mode='o')
                            cost_val = self.cost(sel, plan_id=plan_id, scale=scale)
                            if cost_val < ess_cost_diagram[sel_ix_ls[0]]:
                                ess_plan_diagram[sel_ix_ls] = plan_id
                                ess_cost_diagram[sel_ix_ls] = cost_val
                    mX_ravel, mY_ravel, ess_plan_diagram_ravel, ess_cost_diagram_ravel =  np.ravel(mX), np.ravel(mY), np.ravel(ess_plan_diagram), np.ravel(ess_cost_diagram)
                    for plan_id in self.d2o_m[scale]:
                        bool_arr = (ess_plan_diagram_ravel==plan_id)
                        X_ls, Y_ls, plans_ls = mX_ravel[bool_arr], mY_ravel[bool_arr], ess_cost_diagram_ravel[bool_arr]
                        plan_handle = ax.scatter( X_ls, Y_ls, plans_ls , color = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] )
                        p2h_m[plan_id]  = plan_handle
                    plt.xticks(X_tck) ; plt.yticks(Y_tck)
                    plt.legend( [cmin_handle,contours_handle,*p2h_m.values()] , [ r'$C_{%s}$'%('min'), r'$IC_{%s}$'%('contour'), *[ r'$P_{%s}$'%(str(plan_id)) for plan_id in p2h_m.keys() ] ] , loc='upper right', bbox_to_anchor=(1.25, 1.0) )                        
                    plt.xlim(0.0, 1.0) ; plt.ylim(0.0, 1.0)
                    ax.set_xlabel( '\n'.join(('Selectivity',self.epp[0])) )
                    ax.set_ylabel( '\n'.join(('Selectivity',self.epp[1])) )
                    ax.set_zlabel( 'Cost (log-scale)')
                    plt.title('Cost Diagram')
                    plt.savefig( os.path.join( self.plots_dir, '2D-ESS Cost Diagram {}GB {}.PNG'.format(scale, ('posp' if do_posp else 'regular')) ) , format='PNG' , dpi=600 , bbox_inches='tight' )
                    # plt.show()
                    plt.close() ; plt.clf()
                    # Drawing 2D Plan Diagram
                    plt.axvline( 1.0 ,color='k',linestyle='-') # Black vertical   line at 1.0 selectivity
                    plt.axhline( 1.0 ,color='k',linestyle='-') # Black horizontal line at 1.0 selectivity
                    p2h_m = {} # Plan_id to plot_handle mapping, to be cleared for next plot
                    for plan_id in self.d2o_m[scale]:
                        bool_arr = (ess_plan_diagram_ravel==plan_id)
                        X_ls, Y_ls = mX_ravel[bool_arr], mY_ravel[bool_arr]
                        plan_handle = plt.scatter( X_ls, Y_ls, c = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] , s=None )
                        p2h_m[plan_id]  = plan_handle
                    plt.xticks(X_tck) ; plt.yticks(Y_tck)
                    plt.legend( [*p2h_m.values()] , [ *[ r'$P_{%s}$'%(str(plan_id)) for plan_id in p2h_m.keys() ] ] , loc='upper right', bbox_to_anchor=(1.2, 1.0) )                        
                    plt.xlim(0.0, 1.0) ; plt.ylim(0.0, 1.0)
                    plt.xlabel( '\n'.join(('Selectivity',self.epp[0])) )
                    plt.ylabel( '\n'.join(('Selectivity',self.epp[1])) )
                    plt.grid(True)
                    plt.title('Plan Diagram')
                    plt.savefig( os.path.join( self.plots_dir, '2D-ESS Plan Diagram {}GB {}.PNG'.format(scale, ('posp' if do_posp else 'regular')) ) , format='PNG' , dpi=600 , bbox_inches='tight' )
                    # plt.show()
                else:
                    plt.axvline( 1.0 ,color='k',linestyle='-') # Black vertical   line at 1.0 selectivity
                    plt.axhline( 1.0 ,color='k',linestyle='-') # Black horizontal line at 1.0 selectivity
                    fig = plt.figure()
                    ax = fig.add_subplot(111)
                    for IC_ix in IC_indices:
                        contour_points = set().union( *(self.load_points(IC_ix, self.anorexic_lambda, plan_id, scale) for plan_id in self.iad2p_m[(IC_ix, self.anorexic_lambda, scale)]) )
                        for plan_id in self.iad2p_m[(IC_ix, self.anorexic_lambda, scale)]:
                            plan_points = self.load_points(IC_ix, self.anorexic_lambda, plan_id, scale)
                            # List containing Plan_id or None at non-optimal points of contour
                            sorted_point_ls = np.array(sorted( contour_points,key=lambda point:(+1*point[0],-1*point[1]) )) # ASC on X, DSC on Y
                            sorted_plan_ls  = [ (plan_id if point in plan_points else None) for point in sorted_point_ls ]
                            plan_handle = plt.plot( sorted_point_ls.T[0] , sorted_point_ls.T[1], color = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] )[0]
                            p2h_m[plan_id] = plan_handle
                            # Text & end points of continuous plan region as scatter
                            srch_ix = 0
                            while srch_ix < len(plan_points):
                                srch_ix_reset = False
                                while (srch_ix < len(plan_points)) and (srch_plan_ls[srch_ix] is None):
                                    srch_ix += 1
                                    srch_ix_set = True
                                if srch_ix_reset:
                                    continue
                                continuous_ix= []
                                while (srch_ix < len(plan_points)) and (sorted_plan_ls[srch_ix] is not None):
                                    continuous_points.append( srch_ix )
                                    srch_ix += 1
                                x_pos, y_pos = continuous_points[len(continuous_points)//2] # Median of continuous points of same planb on present contour
                                # Ends have to be shown explicitly, else single point will be missed
                                plan_handle = plt.scatter( (x_pos,) , (y_pos), c = list(mcolors.TABLEAU_COLORS.keys())[plan_id%len(mcolors.TABLEAU_COLORS)] , s=None )
                                ax.text(x_pos, y_pos, r'$P_{%s}$'%(str(plan_id)), fontsize=10)
                    plt.xticks(X_tck) ; plt.yticks(Y_tck)
                    plt.legend( [cmin_handle, contours_handle,*p2h_m.values()] , [ r'$C_{%s}$'%('min'), r'$IC_{%s}$'%('contour'), *[ r'$P_{%s}$'%(str(plan_id)) for plan_id in p2h_m.keys() ] ] , loc='upper right', bbox_to_anchor=(1.2, 1.0) )                        
                    plt.xlim(0.0, 1.0) ; plt.ylim(0.0, 1.0)
                    ax.set_xlabel( '\n'.join(('Selectivity',self.epp[0])) )
                    ax.set_ylabel( '\n'.join(('Selectivity',self.epp[1])) )
                    ax.set_zlabel( 'Cost (log-scale)')
                    plt.title('Cost Diagram')
                    plt.savefig( os.path.join( self.plots_dir, '2D-ESS Contours Diagram {}GB {}.PNG'.format(scale, ('posp' if do_posp else 'regular')) ) , format='PNG' , dpi=600 , bbox_inches='tight' )
                    # plt.show()
            elif self.Dim == 3:
                pass # Some contour plotting can be done in future here

    def run(self, scale=None):
        "Method to Combine, Simulate and Evaluate Plan Bouquet"
        scale = scale if (scale is not None) else self.base_scale
        if self.bouquet_runnable:
            if scale not in self.exec_specific:
                self.exec_specific[scale] = {}
            self.load_maps()
            if ('base_gen' not in self.exec_specific) or (not self.exec_specific['base_gen']):
                self.exec_specific['base_gen'] = False
                self.base_gen( scale=self.base_scale )
            self.simulation_result = self.simulate( act_sel=(sel_range_p[len(self.epp)][-1],)*len(self.epp) , scale=self.base_scale )
            self.save_maps()
            self.plot_contours(do_posp=True, scale=scale)
            # self.evaluate()

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




'######## MAIN EXECUTION ########'

if __name__=='__main__':
    # Creating values of selectivities on each axis to be varied (keyed(indexed) from 1, as Dimensions can't start from 0)
    sel_range_o, sel_range_p = {}, {}
    for ix in range(max(len(resolution_o),len(resolution_p))):
        res_o, res_p = resolution_o[ix], resolution_p[ix]
        if zero_sel:
            res_o, res_p = res_o-1, res_p-1
        if progression=='GP':
            sel_ratio_o , sel_ratio_p = np.exp( np.log(max_sel/min_sel) / (res_o-1) )    , np.exp( np.log(max_sel/min_sel) / (res_p-1) )
            sel_ls_o    , sel_ls_p    = [(min_sel*sel_ratio_o**i) for i in range(res_o)] , [(min_sel*sel_ratio_p**i) for i in range(res_p)]
        else: # progression=='AP':
            sel_diff_o  , sel_diff_p = (max_sel-min_sel) / (res_o-1) , (max_sel-min_sel) / (res_p-1)
            sel_ls_o    , sel_ls_p   = [ min_sel+i*sel_diff_o for i in range(res_o)] , [ min_sel+i*sel_diff_p for i in range(res_p)]
        if zero_sel:
            sel_ls_o.insert( 0, 0.0 )
            sel_ls_p.insert( 0, 0.0 )
        sel_ls_o , sel_ls_p = np.array( sel_ls_o ) , np.array( sel_ls_p )
        if sel_round is not None:
            sel_ls_o , sel_ls_p = np.round( sel_ls_o, sel_round ) , np.round( sel_ls_p, sel_round )
        sel_range_o[ix+1], sel_range_p[ix+1] = sel_ls_o , sel_ls_p

    obj_ls, stderr = [], my_open(os.path.join(master_dir,'stderr.txt'),'w')
    for query_name in my_listdir( os.path.join(master_dir,benchmark,'sql') ):
        query_id = query_name.split('.')[0].strip()
        obj_ls.append( ScaleVariablePlanBouquet(benchmark,query_id,base_scale,exec_scale,db_scales,stderr) )


    obj = obj_ls[0]
    run(obj)

    # try:
    #     with multiprocessing.Pool(processes=CPU) as pool:
    #         for i in pool.imap_unordered(run,obj_ls):
    #             continue
    # except:
    #     pass
    # finally:
    #     stderr.close()

    stderr.close()