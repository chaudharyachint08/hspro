'Basic'
'''
Syntax for Seletivity Injection
explain selectivity (' and '.join(PREDICATE_LIST))(' , '.join(SELECTIVITY_LIST)) SQL_QUERY

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
parser.add_argument("--run_main"           , type=eval , dest='run_main'           , default=False)
# Int Type Arguments
parser.add_argument("--scale"               , type=eval , dest='scale'               , default=4)
parser.add_argument("--disk_batches_limit"  , type=eval , dest='disk_batches_limit'  , default=None)
# Float Type Arguments
parser.add_argument("--lr"                  , type=eval , dest='lr'                  , default=0.001) # Initial learning rate
# String Type Arguments
parser.add_argument("--con_choice"             , type=str  , dest='con_choice'             , default='baseline_con')
parser.add_argument("--adv_loss"               , type=str  , dest='adv_loss'               , default=None)
parser.add_argument("--train_strategy"         , type=str  , dest='train_strategy'         , default='cnn') # other is 'gan'
parser.add_argument("--plots"                  , type=str  , dest='plots'                  , default=os.path.join('.','..','experiments','training_plots'))
# Tuple Type Arguments
parser.add_argument("--channel_indx"           , type=eval , dest='channel_indx'           , default=(0,1,2))

args, unknown = parser.parse_known_args()
globals().update(args.__dict__)

######## GLOBAL INITIAL VARIABLES ENDS ########
