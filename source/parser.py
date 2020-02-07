'''
Parser file to create CSV corresponding to each JSON
'''

import json,os,multiprocessing,copy,math
from S2E_metadata import qm_rltn_attr_ls as R2A

indir   = 'JSONs_B'
outdir  = 'CSVs_B'
config  = 'postgresql_B.conf'

if outdir not in os.listdir():
    os.mkdir(outdir)

for i in open(config).readlines():
    exec(i.strip())

#Node Types as Features
node_types = ['Aggregate','CTE Scan','Group','Hash','Hash Join','Materialize',
'Merge Join','Nested Loop','Seq Scan','Sort','Subquery Scan','Unique','WindowAgg']
node_class = ['Unary|Binary']#,'Base Selection','Between|IN']
#(Unary, can be selection, Between|IN),(Binary cannot be selection)

#Configurations of DBMS as Features
db_config  = ['max_connections','shared_buffers','effective_cache_size',
'maintenance_work_mem','checkpoint_completion_target','wal_buffers',
'default_statistics_target','random_page_cost','effective_io_concurrency',
'work_mem','min_wal_size','max_wal_size','max_worker_processes']

#Attribute as Features
freq_rltn_attr     = [   '_'.join(('freq',y))    for x,y in R2A ]
#min_rltn_attr      = [ '_'.join(('min',y))  for x,y in R2A ]
#max_rltn_attr      = [ '_'.join(('max',y))  for x,y in R2A ]
distinct_rltn_attr = [ '_'.join(('dis',y))  for x,y in R2A ]
null_rltn_attr     = [ '_'.join(('null',y)) for x,y in R2A ]#if R2A[(x,y)][-1][0]=='NULL' ]
#Fabien Features
fabien_features    = ['OPs']
#Custom features
custom_features    = ['DEPTH','SS','NLJ','HJ','MJ','CTES','MAT','AGG','WAGG']
#Estimated Cardinality
est_card           = ['Actual Loops','Plan Width','Plan Rows',]
#Execution Only Outputs
execution_outputs  = ['Actual Rows','Estimation Factor','Q Error']

#List the desired order of features for CSV files
features_order     = node_types+node_class+db_config+ \
freq_rltn_attr+distinct_rltn_attr+null_rltn_attr+ \
fabien_features  + custom_features + \
est_card+execution_outputs

features_order = { features_order[x]:x for x in range(len(features_order)) }

NT_alias = {'Seq Scan':'SS','Nested Loop':'NLJ','Hash Join':'HJ','Merge Join':'MJ',
    'CTE Scan':'CTES','Materialize':'MAT','Aggregate':'AGG','WindowAgg':'WAGG'}

def parse(height,node_dict,parent_data,out):
    'Write data on csv file, based on data of subtree, called node is root of'
    #OPTIONAL, Initially sent empty sent to each child
    features,subtree_data,node_data = {},{},{}

    #Gathering Data from child subtrees
    childs_data = {}
    if 'Plans' in node_dict:
        #Traversing all Children recursively & adding features to subtree_data
        for i in range(len(node_dict['Plans'])):
            childs_data[i] = parse(height+1,node_dict['Plans'][i],node_data,out)
            for x in childs_data[i]:
                if x=='DEPTH':
                    if 'DEPTH' not in subtree_data:
                        subtree_data['DEPTH'] = childs_data[i]['DEPTH']
                    else:
                        subtree_data['DEPTH'] = max(subtree_data['DEPTH'],childs_data[i]['DEPTH'])
                else:
                    if x not in subtree_data:
                        subtree_data[x] = childs_data[i][x]
                    else:
                        subtree_data[x] += childs_data[i][x]
        subtree_data['DEPTH']+=1
        #OPTIONAL: When all Children are traversed, they are not needed
        #del node_dict['Plans']
    else:
        subtree_data['DEPTH']=height
    if 'OPs' in subtree_data:
        subtree_data['OPs']+=1
    else:
        subtree_data['OPs']=1

    if node_dict['Node Type'] in NT_alias and NT_alias[node_dict['Node Type']] not in subtree_data:
        subtree_data[NT_alias[node_dict['Node Type']]] = 1
    elif node_dict['Node Type'] in NT_alias:
        subtree_data[NT_alias[node_dict['Node Type']]] += 1

    for i in custom_features:
        if i not in features:
            features[i] = 0

    copy_ls = ['Plan Rows','Plan Width','Actual Rows','Actual Loops']
    for i in copy_ls:
        features[i] = node_dict[i]
    try:
        features['Estimation Factor'] = features['Actual Rows']/features['Plan Rows']
    except ZeroDivisionError:
        if features['Actual Rows'] == 0 :
            features['Estimation Factor'] = features['Q Error'] = 1
        else:
            features['Estimation Factor'] = features['Q Error'] = math.inf
    else:
        try:
            features['Q Error'] = max(features['Estimation Factor'],1/features['Estimation Factor'])
        except ZeroDivisionError:
            features['Q Error'] = math.inf

    features['Unary|Binary'] = 1 if (('Plans' in node_dict) and (len(node_dict['Plans'])>1)) else 0


    for i in subtree_data:
        features[i] = subtree_data[i]
    for i in db_config:
        features[i] = eval(i)
    for NT in node_types:
        features[NT] = 0
    features[node_dict['Node Type']] = 1

    _ = str(node_dict)
    for x,y in R2A:
        features[ '_'.join(('freq',y)) ] = _.count(y)
    for x,y in R2A:
        features[ '_'.join(('dis',y)) ] = len(R2A[(x,y)])
    for x,y in R2A:
        features[ '_'.join(('null',y)) ] = 1 if R2A[(x,y)][-1][0]=='NULL' else 0

    #Code for Writing features as rows to 'out' file handle
    if features: #any(features[x] for x in NT_alias):
        if len(features)!=len(features_order):
            print('FUCK!!')
            return Exception()
        out.write(','.join(str(features[x]) for x in sorted(features,key = lambda x:features_order[x]))+'\n')

    #Returning data of subtree to it's parent
    return subtree_data


def process(name):
    'Loads json from file, & call the root node for parsing'
    try:
        with open('/'.join((indir, name))) as infile:
            dict_str = json.load(infile)
    except Exception as err:
        while True:
            try:
                log = open('parsing_log','a')
            except:
                continue
            else:
                log.write(','.join(('Error Loading',name,str(err)))+'\n')
                log.close()
                break
    else:
        outfile = '/'.join((outdir,name.split('.')[0]+'.csv'))
        out = open(outfile,'w')
        out.write(','.join(sorted(features_order,key = lambda x:features_order[x]))+'\n')
        parse(0,dict_str['QUERY PLAN'][0]['Plan'],{},out)
        out.close()


#CPUs = int(input('Enter Number of CPUs you have'))
json_ls = sorted(os.listdir(indir))


#Calling Parser on json files in parallel (not on WINDOWS)
'''
with multiprocessing.Pool(CPUs) as pool:
    for i in pool.imap_unordered(process,json_ls):
        continue
'''
#WINDOWS only serial execution of parser
_ = list(map(process,json_ls))





#FINDING COMMON MEMBERS OF EACH NODE TYPE
'''
NT2M = {}

    if node_dict['Node Type'] not in NT2M:
        NT2M[node_dict['Node Type']] = [1,{}]
    else:
        NT2M[node_dict['Node Type']][0]+=1
    for member in node_dict:
        if member not in ('Plans','Node Type'):
            if member not in NT2M[node_dict['Node Type']][1]:
                NT2M[node_dict['Node Type']][1][member] = [1,{str(node_dict[member])}]
            else:
                NT2M[node_dict['Node Type']][1][member][0] += 1
                NT2M[node_dict['Node Type']][1][member][1].add(str(node_dict[member]))


_ = {NT:{member:NT2M[NT][1][member][1] for member in NT2M[NT][1] if NT2M[NT][1][member][0]==NT2M[NT][0]} for NT in NT2M}
    
_2 = {}
for NT in _:
    for j in _[NT]:
        if j not in _2:
            _2[j] = [1,_[NT][j]]
        else:
            _2[j][0] += 1
            _2[j][1].update(_[NT][j])

commonM = {member for member in _2 if _2[member][0]==len(_)}

_ = {x:{member:_[x][member] for member in _[x] if member not in commonM} for x in _}

print(_.keys())
print(_2.keys())


_  = {x:{member:_[x][member] for member in _[x] if len(_[x][member])<=10}for x in _}
_2 = {x:_2[x] for x in _2 if len(_2[x])<=10}
#NT2M = _

for NT in _:
    print(NT)
    for j in _[NT]:
        print("\t",j,_[NT][j])
    print('')





'Below are 13 functions for specific operator to perform feature extraction'
def Aggregate():
    pass

def CTE_Scan():
    pass

def Group():
    pass

def Hash():
    pass

def Hash_Join():
    pass

def Materialize():
    pass

def Merge_Join():
    pass

def Nested_Loop():
    pass

def Seq_Scan():
    pass

def Sort():
    pass

def Subquery_Scan():
    pass

def Unique():
    pass

def WindowAgg():
    pass

'Common operator feature generation node'
def common():
    pass

'''