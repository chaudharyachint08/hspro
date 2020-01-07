import json,os,time
import multiprocessing

import psycopg2
from psycopg2.extras import RealDictCursor

indir  = 'executable_queries'
outdir = 'JSONs'

if outdir not in os.listdir():
    os.mkdir(outdir)

CPUs = int(input('Enter Number of CPUs you have~ '))

START = time.time()

def put_json(arg):
    'It will create cursor, execute given query & write json to desired file'
    name,query = arg

    try:
        connection = psycopg2.connect(user = "postgres",
                                  password = "database",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "tpcds1")
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        init = time.time()
        while True:
            try:
                succ = open('success','a')
            except:
                continue
            else:
                break
        wait = time.time()-init
        succ.write('Starting '+' , '.join((name,str(wait)))+'\n')
        succ.close()

        start = time.time()
        cursor.execute(query)
        end = time.time()
        total = end-start
        while True:
            try:
                succ = open('success','a')
            except:
                continue
            else:
                break
        wait = time.time()-end
        succ.write('Executed '+' , '.join((name,str(total),str(wait)))+'\n')
        succ.close()

    except Exception as err:
        start = time.time()
        while True:
            try:
                log = open('log','a')
            except:
                continue
            else:
                break
        end = time.time()
        wait = end-start
        log.write(' , '.join((name,str(wait)))+'\n'+str(err)+'\n')
        log.close()
    else:
        json_name = '/'.join((outdir,name[:-4]+'.json'))        
        result = cursor.fetchone()
        f = open(json_name,'w')
        f.write(json.dumps(result, indent=2))
        f.close()
    finally:
        cursor.close()
        connection.close()


query_list = []
search_in = set(os.listdir(outdir))
file_list = sorted(x for x in os.listdir(indir) if x.split('.')[0]+'.json' not in search_in )
'''
tmp = []
memo = {}
for i in file_list:
    _ = i.split('(')[0]
    if _ not in memo:
        memo[_] = [1,[i]]
    elif memo[_][0]<1:
        memo[_][0]+=1
        memo[_][1].append(i)
for i in memo:
    tmp.extend(memo[i][1])
file_list = tmp
print(len(file_list))
'''
count = 0
for name in file_list:
    f = open('/'.join((indir,name)))
    query = f.read()
    f.close()
    flagged_query = '''EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) {0}'''.format(query)
    query_list.append((name,flagged_query))
    count+=1
    if count%1000==0:
        print(count//1000,end=',')

print('\n',len(query_list),'Queries Remaining to be Executed')

try:
    with multiprocessing.Pool(CPUs) as pool:
        for i in pool.imap_unordered(put_json,query_list):
            continue
except Exception as err:
    print(err,'This statement should never get printed')

print(time.time()-START)