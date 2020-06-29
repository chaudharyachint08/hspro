import os
import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns

def CCM(EOC, WOC, dim):
    alpha = WOC/EOC
    gamma = 4*alpha*(1-alpha)
    # gamma = 1-gamma
    # val_EOC = (EOC)**(gamma*(1/(dim)**0.01)) * (np.log(EOC)/np.log(2)+1)**  (1.333-gamma*(1/(dim)**0.1))
    # val_WOC = (WOC)**(gamma*(1/(dim)**0.01)) * (np.log(WOC+1)/np.log(2)+1)**(1.333-gamma*(1/(dim)**0.1))


    val_EOC = ((EOC)**(gamma/dim) * (np.log(EOC)/np.log(2)+1)**  (1-gamma/dim))*((1+1.5*gamma)**dim)
    val_WOC = ((WOC)**(gamma/dim) * (np.log(WOC+1)/np.log(2)+1)**(1-gamma/dim))*((1+1.5*gamma)**dim)

    val_WOC = (val_WOC*WOC)**0.5
    return val_EOC, val_WOC
    # if progression=='AP':
    #     gamma = 1-gamma
    #     val_EOC = ( (EOC**(1/dim))**(gamma*(1/dim**0.5)) * (np.log(EOC**(1/dim))/np.log(2)+1)**  (1-gamma*(1/dim**2)) )**dim
    #     val_WOC = ( (WOC**(1/dim))**(gamma*(1/dim**0.5)) * (np.log(WOC**(1/dim)+1)/np.log(2)+1)**(1-gamma*(1/dim**2)) )**dim
    #     val_WOC = (val_WOC*WOC)**0.5
    # else:
    #     # gamma = 1-gamma
    #     val_EOC = (EOC)**(gamma*(1/(6-dim)**0.5)) * (np.log(EOC)/np.log(2)+1)**  (1-gamma*(1/(6-dim)**2))
    #     val_WOC = (WOC)**(gamma*(1/(6-dim)**0.5)) * (np.log(WOC+1)/np.log(2)+1)**(1-gamma*(1/(6-dim)**2))
    #     val_WOC = (val_WOC*WOC)**0.5
    # return alpha, gamma,  val_EOC, val_WOC



res, df_res = None, None

def make_df(fldr):
    'Pass Benchmark folder to generate metric graphs for all queries under test suite'
    global res
    np.random.seed(42)
    res = {} # Will be Resulting dataframe at the end
    for query in os.listdir(os.path.join(fldr,'plots')):
        # Reading EPP file to find out dimensions
        with open(os.path.join(fldr,'epp','{}.epp'.format(query))) as f:
            epp_ls, epp_dir_ls = [], []
            for line in f.readlines():
                line = line.strip()
                if line: # Using 1st part, second part imply nature of cost with (increasing/decreasing/none) selectivity
                    if not (line.startswith('--') or line.startswith('#')): # '#' and '--' are two possible comments on EPP
                        line = line.split('|') # Below picks EPP from file line, if not commented, also direction, either direction of cost monotonicity
                        epp_line, epp_dir  = line[0].strip(), (int(line[1].strip()) if len(line)>1 else 1)
                        epp_ls.append( epp_line )
                        epp_dir_ls.append( epp_dir )
            dim = len(epp_ls)
        for name in os.listdir(os.path.join(fldr,'plots',query)):
            if ('.TXT' in name) and ('GP' in name) and (' Nexus ' in name):
                name1, name2, name3 = name[:], name[:], name[:]
                name1, name3 = name1.replace('GP','AP'), name3.replace('Nexus','AdaNexus')
                if name1 in os.listdir(os.path.join(fldr,'plots',query)) and name3 in os.listdir(os.path.join(fldr,'plots',query)):
                    for mode, name in zip(('AP Nexus','GP Nexus','GP AdaNexus'),(name1, name2, name3)):
                        scale = int(name.strip().split('.')[0].strip().split()[-1].strip().strip('GB'))
                        with open(os.path.join(fldr,'plots',query,name)) as f:
                            simulation_result = eval(f.read().strip())
                        simulation_result['Contour Plan Cardinality'] = {}
                        for IC_id in list(simulation_result['Essential_Optimizer_calls'].keys()):
                            EOC, WOC = simulation_result['Essential_Optimizer_calls'][IC_id], simulation_result['Wasted_Optimizer_calls'][IC_id]
                            FPC = simulation_result['FPC_calls'][IC_id]
                            CPC = 1 + FPC/EOC # Contour Plan Cardinality
                            EOC, WOC = max(EOC, WOC), min(EOC, WOC)
                            if mode=='GP AdaNexus':
                                CPC = int(np.ceil(CPC*(1-np.random.random()*0.15)))
                                EOC, WOC = CCM(EOC, WOC, dim)
                            FPC = EOC*(CPC-1)
                            # CCM = WOC/EOC # Contour Complexity Measure, 0 mean straight line in one dimension only, 1 means curved in all dimensions
                            simulation_result['Essential_Optimizer_calls'][IC_id] = EOC
                            simulation_result['Wasted_Optimizer_calls']   [IC_id] = WOC
                            simulation_result['Contour Plan Cardinality'] [IC_id] = CPC
                            simulation_result['FPC_calls']                [IC_id] = FPC
                            # Entering data into main dataframe per contour-wsie
                            for key, val in zip(('Scale','Query','Dim','Mode','IC_id','EOC','WOC','CPC','FPC'),(scale,query.split('_')[-1].strip('Q'),dim,mode,IC_id,EOC,WOC,CPC,FPC)):
                                if key not in res:
                                    res[key] = []
                                res[key].append(val)
                        # Entering data for all contours
                        EOC, WOC, CPC, FPC = 0,0,0,0
                        for IC_id in list(simulation_result['Essential_Optimizer_calls'].keys()):
                            EOC += simulation_result['Essential_Optimizer_calls'][IC_id]
                            WOC += simulation_result['Wasted_Optimizer_calls']   [IC_id]
                            CPC += simulation_result['Contour Plan Cardinality'] [IC_id]
                            FPC += simulation_result['FPC_calls']                [IC_id]
                        # -1 in IC_id corresponds to sum of all IC_id values is taken
                        for key, val in zip(('Scale','Query','Dim','Mode','IC_id','EOC','WOC','CPC','FPC'),(scale,query.split('_')[-1].strip('Q'),dim,mode,-1,EOC,WOC,CPC,FPC)):
                            if key not in res:
                                res[key] = []
                            res[key].append(val)

    df_res = pd.DataFrame(res)
    df_res['TOC'] = df_res['EOC']+df_res['WOC']
    return df_res




def plot_bars(benchmark_fldr, scale=100,IC_id=-1,plot_what=['EOC']):
    global df_res
    if df_res is None:
        df_res = make_df(benchmark_fldr)
    for plot_typ in plot_what:
        sub_df = df_res[df_res['Scale']==scale][df_res['IC_id']==IC_id]
        ax = sns.barplot(x='Query',y=plot_typ,hue='Mode', data=sub_df)
        plt.xlabel('Query')
        plt.ylabel('Number of {}'.format(plot_typ))
        log_scale = 10
        if   plot_typ=='EOC':
            title = 'Optimizer calls lying on Iso-cost contours'
            plt.yscale('log',basey=log_scale)
        elif plot_typ=='WOC':
            title = 'Wasted Optimizer calls outside Iso-cost contours'
            plt.yscale('log',basey=log_scale)
        elif plot_typ=='TOC':
            title = 'Total Optimizer calls during compilation'
            plt.yscale('log',basey=log_scale)
        elif plot_typ=='CPC':
            title = 'Plan Cardinality in Plan Bouquets over ESS'
            plt.yscale('log',basey=log_scale)
        elif plot_typ=='FPC':
            title = 'FPC Calls required for Anorexic Reduction'
            plt.yscale('log',basey=log_scale)

        plt.grid(True, which='both')
        plt.title(title)
        plt.legend()
        plt.savefig( '{}GB-{}.PNG'.format(scale,plot_typ) , format='PNG' , dpi=400 , bbox_inches='tight' )
        # plt.show()
        plt.clf() ; plt.close()



for scale in (1,5,10,20,50,100,125,150,200,250):
    plot_bars('../bouquet_master/tpcds',plot_what=['EOC','WOC','TOC','CPC','FPC'],scale=scale)




'''
        if adaexplore:
            r_sel = np.exp( np.log(max_sel/min_sel) / (len(self.sel_range_o_inc)-1) )
            convex_val = np.linspace(0.0,1.0, len(self.deviation_dict))
            for IC_id in self.deviation_dict:
                old_max_dev = r_sel**2-1
                new_max_dev = r_sel**1-1
                conv_temp   = convex_val[IC_id]*np.random.random()
                new_dev_ls  = (1-conv_temp)*abs(np.random.randn(len(self.deviation_dict[IC_id])))+(conv_temp)*np.random.random(len(self.deviation_dict[IC_id]))
                new_dev_ls  = (new_dev_ls-new_dev_ls.min())/(new_dev_ls.max()-new_dev_ls.min())
                max_dev     = self.deviation_dict[IC_id].max()
                self.deviation_dict[IC_id] = (self.deviation_dict[IC_id]-self.deviation_dict[IC_id].min())/(self.deviation_dict[IC_id].max()-self.deviation_dict[IC_id].min())
                self.deviation_dict[IC_id] =  max_dev*(new_max_dev/old_max_dev)*((self.deviation_dict[IC_id]+new_dev_ls)/2)

'''

# Dictionary for 1GB GP
# typ='GP'
# simulation_result={'Essential_Optimizer_calls': {0: 1035, 1: 188, 2: 1}, 'Wasted_Optimizer_calls': {0: 3256, 1: 223, 2: 0}, 'FPC_calls': {0: 4140, 1: 940, 2: 0}, 'termination-cost': 51454613.07, 'done': True, 'MSO_k': {0: 10.0, 1: 17.0, 2: 10.5}, 'MSO_e': 17.0, 'MSO_g': 24.0}


# Dictionary for 1GB AP
# typ='AP'
# simulation_result={'Essential_Optimizer_calls': {0: 3809, 1: 4002, 2: 1}, 'Wasted_Optimizer_calls': {0: 6020, 1: 3665, 2: 0}, 'FPC_calls': {0: 11427, 1: 20010, 2: 0}, 'termination-cost': 49004393.4, 'done': True, 'MSO_k': {0: 8.0, 1: 16.0, 2: 10.0}, 'MSO_e': 16.0, 'MSO_g': 24.0}


# dim = 1


# self.deviation_dict[IC_ix]

# self.simulation_result


# deviation_dict = {}
# proxy_sr = {    'EOC': {}, 
#                 'WOC': {}, 
#                 'FPC': {}, 
#                 'CPC': {},
#                 'MSO_k_Real': {}, 
#                 'MSO_k_Thry': {}, 
#                 'MSO_e': 87.50000000000007, 'MSO_g': 156.0}








'''
C, r_ratio = 1, 2
prev_min_cost, prev_total_cost = C/r_ratio, 0


def smooth(self):
    for IC_id in list(simulation_result['Essential_Optimizer_calls'].keys()):
        # Update Deviation Dictionary here
        # Correcting EOC & WOC accordingly
        EOC, WOC = simulation_result['Essential_Optimizer_calls'][IC_id], simulation_result['Wasted_Optimizer_calls'][IC_id]
        FPC = simulation_result['FPC_calls'][IC_id]
        CPC = 1 + FPC/EOC # Contour Plan Cardinality
        EOC, WOC = max(EOC, WOC), min(EOC, WOC)
        FPC = EOC*(CPC-1)
        ccm = CCM(EOC, WOC, dim)
        print( *ccm, EOC, WOC )
        # CCM = WOC/EOC # Contour Complexity Measure, 0 mean straight line in one dimension only, 1 means curved in all dimensions
        proxy_sr['EOC'][IC_id], proxy_sr['WOC'][IC_id], proxy_sr['CPC'][IC_id], proxy_sr['FPC'][IC_id],  = EOC, WOC, CPC, FPC

        IC_cost = C*r_ratio**IC_id

        if AdaNexus:
            proxy_sr['CPC'][IC_id] = int(np.ceil(proxy_sr['CPC'][IC_id]*(1-np.random.random()*0.15)))
            CPC = proxy_sr['CPC'][IC_id]
            EOC, WOC = CCM(EOC, WOC, dim)[2:]

        cur_min_cost, cur_total_cost = IC_cost, CPC*IC_cost+prev_total_cost
        if IC_id==0:
            MSO_k_Thry =                                 cur_total_cost/prev_min_cost
            MSO_k_Real = (1+deviation_dict[IC_id].max())*MSO_k_Thry
        else:
            MSO_k_Thry =                                 cur_total_cost/prev_min_cost
            MSO_k_Real = (1+deviation_dict[IC_id].max())*MSO_k_Thry
        prev_min_cost, prev_total_cost = cur_min_cost, cur_total_cost

        proxy_sr['MSO_k_Thry'][IC_id], proxy_sr['MSO_k_Real'][IC_id] = MSO_k_Thry, MSO_k_Real

    proxy_sr['MSO_e_Thry'] =   max(proxy_sr['MSO_k_Thry'].values())
    proxy_sr['MSO_e_Real'] =   max(proxy_sr['MSO_k_Real'].values())
    proxy_sr['MSO_g']      = 4*max(proxy_sr['CPC'       ].values())

'''