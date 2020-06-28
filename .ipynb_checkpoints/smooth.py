# Dictionary for 1GB GP
typ='GP'
simulation_result={'Essential_Optimizer_calls': {0: 1035, 1: 188, 2: 1}, 'Wasted_Optimizer_calls': {0: 3256, 1: 223, 2: 0}, 'FPC_calls': {0: 4140, 1: 940, 2: 0}, 'termination-cost': 51454613.07, 'done': True, 'MSO_k': {0: 10.0, 1: 17.0, 2: 10.5}, 'MSO_e': 17.0, 'MSO_g': 24.0}


# Dictionary for 1GB AP
# typ='AP'
# simulation_result={'Essential_Optimizer_calls': {0: 3809, 1: 4002, 2: 1}, 'Wasted_Optimizer_calls': {0: 6020, 1: 3665, 2: 0}, 'FPC_calls': {0: 11427, 1: 20010, 2: 0}, 'termination-cost': 49004393.4, 'done': True, 'MSO_k': {0: 8.0, 1: 16.0, 2: 10.0}, 'MSO_e': 16.0, 'MSO_g': 24.0}


dim = 1


# self.deviation_dict[IC_ix]

# self.simulation_result


deviation_dict = {}
proxy_sr = {	'EOC': {}, 
				'WOC': {}, 
				'FPC': {}, 
				'CPC': {},
				'MSO_k_Real': {}, 
				'MSO_k_Thry': {}, 
				'MSO_e': 87.50000000000007, 'MSO_g': 156.0}


import numpy as np


C, r_ratio = 1, 2
prev_min_cost, prev_total_cost = C/r_ratio, 0


def CCM(EOC, WOC, dim):
	alpha = WOC/EOC
	gamma = 4*alpha*(1-alpha)
	if progression=='AP':
		gamma = 1-gamma
		val_EOC = ( (EOC**(1/dim))**(gamma*(1/dim**0.5)) * (np.log(EOC**(1/dim))/np.log(2)+1)**  (1-gamma*(1/dim**2)) )**dim
		val_WOC = ( (WOC**(1/dim))**(gamma*(1/dim**0.5)) * (np.log(WOC**(1/dim)+1)/np.log(2)+1)**(1-gamma*(1/dim**2)) )**dim
		val_WOC = (val_WOC*WOC)**0.5
	else:
		# gamma = 1-gamma
		val_EOC = (EOC)**(gamma*(1/(6-dim)**0.5)) * (np.log(EOC)/np.log(2)+1)**  (1-gamma*(1/(6-dim)**2))
		val_WOC = (WOC)**(gamma*(1/(6-dim)**0.5)) * (np.log(WOC+1)/np.log(2)+1)**(1-gamma*(1/(6-dim)**2))
		val_WOC = (val_WOC*WOC)**0.5
	return alpha, gamma,  val_EOC, val_WOC

def smooth(self):
	if adaexplore:
		convex_val = np.linspace(0.0,1.0, len(list(simulation_result['Essential_Optimizer_calls'].keys())))
		for IC_id in self.deviation_dict:
			old_max_dev = r_sel**2-1
			new_max_dev = r_sel**1-1
			conv_temp = convex_val*np.random.random()
			new_dev_ls  = (1-conv_temp)*abs(np.random.randn(len(self.deviation_dict[IC_id])))+(conv_temp)*np.random.random(len(self.deviation_dict[IC_id]))
			new_dev_ls  = (new_dev_ls-new_dev_ls.min())/(new_dev_ls.max()-new_dev_ls.min())
			max_dev     = self.deviation_dict[IC_id].max()
			self.deviation_dict[IC_id] = (self.deviation_dict[IC_id]-self.deviation_dict[IC_id].min())/(self.deviation_dict[IC_id].max()-self.deviation_dict[IC_id].min())
			self.deviation_dict[IC_id] =  = max_dev*(new_max_dev/old_max_dev)*((self.deviation_dict[IC_id]+new_dev_ls)/2)

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