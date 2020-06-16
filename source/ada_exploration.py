def ada_exploration(org_seed, total_dim, progression=progression):
    "Nested function for exploration using seed and contour generation"
    nonlocal IC_id, contour_cost, scale, iad2p_m, iapd2s_m, nexus_lock, wasted_optimizer_calls
    # print('Entered EXPLORATION',IC_id,len(inspect.stack(0)),threading.current_thread())
    min_sel, max_sel = min(self.sel_range_p_inc), max(self.sel_range_p_inc)
    if progression=='AP':
        d_sel = (max_sel-min_sel) / (len(self.sel_range_p_inc)-1)
    elif progression=='GP':
        r_sel = np.exp( np.log(max_sel/min_sel) / (len(self.sel_range_p_inc)-1) )

    if total_dim >= 1 :
        dim_h = total_dim-1
        cur_sel, exploration_thread_ls = list(org_seed_[:]), [] # Checkpoint, index to selectivity, as build_sel is not needed
        for dim_l in range(total_dim-1):
            # (dim_l, dim_h) is dimension pair to be explored
            p2s_m, seed_sel_ls= {}, [] # plan_index to selectivity, as build_sel is not needed
            # 2D exploration using initial seed
            seed_sel_ls.append( tuple(cur_sel) ) # Checkpoint, index to selectivity, as build_sel is not needed


            while True:
                x, y = cur_ix[dim_l], cur_ix[dim_h] # Checkpoint, index to selectivity, as build_sel is not needed
                next_ix  = cur_ix[:]
                next_ix[dim_h] -= 1
                next_sel = self.build_sel(next_ix)
                cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                if cost_val < contour_cost:  # C_opt[(S(y−1)] < C
                    # S = S(x+1)
                    x += 1
                    if (not (0<=x+1 and x+1<=self.resolution_p-1)):
                        break
                    next_ix  = cur_ix[:]
                    next_ix[dim_l] += 1
                    next_sel = self.build_sel(next_ix)
                    cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                    nexus_lock.acquire()
                    wasted_optimizer_calls += 1
                    nexus_lock.release()
                else:
                    # S = S(y−1)
                    y -= 1
                    if (not (0<=y-1 and y-1<=self.resolution_p-1)) :
                        break
                # Filling entries into contour cost deviation (Contour wise, unlike Query wise which Sriram did)
                self.obj_lock.acquire() ; self.deviation_dict[IC_id].append(cost_val/self.id2c_m[(IC_id,scale)]) ; self.obj_lock.release()
                next_plan_id = self.store_plan( plan_xml )
                if next_plan_id in p2s_m:
                    p2s_m[next_plan_id].add(next_sel)
                else:
                    p2s_m[next_plan_id] = {next_sel}
                cur_sel[dim_l], cur_sel[dim_h] = x, y
                seed_sel_ls.append( tuple(cur_sel) )
                # non-existence of either S(x+1) or S(y−1)
                if (not (0<=x+1 and x+1<=self.resolution_p-1)) or (not (0<=y-1 and y-1<=self.resolution_p-1)) :
                    break


            # First search include both ends of 2D exploration, rest will not include first end
            if dim_l+1 != dim_h:
                del seed_sel_ls[0]
            nexus_lock.acquire()
            iad2p_m[(IC_id,0.0,scale)].update(p2s_m.keys())
            for plan_id in p2s_m:
                if (IC_id,0.0,plan_id,scale) in iapd2s_m:
                    iapd2s_m[(IC_id,0.0,plan_id,scale)].update(p2s_m[plan_id])
                else:
                    iapd2s_m[(IC_id,0.0,plan_id,scale)] = p2s_m[plan_id]
            nexus_lock.release()
            # For each seed generated, call (Dim-1) dimensional subproblem
            d2_exploration_thread_ls = [ threading.Thread(target=ada_exploration,args=(seed_sel,total_dim-1,progression)) for seed_sel in seed_sel_ls ]
            # All exploration are collected and waited to end outside dim_l forloop
            exploration_thread_ls.extend( d2_exploration_thread_ls )
            # Launching construction of all Ico-cost contours
            for d2_explore_thread in d2_exploration_thread_ls:
                d2_explore_thread.start()
        # Waiting for construction of all Ico-cost contours
        for explore_thread in exploration_thread_ls:
            explore_thread.join()
    # print('Exiting EXPLORATION',IC_id,len(inspect.stack(0)),threading.current_thread())
