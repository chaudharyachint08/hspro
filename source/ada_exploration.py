def boundary_constraint(cur_sel, next_sel, dim_tuple, step_size, progression, next_cost_val, contour_cost):
    dim_l, dim_h = dim_tuple
    if (contour_cost/(1+nexus_tolerance) <= next_cost_val) and (next_cost_val <= contour_cost*(1+nexus_tolerance)):
        step_back = False
    else:
        step_back = True

    slope_1 = (next_sel[dim_h]-cur_sel[dim_h]) / (next_sel[dim_l]-cur_sel[dim_l])
    elif progression=='AP':
        if   right_boundary:
            pass
        elif left_boundary:
            pass
        elif top_boundary:
            pass
        elif bottom_boundary:
            pass
    if progression=='GP':
        pass

def ada_exploration(org_seed, total_dim, progression=progression):
    "Nested function for exploration using seed and contour generation"
    nonlocal IC_id, contour_cost, scale, iad2p_m, iapd2s_m, nexus_lock, wasted_optimizer_calls
    # print('Entered EXPLORATION',IC_id,len(inspect.stack(0)),threading.current_thread())
    min_sel, max_sel = min(self.sel_range_p_inc), max(self.sel_range_p_inc)
    if progression=='AP':
        d_sel =               (max_sel-min_sel) / (len(self.sel_range_p_inc)-1)
    elif progression=='GP':
        r_sel = np.exp( np.log(max_sel/min_sel) / (len(self.sel_range_p_inc)-1) )

    org_seed = np.array(org_seed)
    prev_cost_val, plan_xml = self.get_cost_and_plan(org_seed, plan_id=None, scale=scale)
    prev_plan_id = self.store_plan( plan_xml )

    if total_dim >= 1 :
        dim_h = total_dim-1
        cur_sel, exploration_thread_ls = np.copy(org_seed), [] # index to selectivity, as build_sel is not needed
        for dim_l in range(total_dim-1):
            # (dim_l, dim_h) is dimension pair to be explored
            p2s_m, seed_sel_ls= {}, [] # plan_index to selectivity, as build_sel is not needed
            # 2D exploration using initial seed
            seed_sel_ls.append( tuple(cur_sel) ) # index to selectivity, as build_sel is not needed
            step_size, dir_vec = 1, np.array([0.0, -1.0]) # [X, Y] is used for direction vector

            while True:
                # Finding point with 'd' vector
                next_sel = np.copy(cur_sel)
                norm_dir_vec = dir_vec/np.linalg.norm(dir_vec,1)
                # Ahead movement based on d (direction vector)
                if progression=='AP':
                    diff_sel  = d_sel *  (step_size*norm_dir_vec)
                    # Check here  if next_sel is not getting out of the grid
                    if ((next_sel[[dim_l, dim_h]] + diff_sel)>=min_sel).all() and ((next_sel[[dim_l, dim_h]] + diff_sel)<=max_sel).all():
                        next_sel[[dim_l, dim_h]] += diff_sel
                    else:
                        pass
                elif progression=='GP':
                    ratio_sel = r_sel ** (step_size*norm_dir_vec)
                    # Check here  if next_sel is not getting out of the grid
                    if ((next_sel[[dim_l, dim_h]] * ratio_sel)>=min_sel).all() and ((next_sel[[dim_l, dim_h]] * ratio_sel)<=max_sel).all():
                        next_sel[[dim_l, dim_h]] *= ratio_sel
                    else:
                        pass
                next_cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                next_plan_id = self.store_plan( plan_xml )

                # Correction Vector, Check here  if corr_vec is not getting out of the grid
                orth_vec = np.array([1.0,1.0])
                orth_vec[1] = -1*dir_vec[0]*orth_vec[0]/dir_vec[1]
                loop_count, corr_condition = 2, False
                while loop_count: # This loop will break for either of two orthogonal to dir_vec
                    orth_sel = np.copy(next_sel)
                    norm_orth_vec = orth_vec/np.linalg.norm(orth_vec,1)
                    # Ahead movement based on c (direction vector)
                    if progression=='AP':
                        diff_sel  = d_sel *  (1*norm_orth_vec)
                        # Check here  if next_sel is not getting out of the grid
                        if ((orth_sel[[dim_l, dim_h]] + diff_sel)>=min_sel).all() and ((orth_sel[[dim_l, dim_h]] + diff_sel)<=max_sel).all():
                            orth_sel[[dim_l, dim_h]] += diff_sel
                        else: # Halt as even Unit length orthogonal vector cannot be found
                            loop_count-=1
                            orth_vec *= -1.0
                            continue
                    elif progression=='GP':
                        ratio_sel = r_sel ** (1*norm_orth_vec)
                        # Check here  if next_sel is not getting out of the grid
                        if ((orth_sel[[dim_l, dim_h]] * ratio_sel)>=min_sel).all() and ((orth_sel[[dim_l, dim_h]] * ratio_sel)<=max_sel).all():
                            orth_sel[[dim_l, dim_h]] *= ratio_sel
                        else:
                            loop_count-=1
                            orth_vec *= -1.0
                            continue
                    loop_count-=1
                    # Assumption that Plan will remain same at this small interval of orthogonal step
                    orth_cost_val, _ = self.get_cost_and_plan(orth_sel, plan_id=next_plan_id, scale=scale)
                    if (next_cost_val<contour_cost) == (next_cost_val<orth_cost_val):
                        corr_condition = True
                        break # orth_vec is in correct direction
                    else:
                        orth_vec *= -1.0
                if not corr_condition:
                    break # Break outer while loop for searching next point
                # Two point form for finding desired selectivity point
                slope = (orth_cost_val-next_cost_val)/(orth_sel-next_sel)
                desired_sel = next_sel + (contour_cost-next_cost_val)/slope
                if progression=='AP':
                    corr_vec =        desired_sel - next_sel
                elif progression=='GP':
                    corr_vec = np.log(desired_sel / next_sel)
                # Finding 'g' vector, better direction finding
                grad_vec = step_size*norm_dir_vec + corr_vec

                # Finding point with 'g' vector
                next_sel = np.copy(cur_sel)
                norm_grad_vec = grad_vec/np.linalg.norm(grad_vec,1)
                # Ahead movement based on g (gradient vector)
                if progression=='AP':
                    diff_sel = d_sel *  (step_size*norm_grad_vec)
                    # Check here  if next_sel is not getting out of the grid
                    if ((next_sel[[dim_l, dim_h]] + diff_sel)>=min_sel).all() and ((next_sel[[dim_l, dim_h]] + diff_sel)<=max_sel).all():
                        next_sel[[dim_l, dim_h]] += diff_sel
                    else:
                        pass
                elif progression=='GP':
                    ratio_sel = r_sel ** (step_size*norm_grad_vec)
                    # Check here  if next_sel is not getting out of the grid
                    if ((next_sel[[dim_l, dim_h]] * ratio_sel)>=min_sel).all() and ((next_sel[[dim_l, dim_h]] * ratio_sel)<=max_sel).all():
                        next_sel[[dim_l, dim_h]] *= ratio_sel
                    else:
                        pass
                next_cost_val, plan_xml = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
                next_plan_id = self.store_plan( plan_xml )
                if (contour_cost/(1+nexus_tolerance) <= next_cost_val) and (next_cost_val <= contour_cost*(1+nexus_tolerance)):
                    grad_impact = (1-ada_momentum**step_size)
                    dir_vec = grad_impact*norm_grad_vec + (1-grad_impact)*norm_dir_vec
                    # BisectionAPD code here with Simulating Recursion
                    sim_stck = [ ((cur_sel, prev_plan_id, prev_cost_val),(next_sel, next_plan_id, next_cost_val)), ]
                    while sim_stck:
                        (sel_l, plan_id_l, cost_val_l), (sel_r, plan_id_r, cost_val_r) = sim_stck.pop()
                        if progression=='AP':
                            proxim_srch_flg = True if ( np.linalg.norm(      (sel_l-sel_r),1) > d_sel         ) else False
                            sel_m = (sel_l+sel_r)/2 # Arithmetic Mean
                        elif progression=='GP':
                            proxim_srch_flg = True if ( np.linalg.norm(np.log(sel_l/sel_r),1) > np.log(r_sel) ) else False
                            sel_m = (sel_l*sel_r)**0.5 # Geometric mean
                        if proxim_srch_flg and (plan_id_l != plan_id_r): # Base condition for Bisection search
                            cost_val_m, plan_xml = self.get_cost_and_plan(sel_m, plan_id=None, scale=scale)
                            plan_id_m = self.store_plan( plan_xml )
                            if plan_id_m!=plan_id_l: # Left Side recursive search
                                cost_proxy, _ = self.get_cost_and_plan(sel_m, plan_id=plan_id_l, scale=scale)
                                if cost_proxy > cost_val_m*(1+bisection_lambda):
                                    sim_stck.append( ((sel_l, plan_id_l, cost_val_l),(sel_m, plan_id_m, cost_val_m)) )
                            if plan_id_m!=plan_id_r: # Right Side recursive search
                                cost_proxy, _ = self.get_cost_and_plan(sel_m, plan_id=plan_id_r, scale=scale)
                                if cost_proxy > cost_val_m*(1+bisection_lambda):
                                    sim_stck.append( ((sel_m, plan_id_m, cost_val_m),(sel_r, plan_id_r, cost_val_r)) )
                            # Filling entries into contour cost deviation (Contour wise, unlike Query wise which Sriram did)
                            self.obj_lock.acquire() ; self.deviation_dict[IC_id].append(cost_val_m/self.id2c_m[(IC_id,scale)]) ; self.obj_lock.release()
                            if plan_id_m in p2s_m:
                                p2s_m[plan_id_m].add(tuple(sel_m))
                            else:
                                p2s_m[plan_id_m] = {tuple(sel_m)}
                            seed_sel_ls.append( tuple(sel_m) )
                    step_size *= 2 # Increasing Step size by 2
                    cur_sel, prev_plan_id, prev_cost_val = next_sel, next_plan_id, next_cost_val
                    # Filling entries into contour cost deviation (Contour wise, unlike Query wise which Sriram did)
                    self.obj_lock.acquire() ; self.deviation_dict[IC_id].append(next_cost_val/self.id2c_m[(IC_id,scale)]) ; self.obj_lock.release()
                    if next_plan_id in p2s_m:
                        p2s_m[next_plan_id].add(tuple(next_sel))
                    else:
                        p2s_m[next_plan_id] = {tuple(next_sel)}
                    seed_sel_ls.append( tuple(next_sel) )
                else:
                    nexus_lock.acquire()
                    wasted_optimizer_calls += 2 # dir_vec and grad_vec lead to two extra optimizer calls
                    nexus_lock.release()
                    if step_size>1:
                        step_size /= 2 # Decreasing Step size by 2
                    else:
                        # Exponential rotation algorithm
                        pass
                        # 3 point initialization of search interval (1,3,4 Quarter from cur_sel amenable to search)
                        top_vec, left_vec, diag_vec = np.array([0.0,1.0]), np.array([-1.0,0.0]), np.array([1.0,-1.0])
                        top_sel, left_sel, diag_sel = np.copy(cur_sel),    np.copy(cur_sel),     np.copy(cur_sel)
                        norm_top_vec, norm_left_vec, norm_diag_vec = top_vec/np.linalg.norm(top_vec,1), left_vec/np.linalg.norm(left_vec,1), diag_vec/np.linalg.norm(diag_vec,1)

                        # (Finding 3 selectivity points and constraining each of them to be within selectivity space )
                        # In one of two interval via 3 point intialization will increase cost value

                        if progression=='AP':
                            top_diff_sel, left_diff_sel, diag_diff_sel = d_sel *  (1*norm_top_vec), d_sel *  (1*norm_left_vec), d_sel *  (1*norm_diag_vec)
                            if  (( top_sel[[dim_l, dim_h]] +  top_diff_sel)>=min_sel).all() and (( top_sel[[dim_l, dim_h]] +  top_diff_sel)<=max_sel).all() \
                            and ((diag_sel[[dim_l, dim_h]] + diag_diff_sel)>=min_sel).all() and ((diag_sel[[dim_l, dim_h]] + diag_diff_sel)<=max_sel).all():
                                if ((left_sel[[dim_l, dim_h]] + left_diff_sel)>=min_sel).all() and ((left_sel[[dim_l, dim_h]] + left_diff_sel)<=max_sel).all():
                                    left_sel[[dim_l, dim_h]] = left_sel[[dim_l, dim_h]]+left_diff_sel
                                else:  # To prevent crossing left boundary of ESS
                                    if ((left_sel[[dim_l, dim_h]]+d_sel*np.array([0.0, -1.0]))>=min_sel).all() and ((left_sel[[dim_l, dim_h]]+d_sel*np.array([0.0, -1.0]))<=max_sel).all():
                                        left_sel[[dim_l, dim_h]] = left_sel[[dim_l, dim_h]]+d_sel*np.array([0.0, -1.0])
                                    else:
                                        break
                                top_sel[[dim_l, dim_h]], diag_sel[[dim_l, dim_h]] = top_sel[[dim_l, dim_h]]+top_diff_sel, diag_sel[[dim_l, dim_h]]+diag_diff_sel
                            else:
                                break # Simple termination, if rotation failed, we are
                        elif progression=='GP':
                            top_ratio_sel, left_ratio_sel, diag_ratio_sel = r_sel ** (1*norm_top_vec), r_sel ** (1*norm_left_vec), r_sel ** (1*norm_diag_vec)
                            if  (( top_sel[[dim_l, dim_h]] *  top_ratio_sel)>=min_sel).all() and (( top_sel[[dim_l, dim_h]] *  top_ratio_sel)<=max_sel).all() \
                            and ((diag_sel[[dim_l, dim_h]] * diag_ratio_sel)>=min_sel).all() and ((diag_sel[[dim_l, dim_h]] * diag_ratio_sel)<=max_sel).all():
                                if ((left_sel[[dim_l, dim_h]] * left_ratio_sel)>=min_sel).all() and ((left_sel[[dim_l, dim_h]] * left_ratio_sel)<=max_sel).all():
                                    left_sel[[dim_l, dim_h]] = left_sel[[dim_l, dim_h]]*left_ratio_sel
                                else:  # To prevent crossing left boundary of ESS
                                    if ((left_sel[[dim_l, dim_h]]+r_sel**np.array([0.0, -1.0]))>=min_sel).all() and ((left_sel[[dim_l, dim_h]]+r_sel**np.array([0.0, -1.0]))<=max_sel).all():
                                        left_sel[[dim_l, dim_h]] = left_sel[[dim_l, dim_h]]+r_sel**np.array([0.0, -1.0])
                                    else:
                                        break
                                top_sel[[dim_l, dim_h]], diag_sel[[dim_l, dim_h]] = top_sel[[dim_l, dim_h]]*top_diff_sel, diag_sel[[dim_l, dim_h]]*diag_diff_sel
                            else:
                                break # Simple termination, if rotation failed, we are

                        (top_cost_val, top_plan_xml), (left_cost_val, left_plan_xml), (diag_cost_val, diag_plan_xml) = self.get_cost_and_plan(top_sel, plan_id=None, scale=scale), self.get_cost_and_plan(left_sel, plan_id=None, scale=scale), self.get_cost_and_plan(diag_sel, plan_id=None, scale=scale)

                        # CHECKPOINT, Deciding which interval to continur search into for next direction, dir_vec will be updated here
                        # 2 point interpolation search (Instead of Exponential rotation, led to faster convergence using FPC
                        # Use optimizer calls only when plans on both end are not same, else use FPC module
                        next_plan_id = self.store_plan( plan_xml )

                        while True:
                            norm_start_vec, norm_end_vec = start_vec/np.linalg.norm(start_vec,1), end_vec/np.linalg.norm(end_vec,1)
                            if np.linalg.norm((norm_start_vec-norm_end_vec),1) <= 0.035: # Approximatle sin(2 degree)
                                break
                            mid_vec = (start_vec+end_vec)/2
                        dir_vec = (start_vec+end_vec)/2


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
