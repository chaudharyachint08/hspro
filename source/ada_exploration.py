def boundary_constraint(cur_sel, next_sel, dim_tuple):
    if (min_sel<=next_sel[list(dim_tuple)]).all() and (next_sel[list(dim_tuple)]<=max_sel).all():
        return False, next_sel
    dim_l, dim_h = dim_tuple
    cur_sel, next_sel = np.copy(cur_sel), np.copy(next_sel)
    # Using Point slope form, (y-y1) = slope*(x-x1)
    slope = (next_sel[dim_h]-cur_sel[dim_h]) / (next_sel[dim_l]-cur_sel[dim_l])
    # Finding X or Y value accordingly at all boundary which specifies constraints
    y_val_at_max_x = (cur_sel[dim_h]+(max_sel-cur_sel[dim_l])*slope)
    y_val_at_min_x = (cur_sel[dim_h]+(min_sel-cur_sel[dim_l])*slope)
    x_val_at_max_y = (cur_sel[dim_l]+(max_sel-cur_sel[dim_h])/slope)
    x_val_at_min_y = (cur_sel[dim_l]+(min_sel-cur_sel[dim_h])/slope)
    # Finding if it goes beyond ESS, by which side of ESS, and marking intesection points at boundary
    if   min_sel<=y_val_at_max_x and y_val_at_max_x<=max_sel: # x = max_sel, limit constaint on y (right  boundary)
        x,y = max_sel, y_val_at_max_x
    elif min_sel<=y_val_at_min_x and y_val_at_min_x<=max_sel: # x = min_sel, limit constaint on y (left   boundary)
        x,y = min_sel, y_val_at_min_x
    elif min_sel<=x_val_at_max_y and x_val_at_max_y<=max_sel: # y = max_sel, limit constaint on x (top    boundary)
        y,x = max_sel, x_val_at_max_y
    elif min_sel<=x_val_at_min_y and x_val_at_min_y<=max_sel: # y = min_sel, limit constaint on x (bottom boundary)
        y,x = min_sel, x_val_at_min_y
    next_sel[list(dim_tuple)] = [x,y]
    return True, next_sel
    # Below code in unreachable for future use
    next_cost_val, _ = self.get_cost_and_plan(next_sel, plan_id=None, scale=scale)
    if (contour_cost/(1+nexus_tolerance) <= next_cost_val) and (next_cost_val <= contour_cost*(1+nexus_tolerance)):
        return True, next_sel
    else: # Intersection point 
        return True, next_sel # Job of this function is to return constrained vector in ESS

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
                    next_sel[[dim_l, dim_h]]+=diff_sel
                    end_point, next_sel = boundary_constraint(cur_sel, next_sel,  (dim_l, dim_h))
                elif progression=='GP':
                    ratio_sel = r_sel ** (step_size*norm_dir_vec)
                    next_sel[[dim_l, dim_h]]*=ratio_sel
                    end_point, next_sel = boundary_constraint(cur_sel, next_sel, (dim_l, dim_h))
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
                        orth_sel[[dim_l, dim_h]]+=diff_sel
                        end_point, orth_sel = boundary_constraint(next_sel, orth_sel,  (dim_l, dim_h))
                        if np.linalg.norm((orth_sel-next_sel),1)<epsilon: # Halt if orth_sel is same as next_sel
                            loop_count-=1
                            orth_vec *= -1.0
                            continue
                    elif progression=='GP':
                        ratio_sel = r_sel ** (1*norm_orth_vec)
                        orth_sel[[dim_l, dim_h]]*=ratio_sel
                        end_point, orth_sel = boundary_constraint(next_sel, orth_sel, (dim_l, dim_h))
                        if np.linalg.norm((orth_sel-next_sel),1)<epsilon: # Halt if orth_sel is same as next_sel
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
                if (not corr_condition) and (not ((contour_cost/(1+nexus_tolerance) <= next_cost_val) and (next_cost_val <= contour_cost*(1+nexus_tolerance)))):
                    if step_size>1:
                        step_size /= 2
                        continue
                    else:
                        break # Break outer while loop for searching next point
                else:
                    if corr_condition:
                        # Two point form for finding desired selectivity point
                        slope = (orth_cost_val-next_cost_val)/(orth_sel-next_sel)
                        desired_sel = next_sel + (contour_cost-next_cost_val)/slope
                        if progression=='AP':
                            corr_vec =        desired_sel - next_sel
                        elif progression=='GP':
                            corr_vec = np.log(desired_sel / next_sel)
                        # Finding 'g' vector, better direction finding
                        grad_vec = step_size*norm_dir_vec + corr_vec
                    else:
                        grad_vec = step_size*norm_dir_vec + 0.0 # No correction is required

                # Finding point with 'g' vector
                next_sel = np.copy(cur_sel)
                norm_grad_vec = grad_vec/np.linalg.norm(grad_vec,1)
                # Ahead movement based on g (gradient vector)
                if progression=='AP':
                    diff_sel = d_sel *  (step_size*norm_grad_vec)
                    next_sel[[dim_l, dim_h]]+=diff_sel
                    end_point, next_sel = boundary_constraint(cur_sel, next_sel,  (dim_l, dim_h))
                elif progression=='GP':
                    ratio_sel = r_sel ** (step_size*norm_grad_vec)
                    next_sel[[dim_l, dim_h]]*=ratio_sel
                    end_point, next_sel = boundary_constraint(cur_sel, next_sel, (dim_l, dim_h))
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
                        # Exponential rotation algorithm for finding direction in case of failed tuning with minimum step size
                        # 4 point initialization of search interval (1,3,4 Quarter from cur_sel amenable to search)
                        top_vec, left_vec, bottom_vec, right_vec = np.array([0.0,1.0]), np.array([-1.0,0.0]), np.array([0.0,-1.0]), np.array([1.0,0.0])
                        top_sel, left_sel, bottom_sel, right_sel = np.copy(cur_sel),    np.copy(cur_sel),     np.copy(cur_sel),     np.copy(cur_sel)
                        # (Finding 4 selectivity points and constraining each of them to be within selectivity space )
                        # In one of three interval via 4 point intialization will obtain desired cost value
                        if   progression=='AP':
                            top_diff_sel, left_diff_sel, bottom_diff_sel, right_diff_sel  =  d_sel *  (1*top_vec), d_sel *  (1*left_vec), d_sel *  (1*bottom_vec), d_sel *  (1*right_vec)
                            top_sel[[dim_l, dim_h]], left_sel[[dim_l, dim_h]], bottom_sel[[dim_l, dim_h]], right_sel[[dim_l, dim_h]]  =  top_sel[[dim_l, dim_h]]+top_diff_sel, left_sel[[dim_l, dim_h]]+left_diff_sel, bottom_sel[[dim_l, dim_h]]+bottom_diff_sel, right_sel[[dim_l, dim_h]]+right_diff_sel
                        elif progression=='AP':
                            top_ratio_sel, left_ratio_sel, bottom_ratio_sel, right_ratio_sel  =  r_sel ** (1*top_vec), r_sel ** (1*left_vec), r_sel ** (1*bottom_vec), r_sel ** (1*right_vec)
                            top_sel[[dim_l, dim_h]], left_sel[[dim_l, dim_h]], bottom_sel[[dim_l, dim_h]], right_sel[[dim_l, dim_h]]  =  top_sel[[dim_l, dim_h]]*top_ratio_sel, left_sel[[dim_l, dim_h]]*left_ratio_sel, bottom_sel[[dim_l, dim_h]]*bottom_ratio_sel, right_sel[[dim_l, dim_h]]*right_ratio_sel

                        (top_end, top_sel), (left_end, left_sel), (bottom_end, bottom_sel), (right_end, right_sel)  =  boundary_constraint(cur_sel, top_sel,  (dim_l, dim_h)), boundary_constraint(cur_sel, left_sel,  (dim_l, dim_h)), boundary_constraint(cur_sel, bottom_sel,  (dim_l, dim_h)), boundary_constraint(cur_sel, right_sel,  (dim_l, dim_h))
                        
                        init_intervals = [  ((left_end, left_sel), (bottom_end, bottom_sel)), ((bottom_end, bottom_sel), (right_end, right_sel)), ((right_end, right_sel), (top_end, top_sel)) ]
                        viable_intervals = []
                        for (start_end, start_sel), (end_end, end_sel) in init_intervals:
                            if not (start_end or end_end):
                                viable_intervals.append( (start_sel, end_sel) )
                        if not viable_intervals:
                            break # Break search as rotation based correction is not possible now, due to lack on any side to rotate with unit length
                        # Costing Viable intervals and check if contour_cost lies in any interval
                        viable_sel_cost_plan = []
                        for viable_ix, (start_sel, end_sel) in enumerate(viable_intervals):
                            (start_cost_val, start_plan_xml), (end_cost_val, end_plan_xml) = self.get_cost_and_plan(start_sel, plan_id=None, scale=scale), self.get_cost_and_plan(end_sel, plan_id=None, scale=scale)
                            start_plan_id, end_plan_id = self.store_plan( start_plan_xml ), self.store_plan( end_plan_xml )
                            if   (start_cost_val<=contour_cost and contour_cost<=end_cost_val):
                                break
                            elif (end_cost_val<=contour_cost and contour_cost<=start_cost_val):
                                (start_sel, start_cost_val, start_plan_id), (end_sel, end_cost_val, end_plan_id) = (end_sel, end_cost_val, end_plan_id), (start_sel, start_cost_val, start_plan_id)
                                break
                        else: # if contour_cost does not lie in either of interval, then take maximum or minimum cost step depending of prev_cost_val
                            if   prev_cost_val < contour_cost:
                                if right_end and top_end:
                                    break
                                right_cost_val  = self.get_cost_and_plan( right_sel, plan_id=None, scale=scale)[0] if (not right_end) else -np.inf
                                top_cost_val    = self.get_cost_and_plan( top_sel,   plan_id=None, scale=scale)[0] if (not top_end)   else -np.inf
                                dir_vec = right_vec if (right_cost_val > top_cost_val) else top_vec
                            elif prev_cost_val >  contour_cost:
                                if left_end and bottom_end:
                                    break
                                left_cost_val   = self.get_cost_and_plan( left_sel,   plan_id=None, scale=scale)[0] if (not left_end)   else np.inf
                                bottom_cost_val = self.get_cost_and_plan( bottom_sel, plan_id=None, scale=scale)[0] if (not bottom_end) else np.inf
                                dir_vec = left_vec if (left_cost_val < bottom_cost_val) else bottom_vec
                            continue
                        nexus_lock.acquire()
                        wasted_optimizer_calls += (2+viable_ix) # Only Viable intervals will lead to optimizer call
                        nexus_lock.release()
                        # If an interval is selected within for loop, make Binary search when plans are different, interpolation search when same for finding dir_vec
                        # 2 point interpolation search (Instead of Exponential rotation, led to faster convergence using FPC
                        # Use optimizer calls only when plans on both end are not same, else use FPC module
                        while True:
                            if   progression=='AP':
                                if np.linalg.norm(      (norm_start_vec-norm_end_vec),1) <=        0.035*d_sel: # Approximatle sin(2 degree precision)
                                    break
                                else:
                                    mid_sel = (start_sel+end_sel)/2
                            elif progression=='GP':
                                if np.linalg.norm(np.log(norm_start_vec/norm_end_vec),1) <= np.log(0.035*r_sel): # Approximatle sin(2 degree precision)
                                    break
                                else:
                                    mid_sel = (start_sel*end_sel)**0.5
                            # Binary search will use FPC, when plans on both end is same
                            mid_cost_val, mid_plan_xml = self.get_cost_and_plan(mid_sel, plan_id=(None if (start_plan_id!=end_plan_id) else start_plan_id), scale=scale)
                            if start_plan_id!=end_plan_id:
                                mid_plan_id = self.store_plan( mid_plan_xml )
                            if start_plan_id!=end_plan_id:
                                nexus_lock.acquire()
                                wasted_optimizer_calls += 1 # Optimizer call only when plans on both end are different
                                nexus_lock.release()
                            if mid_cost_val < contour_cost:
                                start_sel, start_cost_val, start_plan_id = mid_sel, mid_cost_val, mid_plan_id
                            else:
                                end_sel,   end_cost_val,   end_plan_id   = mid_sel, mid_cost_val, mid_plan_id
                        # Once desired value is obtained, use selectivity to find dir_vec
                        if   progression=='AP':
                            dir_vec =         mid_sel[[dim_l, dim_h]]-cur_sel[[dim_l, dim_h]]
                        elif progression=='AP':
                            dir_vec = np.log( mid_sel[[dim_l, dim_h]]/cur_sel[[dim_l, dim_h]] )
                        dir_vec = dir_vec / np.linalg(dir_vec,1)


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
