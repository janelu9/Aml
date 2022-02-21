#!/usr/bin/env python
# coding: utf-8
#Created on Thu Nov 9 10:38:29 2021
#@author: Lu Jian Email:janelu@live.cn

import numpy as np

def lu_iteration(uniq_edge,depth):
    l=uniq_edge.selectExpr('a as n').groupby(['n']).max()
    u=uniq_edge.selectExpr('b as n').groupby(['n']).max()
    outs = [l]
    ins  = [u]
    for k in range(1,depth):
        l=uniq_edge.withColumnRenamed('b','n').join(outs[-1],'n','inner').selectExpr('a as n').groupby(['n']).max()
        u=uniq_edge.withColumnRenamed('a','n').join( ins[-1],'n','inner').selectExpr('b as n').groupby(['n']).max()    
        outs.append(l)
        ins.append(u)
    bridges = [outs[-1]]
    for i in range(depth-1):
        bridges.append(ins[i].join(outs[depth-2-i],'n','inner'))
    bridges.append(ins[-1])   
    return bridges
    
def groupByNode(x):
    try:
        buffer = []
        (a0,b0,t),(k,m,l) = next(x)
        buffer.append([k,t,l,m])
        while True:
            (a,b,t),(k,m,l) = next(x)
            if a == a0 and b == b0:
                buffer.append([k,t,l,m])
            else:
                yield (a0,(b0,np.array(buffer,float)))
                a0 = a
                b0 = b
                buffer=[[k,t,l,m]]
    except:
        if buffer:
            yield (a0,(b0,np.array(buffer,float)))
def joinAndFilterByDate(x,T,R = False):
    vbuf, wbuf = [], []
    for v in x:
        if isinstance(v[0],list):
            vbuf.append(v)
        else:
            wbuf.append(v)
    for n,e in vbuf:
        if R:
            for c,e_A in wbuf:
                if c == n[0]:
                    cond_w = (e[-1][1] < e_A[:,1]) & (e[0][1]+T > e_A[:,1])
                    if np.any(cond_w):
                        cond_n = cond_w & (e[-1][2] > e_A[:,1])
                        e_A =  e_A[cond_n,:] if np.any(cond_n) else e_A[cond_w,:][0:1,:]
                        for e_Ai in e_A:
                            yield (n+[c],e+[e_Ai])            
        else:
            for c,e_A in wbuf:
                n_set=set(n)
                if c not in n_set:
                    cond_w = (e[-1][1] < e_A[:,1]) & (e[0][1]+T > e_A[:,1])
                    if np.any(cond_w):
                        cond_n = cond_w & (e[-1][2] > e_A[:,1])
                        e_A =  e_A[cond_n,:] if np.any(cond_n) else e_A[cond_w,:][0:1,:]
                        for e_Ai in e_A:
                            yield (n+[c],e+[e_Ai])
                            
def mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,SIGMA):
    for idx,st in enumerate(cur_st):
        pre_get = pre_tx[pre_ed == st,:]
        pre_get = pre_get[cur_tx[idx,1]>= pre_get[:,1]]
        if pre_get[:,-1].sum()*(1+SIGMA)<cur_tx[idx,-1]:
            cur_tx[idx,-1] = 0
    cond = cur_tx[:,-1]>0
    return cur_tx[cond,:],cur_st[cond],cur_ed[cond]    
def income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set,SIGMA):
    cur_st_set = set(cur_st)
    if cur_st_set == pre_ed_set:
        for idx,st in enumerate(cur_st):
            pre_get = pre_tx[pre_ed == st,:]
            all_get = pre_get[cur_tx[idx,1]>= pre_get[:,1]][:,-1].sum()
            cur_out = cur_tx[cur_st == st,:]
            all_out = cur_out[cur_tx[idx,1]>= cur_out[:,1]][:,-1].sum()
            if all_get<= all_out*(1-SIGMA):
                return False,None
        return True,set(cur_ed)
    return False,None  
def fast_search(batch,node,SIGMA):
    batch = np.array(batch)
    node = np.array(node,str)
    pre_tx,st_id = np.unique(batch[:, 0,:],axis = 0,return_index = True)
    pre_ed = node[st_id,1]
    AMOUNT = pre_tx[:,-1].sum()
    st_ids = pre_tx[:,0]
    pre_ed_set = set(pre_ed)
    depth = len(batch[0])
    MID = []
    for mid in range(1,depth):
        cur_tx,cur_id = np.unique(batch[:,mid,:],axis = 0,return_index = True)
        cur_st = node[cur_id,mid]
        cur_ed = node[cur_id,mid+1]
        cur_tx,cur_st,cur_ed = mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,SIGMA)
        amts = cur_tx[:,-1]
        if abs(amts.sum()/AMOUNT-1)<= SIGMA:
            mid_ids = cur_tx[:,0]
            PASS,cur_ed_set = income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set,SIGMA)
            if PASS:
                pre_ed_set = cur_ed_set
                pre_tx,pre_ed = cur_tx,cur_ed
                MID.extend(list(mid_ids))
            else:
                return None
        else:
            return None
    if PASS:
        t=tuple(st_ids)+tuple(MID)
        return (node[0][0],node[0][-1],-len(t)),(float(AMOUNT),depth,t)
    return None    
def accurate_search(batch,node,SIGMA):
    batch = np.array(batch)
    node = np.array(node,str)
    st_tx,st_id = np.unique(batch[:, 0,:],axis = 0,return_index = True)
    st_ed = node[st_id,1]
    ed_tx,ed_id = np.unique(batch[:,-1,:],axis = 0,return_index = True)
    ed_st = node[ed_id,-2]
    a = st_tx[:,-1]
    b = ed_tx[:,-1]
    m = len(a)
    n = m+len(b)
    up = min(sum(a),sum(b))*(1+SIGMA)
    solvers=[]
    def tree(a_i=[], a_sum=0, b_i=[], b_sum=0.01, i=0):
        if a_sum>up or b_sum>up:
            return
        if abs(a_sum/b_sum-1) <= SIGMA:
            solvers.append((a_sum,a_i,b_i))
            return
        if i==n:
            return
        if i<m:
            tree(a_i+[i], a_sum+a[i] ,b_i ,b_sum ,i+1)
        else:
            tree(a_i, a_sum ,b_i+[i-m] ,b_sum+b[i-m] ,i+1)
        tree(a_i, a_sum ,b_i, b_sum, i+1)
    tree()
    if solvers:
        for AMOUNT,st_index,ed_index in solvers:
            pre_tx = st_tx[st_index,:]
            pre_ed = st_ed[st_index]
            lst_tx = ed_tx[ed_index,:]
            lst_st = ed_st[ed_index]
            st_ids = pre_tx[:,0]            
            ed_ids = lst_tx[:,0]
            pre_ed_set = set(pre_ed)
            depth = len(batch[0])
            MIDS = []
            def recursive_search(pre_ed_set,pre_tx,pre_ed,MID = [],mid = 1):
                if mid > depth-2:
                    MIDS.append([MID,(pre_ed_set,pre_tx,pre_ed)])
                    return                     
                cur_tx,cur_id = np.unique(batch[:,mid,:],axis = 0,return_index = True)
                cur_st = node[cur_id,mid]
                cur_ed = node[cur_id,mid+1]
                cur_tx,cur_st,cur_ed = mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,SIGMA)
                amts = cur_tx[:,-1]
                if amts.sum()<(1-SIGMA)*AMOUNT:
                    return 
                mid_indexs = []
                amts_len = len(amts)
                def sovler2(index_list = [],n = 0,des = -1):
                    if des>SIGMA:
                        return
                    if abs(des)<= SIGMA:
                        mid_indexs.append(index_list)
                        return
                    if n ==  amts_len :
                        return
                    sovler2(index_list+[n],n+1,des+amts[n]/AMOUNT)
                    sovler2(index_list,n+1,des)
                sovler2()
                if not mid_indexs:
                    return
                for mid_index in mid_indexs:
                    mid_ids = cur_tx[mid_index,0]
                    cur_tx_tmp = cur_tx[mid_index,:]
                    cur_st_tmp = cur_st[mid_index]
                    cur_ed_tmp = cur_ed[mid_index]
                    PASS,cur_ed_set = income_expenditure_check_out(pre_tx,pre_ed,cur_tx_tmp,cur_st_tmp,cur_ed_tmp,pre_ed_set,SIGMA)
                    if PASS:
                        recursive_search(cur_ed_set,cur_tx_tmp,cur_ed_tmp,MID+list(mid_ids),mid+1)
                    else:
                        return
            recursive_search(pre_ed_set,pre_tx,pre_ed)
            if MIDS:
                for MID,(pre_ed_set,pre_tx,pre_ed) in MIDS:
                    PASS,_ = income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set,SIGMA)
                    if PASS:
                        t=tuple(st_ids)+tuple(MID)+tuple(ed_ids)
                        yield (node[0][0],node[0][-1],-len(t)),(float(AMOUNT),depth,t)