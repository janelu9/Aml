#!/usr/bin/env python
# coding: utf-8
#Created on Thu Nov 9 10:38:29 2021
#@author: Lu Jian Email:janelu@live.cn

from scipy.sparse import csr_matrix
import numpy as np

def jian_iteration(values,depth):
    n=len(set(values[:,0]))+len(set(values[:,1]))
    s = csr_matrix(([1]*len(values), (values[:,0], values[:,1])), shape = (n, n))
    u = s.sum(axis = 1)
    v = s.sum(axis = 0)
    ins =[set(np.where(v>0)[1])]
    outs=[set(np.where(u>0)[0])]
    for i in range(1,depth-1):
        u = s*u
        v = v*s
        ins.append(set(np.where(v>0)[1]))
        outs.append(set(np.where(u>0)[0]))   
    srcs = outs[-1]
    dsts = ins[-1]
    if not srcs:
        return set(),set(),set()
    nodes_set=set()
    for i in range(depth-2):
        nodes_set.update(outs[i]&ins[depth-3-i])
    return srcs,nodes_set,dsts

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

def binary_search(batch,node,st_tx,st_ed,ed_tx,ed_st,SIGMA):
    st_amts,ed_amts = st_tx[:,-1],ed_tx[:,-1]
    amts = np.hstack([st_amts,ed_amts])
    pivot = len(st_amts)
    upbound = min(sum(st_amts),sum(ed_amts))*(1+SIGMA)   
    select = []
    sum_list = []
    def sovler1(index_list = [], n = 0):
        st = sum([amts[i] for i in index_list if  i< pivot])   
        ed = sum([amts[i] for i in index_list if  i>= pivot],1e-5)
        if max(st,ed)>upbound:
            return
        des = st/ed    
        if abs(des-1)<= SIGMA:
            select.append(index_list)
            sum_list.append(st)
            return
        if n ==  len(amts) :
            return
        sovler1(index_list+[n], n+1)
        sovler1(index_list, n+1)
    sovler1()
    if select:
        for sel_idx,AMOUNT in zip(select,sum_list):
            select_index = np.array(sel_idx)
            st_index = select_index[select_index<pivot]
            ed_index = select_index[select_index>= pivot]-pivot
            pre_tx = st_tx[st_index,:]
            pre_ed = st_ed[st_index]
            lst_tx = ed_tx[ed_index,:]
            lst_st = ed_st[ed_index]
            st_ids = pre_tx[:,0]            
            ed_ids = lst_tx[:,0]
            pre_ed_set = set(pre_ed)
            depth = len(node[0])
            MIDS = []
            def recursive_search(pre_ed_set,pre_tx,pre_ed,MID = [],mid = 1):
                if mid > depth-3:
                    MIDS.append([MID,(pre_ed_set,pre_tx,pre_ed)])
                    return                     
                cur_tx,cur_id = np.unique(batch[:,mid,:],axis = 0,return_index = True)
                cur_st,cur_ed = node[cur_id,mid],node[cur_id,mid+1]
                cur_tx,cur_st,cur_ed = mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,SIGMA)
                amts = cur_tx[:,-1]
                if amts.sum()<(1-SIGMA)*AMOUNT:
                    return 
                mid_indexs = []
                def sovler2(index_list = [],n = 0,asum = 0):
                    des = asum/AMOUNT-1
                    if des>SIGMA:
                        return
                    if abs(des)<= SIGMA:
                        mid_indexs.append(index_list)
                        return
                    if n ==  len(amts) :
                        return
                    sovler2(index_list+[n],n+1,asum+amts[n])
                    sovler2(index_list,n+1,asum)
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
                        yield (int(node[0][0]),int(node[0][-1]),-len(t)),(float(AMOUNT),depth,t)

