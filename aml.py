#!/usr/bin/env python
# coding: utf-8
#Created on Thu Nov 9 10:38:29 2021
#@author: Lu Jian

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.rdd import portable_hash
from scipy.sparse import csr_matrix
import numpy as np

conf = SparkConf()
conf.set("spark.hadoop.mapred.output.compress", "false")
spark = SparkSession.builder.config(conf = conf).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
df = spark.read.parquet("hdfs://localhost:9000/data")
def build_index(df,max_depth,need3=True):
    uniq_edge = df.selectExpr('lower(trim(accname)) a','lower(trim(Cntpty_Acct_Name)) b ').filter('a<>b ').groupby(['a','b']).max().persist()
    aconts = uniq_edge.selectExpr('a as n').groupby(['n']).max().union(uniq_edge.selectExpr('b as n').groupby(['n']).max()).groupby('n').max().toPandas().values
    name2id = {j[0]:i for i,j in enumerate(aconts)}
    values = uniq_edge.rdd.map(lambda x:(name2id[x[0]],name2id[x[1]])).toDF(['a','b']).toPandas().values
    uniq_edge.unpersist()
    name2id_len = len(name2id)
    s = csr_matrix(([1]*len(values), (values[:,0], values[:,1])), shape = (name2id_len, name2id_len))
    u = s.sum(axis = 1)
    v = s.sum(axis = 0)
    ins =[set(np.where(v>0)[1])]
    outs=[set(np.where(u>0)[0])]
    depth=3 if need3 else max_depth
    for i in range(1,depth-1):
        u = s*u
        v = v*s
        ins.append(set(np.where(v>0)[1]))
        outs.append(set(np.where(u>0)[0]))   
    srcs = outs[-1]
    dsts = ins[-1]
    if not srcs:
        return {},set(),{}
    nodes_set=set()
    for i in range(depth-2):
        nodes_set.update(outs[i]&ins[depth-3-i])
    def f(iterator):
        for i in iterator:
            a=i['accname'].strip().lower()
            b=i['Cntpty_Acct_Name'].strip().lower()
            if a!=b:
                a = name2id[a]
                b = name2id[b]
                if (a in nodes_set or a in srcs) and (b in nodes_set or b in dsts):
                    yield a,i[1],b,i[3],i[4],i[5]
    data_values = df.select(['accname', 'Tx_Amt', 'Cntpty_Acct_Name', 'id', 'daystamp','lag']).rdd.mapPartitions(f).toDF().toPandas().values
    D = {}
    for a,m,b,k,t,l in data_values:
        if a not in D:
            D[a] = {b:[[k,t,l,m]]}
        elif b not in D[a]:
            D[a][b] = [[k,t,l,m]]
        else:
            D[a][b].append([k,t,l,m])
    for i in D:
        for j in D[i]:
            D[i][j] = np.array(sorted(D[i][j],key = lambda x:x[1]),dtype = float)
    return D,srcs,{v:k for k,v in name2id.items()}
T = 5;
SIGMA = 0.05;
max_depth = 4;
P = 16;
LIMIT = 20;
need3=False
D,srcs,id2name=build_index(df,max_depth,need3)
def prepares(srcs):
    for a in srcs:
        for b in D[a]:
            Db = D.get(b,None)
            if Db is not None:# b can be joined
                for e_ab in D[a][b]:
                    for c in Db:
                        if c!= a:
                            e_bc = Db[c]
                            cond_w = (e_ab[1]<= e_bc[:,1]) & (e_ab[1]+T>= e_bc[:,1])
                            if np.any(cond_w):
                                cond_n = cond_w & (e_ab[2]>e_bc[:,1])
                                e_bc =  e_bc[cond_n,:] if np.any(cond_n) else e_bc[cond_w,:][0:1,:]
                                for e_bc_i in e_bc:
                                    yield ((a,c,e_ab[1],e_bc_i[1]),[[a,b,c],[e_ab,e_bc_i]])
def deep_search(iterator):
    for item in iterator:
        q=[item[1]]
        while q:
            n,e = q.pop(0)
            if len(n)<max_depth:
                Dn = D.get(n[-1],None)
                if Dn is not None:
                    n_set = set(n)
                    for n1 in Dn:
                        if n1 not in n_set:
                            e_A = Dn[n1]
                            cond_w = (e[-1][1]<= e_A[:,1]) & (e[0][1]+T>= e_A[:,1])
                            if np.any(cond_w):
                                cond_n = cond_w & (e[-1][2]>e_A[:,1])
                                e_A =  e_A[cond_n,:] if np.any(cond_n) else e_A[cond_w,:][0:1,:]
                                q.extend([[n+[n1],e+[e_Ai]] for e_Ai in e_A])
            else:
                yield [(n[0],n[-1],e[0][1],e[-1][1]),(n,e)]
def mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed):
    for idx,st in enumerate(cur_st):
        pre_get = pre_tx[pre_ed == st,:]
        pre_get = pre_get[cur_tx[idx,1]>= pre_get[:,1]]
        if pre_get[:,-1].sum()*(1+SIGMA)<cur_tx[idx,-1]:
            cur_tx[idx,-1] = 0
    cond = cur_tx[:,-1]>0
    return cur_tx[cond,:],cur_st[cond],cur_ed[cond]
def income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set):
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
def binary_search(st_amts,ed_amts,batch,node,st_tx,st_ed,ed_tx,ed_st):
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
            def recurdive_search(pre_ed_set,pre_tx,pre_ed,MID = [],mid = 1):
                if mid > depth-3:
                    MIDS.append([MID,(pre_ed_set,pre_tx,pre_ed)])
                    return                     
                cur_tx,cur_id = np.unique(batch[:,mid,:],axis = 0,return_index = True)
                cur_st,cur_ed = node[cur_id,mid],node[cur_id,mid+1]
                cur_tx,cur_st,cur_ed = mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed)
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
                    PASS,cur_ed_set = income_expenditure_check_out(pre_tx,pre_ed,cur_tx_tmp,cur_st_tmp,cur_ed_tmp,pre_ed_set)
                    if PASS:
                        recurdive_search(cur_ed_set,cur_tx_tmp,cur_ed_tmp,MID+list(mid_ids),mid+1)
                    else:
                        return
            recurdive_search(pre_ed_set,pre_tx,pre_ed)
            if MIDS:
                for MID,(pre_ed_set,pre_tx,pre_ed) in MIDS:
                    PASS,_ = income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set)
                    if PASS:
                        t=tuple(st_ids)+tuple(MID)+tuple(ed_ids)
                        yield (int(node[0][0]),int(node[0][-1]),-len(t)),(float(AMOUNT),depth,t)              
def fast_search(st_amts,ed_amts,batch,node,pre_tx,pre_ed,lst_tx,lst_st):
    AMOUNT = sum(ed_amts)
    if abs(AMOUNT/sum(st_amts,1e-5)-1)<= SIGMA:
        st_ids = pre_tx[:,0]
        ed_ids = lst_tx[:,0]
        pre_ed_set = set(pre_ed)
        depth = len(node[0])
        MID = []
        for mid in range(1,depth-2):
            cur_tx,cur_id = np.unique(batch[:,mid,:],axis = 0,return_index = True)
            cur_tx = cur_tx.reshape(-1,4)
            cur_st,cur_ed = node[cur_id,mid],node[cur_id,mid+1]
            cur_tx,cur_st,cur_ed = mask(pre_tx,pre_ed,cur_tx,cur_st,cur_ed)
            amts = cur_tx[:,-1]
            if abs(amts.sum()/AMOUNT-1)<= SIGMA:
                mid_ids = cur_tx[:,0]
                PASS,cur_ed_set = income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set,False)
                if PASS:
                    pre_ed_set = cur_ed_set
                    pre_tx,pre_ed = cur_tx,cur_ed
                    MID.extend(list(mid_ids))
                else:
                    return None
            else:
                return None
        PASS,_ = income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set)
        if not PASS:
            return None
        t=tuple(st_ids)+tuple(MID)+tuple(ed_ids)
        yield (int(node[0][0]),int(node[0][-1]),-len(t)),(float(AMOUNT),depth,t)
    return None
def main(iterator):
    def search(batches,nodes):
        batch = np.array(batches)
        node = np.array(nodes,int)
        st_tx,st_id = np.unique(batch[:, 0,:],axis = 0,return_index = True)
        st_ed = node[st_id,1]
        ed_tx,ed_id = np.unique(batch[:,-1,:],axis = 0,return_index = True)
        ed_st = node[ed_id,-2]
        st_amts = st_tx[:,-1]
        ed_amts = ed_tx[:,-1]
        amts_len = len(st_amts)+len(ed_amts)
        if amts_len<= LIMIT :
            for r in binary_search(st_amts,ed_amts,batch,node,st_tx,st_ed,ed_tx,ed_st):
                yield r
        else:
            r =  fast_search(st_amts,ed_amts,batch,node,st_tx,st_ed,ed_tx,ed_st)
            if r is not None:
                yield r
            else:
                for j in range(len(batch)):
                    mini_batch = batch[j:j+LIMIT,:,:]
                    mini_node = node[j:j+LIMIT,:]
                    st_tx,st_id = np.unique(mini_batch[:, 0,:],axis = 0,return_index = True)
                    st_ed = mini_node[st_id,1]
                    ed_tx,ed_id = np.unique(mini_batch[:,-1,:],axis = 0,return_index = True)
                    ed_st = mini_node[ed_id,-2]
                    st_amts = st_tx[:,-1]
                    ed_amts = ed_tx[:,-1]
                    for r in binary_search(st_amts,ed_amts,mini_batch,mini_node,st_tx,st_ed,ed_tx,ed_st):
                        yield r
    try:
        batches=[]
        (st_nd,ed_nd,st_dt,_),(nds,egs)= next(iterator)
        batches.append(egs)
        nodes = [nds]
        while True:
            (st_nd_,ed_nd_,st_dt_,ed_dt_),(nds,egs)= next(iterator)
            if (st_nd_,ed_nd_)==(st_nd,ed_nd) and ed_dt_<st_dt+T:
                batches.append(egs)
                nodes.append(nds)
            else:
                for r in search(batches,nodes):
                    yield r       
                if (st_nd_,ed_nd_)!=(st_nd,ed_nd):
                    st_nd,ed_nd,st_dt=st_nd_,ed_nd_,st_dt_
                    batches = [egs_]
                    nodes = [nds_]
                else:
                    while batches and batches[0][1]+T<ed_dt_ :
                        batches.pop(0)
                        nodes.pop(0)
                    batches.append(egs)
                    nodes.append(nds)
                    st_nd,ed_nd,st_dt=nodes[0][0],nodes[0][-1],batches[0][1]
    except:
        if batches:
            for r in search(batches,nodes):
                yield r
def combine(iterator):
    base={}
    for item in iterator:
        k,s=item[0][:2],set(item[1][-1])
        if k not in base:
            base={item[0][:2]:[set(item[1][-1])]}
            yield item
        else:
            not_sub=True
            for S in base[k]:
                if len(s)>2*len(s-S):
                    not_sub=False
                    break
            if not_sub:
                base[k].append(s)
                yield item
srcs_rdd = sc.parallelize(srcs,min(P,max(len(srcs),1)))
srcs_rdd2 = srcs_rdd.mapPartitions(prepares).persist()
srcs_rdd4 = srcs_rdd2.mapPartitions(deep_search)\
.repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])))\
.mapPartitions(main).distinct()\
.repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])))\
.mapPartitions(combine).persist()
if need3 :
    base4 = [set(i) for i in srcs_rdd4.map(lambda x:x[1][-1]).collect()]
    def combine3(iterator):
        for item in iterator:
            s=set(item[1][-1])
            not_sub=True
            for S in base4:
                if len(s)>2*len(s-S):
                    not_sub=False
                    break
            if not_sub:
                yield item
    srcs_rdd3 = srcs_rdd2.repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])))\
    .mapPartitions(main).distinct()\
    .repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])))\
    .mapPartitions(combine).mapPartitions(combine3)
    srcs_rdd2.unpersist()
    result=srcs_rdd4.union(srcs_rdd3).zipWithIndex()
    srcs_rdd4.unpersist()
else:
    result=srcs_rdd4.zipWithIndex()
def flatID(iterator):
    for (k,(*v,s)),idx in iterator:
        for payid in s:
            yield (idx,id2name[k[0]],id2name[k[1]],v[0],v[1],int(payid))
RESULT = result.mapPartitions(flatID).toDF('''batch_id: int, src: string, dst: string, amount: float, depth: int, id: int''')
RESULT.join(df,'id','left').repartition(1).write.parquet("hdfs://localhost:9000/RESULT",mode = 'overwrite')
