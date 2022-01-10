#!/usr/bin/env python
# coding: utf-8
#Created on Thu Nov 9 10:38:29 2021
#@author: Lu Jian Email:janelu@live.cn; lujian@sdc.icbc.com.cn 

from pyspark.sql import SparkSession,Window,functions as F
from pyspark.conf import SparkConf
from pyspark.rdd import portable_hash
conf = SparkConf()
conf.set("spark.hadoop.mapred.output.compress", "false")
spark = SparkSession.builder.config(conf = conf).enableHiveSupport().getOrCreate()
sc = spark.sparkContext

from pygraph import jian_iteration,binary_search
import numpy as np
from numba import jit

df = spark.read.parquet("hdfs://localhost:9000/data")
def build_index(df,max_depth,need3=True):
    uniq_edge = df.selectExpr('lower(trim(accname)) a','lower(trim(Cntpty_Acct_Name)) b ').filter('a<>b ').groupby(['a','b']).max().persist()
    aconts = uniq_edge.selectExpr('a as n').groupby(['n']).max().union(uniq_edge.selectExpr('b as n').groupby(['n']).max()).groupby('n').max().toPandas().values
    name2id = {j[0]:i for i,j in enumerate(aconts)}
    values = uniq_edge.rdd.map(lambda x:(name2id[x[0]],name2id[x[1]])).toDF(['a','b']).toPandas().values
    uniq_edge.unpersist()
    depth=3 if need3 else max_depth
    srcs,nodes_set,dsts=jian_iteration(values,depth)
    if not srcs:
        return {},set(),{}
    def f(iterator):
        for i in iterator:
            a=i['accname'].strip().lower()
            b=i['Cntpty_Acct_Name'].strip().lower()
            if a!=b:
                a = name2id[a]
                b = name2id[b]
                if (a in nodes_set or a in srcs) and (b in nodes_set or b in dsts):
                    yield a,i[1],b,i[3],i[4],i[5]
    data_values = df.withColumn('time_stamp',F.unix_timestamp('Event_Dt','yyyy-MM-dd')+F.col('id')/1e28)\
    .withColumn('lag',F.coalesce(F.lag('time_stamp',-1).over(Window.partitionBy('accname','Cntpty_Acct_Name').orderBy('time_stamp')),F.lit(np.inf)))\
    .select(['accname', 'Tx_Amt', 'Cntpty_Acct_Name', 'id', 'time_stamp','lag'])\
    .rdd.mapPartitions(f).toDF()\
    .toPandas().values
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
T = 5*86400;
SIGMA = 0.05;
max_depth = 4;
P = 16;
LIMIT = 20;
need3=True
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
def fast_search(st_amts,ed_amts,batch,node,pre_tx,pre_ed,lst_tx,lst_st,SIGMA):
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
        PASS,_ = income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set,SIGMA)
        if not PASS:
            return None
        t=tuple(st_ids)+tuple(MID)+tuple(ed_ids)
        yield (int(node[0][0]),int(node[0][-1]),-len(t)),(float(AMOUNT),depth,t)
    return None
def main(iterator):
    @jit
    def search(batches,nodes,SIGMA,LIMIT):
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
            for r in binary_search(st_amts,ed_amts,batch,node,st_tx,st_ed,ed_tx,ed_st,SIGMA):
                yield r
        else:
            r =  fast_search(st_amts,ed_amts,batch,node,st_tx,st_ed,ed_tx,ed_st,SIGMA)
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
                    for r in binary_search(st_amts,ed_amts,mini_batch,mini_node,st_tx,st_ed,ed_tx,ed_st,SIGMA):
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
                for r in search(batches,nodes,SIGMA,LIMIT):
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
            for r in search(batches,nodes,SIGMA,LIMIT):
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
spark.read.parquet("hdfs://localhost:9000/RESULT").show()