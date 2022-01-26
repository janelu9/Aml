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

from some_ideas import Lu_iteration,accurate_search,fast_search
import numpy as np

df = spark.read.parquet("hdfs://localhost:9000/data")
pay_id,acc_name,event_dt,tx_amt,cntpty_acc_name='id','accname','Event_Dt','Tx_Amt','Cntpty_Acct_Name'
def build_index(df,MAX_DEPTH,need2=True):
    df=df.selectExpr(pay_id,f'lower(trim({acc_name})) {acc_name}',tx_amt,f'lower(trim({cntpty_acc_name})) {cntpty_acc_name}',f"unix_timestamp({event_dt},'yyyy-MM-dd')+float(substring({pay_id},-6))/1e6 time_stamp").filter(f'{acc_name}<>{cntpty_acc_name} and {tx_amt}>0').withColumn('lag',F.coalesce(F.lag('time_stamp',-1).over(Window.partitionBy(acc_name,cntpty_acc_name).orderBy('time_stamp')),F.lit(np.inf))).persist()
    uniq_edge = df.selectExpr(f'{acc_name} a',f'{cntpty_acc_name} b ').groupby(['a','b']).max().persist()
    aconts = uniq_edge.selectExpr('a as n').groupby(['n']).max().union(uniq_edge.selectExpr('b as n').groupby(['n']).max()).groupby('n').max().toPandas().values
    name2id = {j[0]:i for i,j in enumerate(aconts)}
    edges = uniq_edge.rdd.map(lambda x:(name2id[x[0]],name2id[x[1]])).toDF(['a','b']).toPandas().values
    uniq_edge.unpersist()
    depth=2 if need2 else MAX_DEPTH
    srcs,nodes_set,dsts=Lu_iteration(edges,depth)
    if not srcs:return {},set(),{}
    def filtrate(iterator):
        for i in iterator:
            a = name2id[i[acc_name]]
            b = name2id[i[cntpty_acc_name]]
            if (a in nodes_set or a in srcs) and (b in nodes_set or b in dsts):
                yield i[pay_id],a,i[tx_amt],b,i['time_stamp'],i['lag']
    data_values = df.rdd.mapPartitions(filtrate).collect()
    df.unpersist()
    D = {}
    for k,a,m,b,t,l in data_values:
        if a not in D:
            D[a] = {b:[[k,t,l,m]]}
        elif b not in D[a]:
            D[a][b] = [[k,t,l,m]]
        else:
            D[a][b].append([k,t,l,m])
    for i in D:
        for j in D[i]:
            D[i][j] = np.array(sorted(D[i][j],key = lambda x:x[1]),dtype = float)
    return D,srcs,{v:k for k,v in name2id.items() if v in srcs or v in dsts}
T = 5*86400;
SIGMA = 0.05;
MAX_DEPTH = 3;
P = 16;
LIMIT = 20;
need2 = True
D,srcs,id2name = build_index(df,MAX_DEPTH,need2)
def prepares(srcs):
    for a in srcs:
        for b in D[a]:
            Db = D.get(b,None)
            if Db is not None:# b can be joined
                for e_ab in D[a][b]:
                    for c in Db:
                        if c!= a:
                            e_bc = Db[c]
                            cond_w = (e_ab[1] < e_bc[:,1]) & (e_ab[1]+T > e_bc[:,1])
                            if np.any(cond_w):
                                cond_n = cond_w & (e_ab[2] > e_bc[:,1])
                                e_bc =  e_bc[cond_n,:] if np.any(cond_n) else e_bc[cond_w,:][0:1,:]
                                for e_bc_i in e_bc:
                                    yield ((a,c,e_ab[1],e_bc_i[1]),[[a,b,c],[e_ab,e_bc_i]])
def deep_search(iterator):
    for item in iterator:
        q=[item[1]]
        while q:
            n,e = q.pop(0)
            if len(e) < MAX_DEPTH:
                Dn = D.get(n[-1],None)
                if Dn is not None:
                    n_set = set(n)
                    for n1 in Dn:
                        if n1 not in n_set:
                            e_A = Dn[n1]
                            cond_w = (e[-1][1] < e_A[:,1]) & (e[0][1]+T > e_A[:,1])
                            if np.any(cond_w):
                                cond_n = cond_w & (e[-1][2] > e_A[:,1])
                                e_A =  e_A[cond_n,:] if np.any(cond_n) else e_A[cond_w,:][0:1,:]
                                for e_Ai in e_A:
                                    q.append([n+[n1],e+[e_Ai]])
            else:
                yield [(n[0],n[-1],e[0][1],e[-1][1]),(n,e)]
def graph_detect(batch,node,SIGMA,LIMIT):
    batch = np.array(batch)
    node = np.array(node,int)
    r = fast_search(batch,node,SIGMA)
    if r is not None:
        yield r
    else:
        i,count_set,length = 0,set(),len(batch)
        for j in range(length):
            count_set.update({batch[j,0,0],batch[j,-1,0]})
            if len(count_set)>LIMIT:
                for r in accurate_search(batch[i:j,:,:],node[i:j,:],SIGMA):
                    yield r
                while len(count_set)>LIMIT:
                    if batch[i, 0,0] in count_set:count_set.remove(batch[i, 0,0])
                    if batch[i,-1,0] in count_set:count_set.remove(batch[i,-1,0])
                    i+=1
        for r in accurate_search(batch[i:,:,:],node[i:,:],SIGMA):
            yield r
def main(iterator):
    try:
        batch_buffer = []
        (st_nd,ed_nd,st_dt,_),(nds,egs) = next(iterator)
        batch_buffer.append(egs)
        nodes = [nds]
        while True:
            (st_nd_,ed_nd_,st_dt_,ed_dt_),(nds,egs) = next(iterator)
            if (st_nd_,ed_nd_) == (st_nd,ed_nd) and ed_dt_< st_dt+T:
                batch_buffer.append(egs)
                nodes.append(nds)
            else:
                for r in graph_detect(batch_buffer,nodes,SIGMA,LIMIT):
                    yield r       
                if (st_nd_,ed_nd_) != (st_nd,ed_nd):
                    st_nd,ed_nd,st_dt = st_nd_,ed_nd_,st_dt_
                    batch_buffer, nodes = [egs],[nds]
                else:
                    batch_buffer.append(egs)
                    nodes.append(nds)
                    while batch_buffer[0][0][1]+T < ed_dt_ :
                        batch_buffer.pop(0)
                        nodes.pop(0)
                    st_nd,ed_nd,st_dt = nodes[0][0],nodes[0][-1],batch_buffer[0][0][1]
    except:
        if batch_buffer:
            for r in graph_detect(batch_buffer,nodes,SIGMA,LIMIT):
                yield r
def drop_duplicates(iterator):
    base = {}
    for item in iterator:
        k,s = item[0][:2],set(item[1][-1])
        if k not in base:
            base = {item[0][:2]:[set(item[1][-1])]}
            yield item
        else:
            not_sub = True
            for S in base[k]:
                if len(s)>2*len(s-S):
                    not_sub = False
                    break
            if not_sub:
                base[k].append(s)
                yield item
srcs_rdd = sc.parallelize(srcs,min(P,max(len(srcs),1)))
space3 = srcs_rdd.mapPartitions(prepares).persist()
chains_deeper = space3.mapPartitions(deep_search).repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(main).distinct().repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(drop_duplicates).persist()
if need2 :
    deeper_id_set = [set(i) for i in chains_deeper.map(lambda x:x[1][-1]).collect()]
    def downward_drop_duplicates(iterator):
        for item in iterator:
            s = set(item[1][-1])
            not_sub = True
            for S in deeper_id_set:
                if len(s) > 2*len(s-S):
                    not_sub = False
                    break
            if not_sub:
                yield item
    chains3 = space3.repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(main).distinct().repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(drop_duplicates).mapPartitions(downward_drop_duplicates)
    space3.unpersist()
    result = chains_deeper.union(chains3).zipWithIndex()
    chains_deeper.unpersist()
else:
    result = chains_deeper.zipWithIndex()
def flatID(iterator):
    for (k,(*v,s)),idx in iterator:
        for payid in s:
            yield (idx,id2name[k[0]],id2name[k[1]],v[0],v[1],int(payid))
result = result.mapPartitions(flatID).toDF(f'''chain_id: int, src: string, dst: string, amount: float, depth: int, {pay_id}: int''')
result.join(df,pay_id,'left').repartition(1).write.parquet("hdfs://localhost:9000/result",mode = 'overwrite')
spark.read.parquet("hdfs://localhost:9000/result").show()