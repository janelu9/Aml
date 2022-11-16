#!/usr/bin/env python
# coding: utf-8
# Created on Thu Nov 9 10:38:29 2021
# @author: Lu Jian
# Email:janelu@live.cn;

from pyspark.sql import SparkSession,Window,functions as F
from pyspark.conf import SparkConf
from pyspark.rdd import portable_hash
from pyspark import StorageLevel
import numpy as np

conf = SparkConf()
conf.set("spark.hadoop.mapred.output.compress", "false")
spark = SparkSession.builder.config(conf = conf).enableHiveSupport().getOrCreate()

from build_functions import fast_search,accurate_search
 
data = spark.read.parquet("hdfs://localhost:9000/data5")
T = 5;SIGMA = 0.05;DEPTH = 2;P = 16;LIMIT = 10;RECYCLE = False
pay_id,acc_name,event_dt,tx_amt,cntpty_acc_name='id','accname','Event_Dt','Tx_Amt','Cntpty_Acct_Name'
df=data.selectExpr(pay_id,f'lower(trim({acc_name})) {acc_name}',tx_amt,f'lower(trim({cntpty_acc_name})) {cntpty_acc_name}',f"unix_timestamp({event_dt},'yyyy-MM-dd')+float(substring({pay_id},-6))/1e6 time_stamp").filter(f'{acc_name}<>{cntpty_acc_name} and {tx_amt}>0').withColumn('lag',F.coalesce(F.lag('time_stamp',-1).over(Window.partitionBy(acc_name,cntpty_acc_name).orderBy('time_stamp')),F.lit(float('inf')))).persist(StorageLevel(True, True, False, False, 1))
uniq_edge = df.selectExpr(f'{acc_name} a',f'{cntpty_acc_name} b ').groupby(['a','b']).max()
DEPTH = max(DEPTH,2)
T*=86400

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
    
bridges = lu_iteration(uniq_edge,DEPTH)
srcs_rdd = df.join(bridges[1].withColumnRenamed('n',cntpty_acc_name),cntpty_acc_name,'leftsemi').join(bridges[0].withColumnRenamed('n',acc_name),acc_name,'leftsemi').rdd.map(lambda x:(x[1],([x[0],x[1]],[[x[2],x[4],x[5],x[3]]])))

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


for i in range(1,len(bridges)-2):
    D=df.join(bridges[i+1].withColumnRenamed('n',cntpty_acc_name),cntpty_acc_name,'leftsemi').join(bridges[i].withColumnRenamed('n',acc_name),acc_name,'leftsemi').rdd.map(lambda x:((x[0],x[1],x[4]),(x[2],x[3],x[5]))).repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(groupByNode)
    srcs_rdd=srcs_rdd.union(D).groupByKey(P).flatMapValues(lambda x:joinAndFilterByDate(x,T)).map(lambda x:(x[1][0][-1],x[1]))
D=df.join(bridges[-1].withColumnRenamed('n',cntpty_acc_name),cntpty_acc_name,'leftsemi').join(bridges[-2].withColumnRenamed('n',acc_name),acc_name,'leftsemi').rdd.map(lambda x:((x[0],x[1],x[4]),(x[2],x[3],x[5]))).repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(groupByNode)
srcs_rdd=srcs_rdd.union(D).groupByKey(P).flatMapValues(lambda x:joinAndFilterByDate(x,T,RECYCLE)).map(lambda x:((x[1][0][0],x[1][0][-1],x[1][1][0][1],x[1][1][-1][1]),x[1]))

def graph_detect(batch,node,SIGMA,LIMIT):
    r = fast_search(batch,node,SIGMA)
    if r is not None:
        yield r
    else:
        i ,count_set, length= 0, set(), len(batch)
        for j in range(length):
            count_set.update({batch[j][0][0],batch[j][-1][0]})
            if len(count_set)>LIMIT:
                for r in accurate_search(batch[i:j],node[i:j],SIGMA):
                    yield r
                while len(count_set)>LIMIT:
                    if batch[i][0][0] in count_set:count_set.remove(batch[i][0][0])
                    if batch[i][-1][0] in count_set:count_set.remove(batch[i][-1][0])
                    i+=1
        for r in accurate_search(batch[i:],node[i:],SIGMA):
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
        k, s = item[0][:2],set(item[1][-1])
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
                
chains = srcs_rdd.repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(main).distinct().repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(drop_duplicates).zipWithIndex()

def flatID(iterator):
    for (k,(*v,s)),idx in iterator:
        for payid in s:
            yield (idx,str(k[0]),str(k[1]),v[0],v[1],int(payid))
            
result = chains.mapPartitions(flatID).toDF(f'''chain_id: int, src: string, dst: string, amount: float, depth: int, {pay_id}: int''')
result.join(data,pay_id,'left').repartition(1).write.parquet("hdfs://localhost:9000/result",mode = 'overwrite')
spark.read.parquet("hdfs://localhost:9000/result").show()
