#!/usr/bin/env python
# coding: utf-8
#@author: Lu Jian Email:janelu@live.cn; lujian@sdc.icbc.com.cn Created on Thu Nov 9 10:38:29 2021

from pyspark.sql import SparkSession,Window,functions as F
from pyspark.conf import SparkConf
from pyspark.rdd import portable_hash
from pyspark import StorageLevel

conf = SparkConf()
conf.set("spark.hadoop.mapred.output.compress", "false")
spark = SparkSession.builder.config(conf = conf).enableHiveSupport().getOrCreate()

from some_ideas import lu_iteration,groupByNode,filterByDate,fast_search,accurate_search
 
T = 5*86400;SIGMA = 0.05;MAX_DEPTH = 3;P = 16;LIMIT = 20; 
data = spark.read.parquet("hdfs://localhost:9000/data")
pay_id,acc_name,event_dt,tx_amt,cntpty_acc_name='id','accname','Event_Dt','Tx_Amt','Cntpty_Acct_Name'
df=data.selectExpr(pay_id,f'lower(trim({acc_name})) {acc_name}',tx_amt,f'lower(trim({cntpty_acc_name})) {cntpty_acc_name}',f"unix_timestamp({event_dt},'yyyy-MM-dd')+float(substring({pay_id},-6))/1e6 time_stamp").filter(f'{acc_name}<>{cntpty_acc_name} and {tx_amt}>0').withColumn('lag',F.coalesce(F.lag('time_stamp',-1).over(Window.partitionBy(acc_name,cntpty_acc_name).orderBy('time_stamp')),F.lit(float('inf'))))
uniq_edge = df.selectExpr(f'{acc_name} a',f'{cntpty_acc_name} b ').groupby(['a','b']).max()
srcs,bridges,dsts = lu_iteration(uniq_edge,MAX_DEPTH)
s, e = srcs, dsts
for n in bridges:
    s=s.union(n)
    e=e.union(n)
edges=df.join(e.withColumnRenamed('n',cntpty_acc_name),cntpty_acc_name,'leftsemi').join(s.withColumnRenamed('n',acc_name),acc_name,'leftsemi')          
D=edges.rdd.map(lambda x:((x[0],x[1],x[4]),(x[2],x[3],x[5]))).repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(groupByNode).persist(StorageLevel(True, True, False, False, 1))
D.count()
srcs_rdd = edges.join(srcs.withColumnRenamed('n',acc_name),acc_name,'leftsemi').rdd.map(lambda x:(x[1],([x[0]],[[x[2],x[4],x[5],x[3]]])))
for step in range(0,MAX_DEPTH-1):
    srcs_rdd = srcs_rdd.join(D,P).mapPartitions(lambda x:filterByDate(x,T))                               
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
chains = srcs_rdd.map(lambda x:((x[1][0][0],x[0],x[1][1][0][1],x[1][1][-1][1]),(x[1][0]+[x[0]],x[1][1]))).repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(main).distinct().repartitionAndSortWithinPartitions(P,lambda x:portable_hash((x[0],x[1]))).mapPartitions(drop_duplicates).zipWithIndex()
def flatID(iterator):
    for (k,(*v,s)),idx in iterator:
        for payid in s:
            yield (idx,str(k[0]),str(k[1]),v[0],v[1],int(payid))
result = chains.mapPartitions(flatID).toDF(f'''chain_id: int, src: string, dst: string, amount: float, depth: int, {pay_id}: int''')
result.join(data,pay_id,'left').repartition(1).write.parquet("hdfs://localhost:9000/result",mode = 'overwrite')
spark.read.parquet("hdfs://localhost:9000/result").show()