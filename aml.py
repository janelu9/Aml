#!/usr/bin/env python
# coding: utf-8
#Created on Thu Nov 9 10:38:29 2021
#@author: Lu Jian

from pyspark.sql import HiveContext,SparkSession
from pyspark.conf import SparkConf
from pyspark.rdd import portable_hash
from scipy.sparse import csr_matrix
from queue import deque
from functools import reduce
import numpy as np

conf = SparkConf()
conf.set("spark.hadoop.mapred.output.compress", "false")
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sc=spark.sparkContext
hc=HiveContext(sc)
df=hc.read.parquet("hdfs://localhost:9000/data")
uniq_edge=df.selectExpr('accname a','Cntpty_Acct_Name b ').filter('a<>b ').groupby(['a','b']).max()
uniq_edge.persist()
aconts=uniq_edge.selectExpr('a as n').groupby(['n']).max().union(uniq_edge.selectExpr('b as n').groupby(['n']).max()).groupby('n').max()
x=aconts.toPandas().values
name2id={j[0]:i for i,j in enumerate(x)}
uniq_edge_pd=uniq_edge.rdd.map(lambda x:(name2id[x[0]],name2id[x[1]])).toDF(['a','b']).toPandas()
uniq_edge.unpersist()
id2name={v:k for k,v in name2id.items()}
values=uniq_edge_pd.values
name2id_len=len(name2id)
s = csr_matrix(([1]*len(values), (values[:,0], values[:,1])), shape=(name2id_len, name2id_len))
u=s.sum(axis=1)
v=s.sum(axis=0)
nodes_set=set(np.where(u>0)[0]) & set(np.where(v>0)[1])
u=s*u
srcs=np.where(u>0)[0]
nodes_set.update(srcs)
nodes_name_set={id2name[i] for i in nodes_set}
def f(iterator):
    for i in iterator:
        if i['accname'].strip().lower() in nodes_name_set:
            yield i
data_values=df.select(['accname', 'Tx_Amt', 'Cntpty_Acct_Name', 'id', 'daystamp','lag']).rdd.mapPartitions(f).toDF().toPandas().values
D={}
for a,m,b,k,t,l in data_values:
    a=name2id[a.strip().lower()]
    b=name2id[b.strip().lower()]
    if a!=b:
        if a not in D:
            D[a]={b:[[k,t,l,m]]}
        elif b not in D[a]:
            D[a][b]=[[k,t,l,m]]
        else:
            D[a][b].append([k,t,l,m])
for i in D:
    for j in D[i]:
        D[i][j]=np.array(sorted(D[i][j],key=lambda x:x[1]),dtype=float)
T=5;SIGMA=0.05;max_depth=4;P=16;LIMIT=20
srcs_rdd=sc.parallelize(srcs,min(P,len(srcs)))
def prepares(srcs):
    for a in srcs:
        for b in D[a]:
            Db=D.get(b,None)
            if Db is not None:# b can be joined
                for e_ab in D[a][b]:
                    for c in Db:
                        if c!=a:
                            e_bc=Db[c]
                            cond_w=(e_ab[1]<=e_bc[:,1]) & (e_ab[1]+T>=e_bc[:,1])
                            if np.any(cond_w):
                                cond_n=cond_w & (e_ab[2]>e_bc[:,1])
                                e_bc= e_bc[cond_n,:] if np.any(cond_n) else e_bc[cond_w,:][0:1,:]
                                for e_bc_i in e_bc:
                                    yield ((a,c,e_ab[1],e_bc_i[1]),[[a,b,c],[e_ab,e_bc_i]])
srcs_rdd2=srcs_rdd.mapPartitions(prepares).repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])),keyfunc=lambda x:(x[2],x[3]))
def deep_search(iterator):
    for item in iterator:
        q=deque()
        q.append(item[1])
        while q:
            n,e=q.popleft()
            if len(n)<max_depth:
                Dn = D.get(n[-1],None)
                if Dn is not None:
                    n_set=set(n)
                    for n1 in Dn:
                        if n1 not in n_set:
                            e_A=Dn[n1]
                            cond_w=(e[-1][1]<=e_A[:,1]) & (e[0][1]+T>=e_A[:,1])
                            if np.any(cond_w):
                                cond_n=cond_w & (e[-1][2]>e_A[:,1])
                                e_A= e_A[cond_n,:] if np.any(cond_n) else e_A[cond_w,:][0:1,:]
                                q.extend([[n+[n1],e+[e_Ai]] for e_Ai in e_A])
            else:
                yield [(n[0],n[-1],e[0][1],e[-1][1]),(n,e)]
srcs_rdd3=srcs_rdd2.mapPartitions(deep_search).repartitionAndSortWithinPartitions(P,partitionFunc=lambda x:portable_hash((x[0],x[1])),keyfunc=lambda x:(x[2],x[3]))
def check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed):
    for idx,st in enumerate(cur_st):
        pre_get=pre_tx[pre_ed==st,:]
        pre_get=pre_get[cur_tx[idx,1]>=pre_get[:,1]]
        if pre_get[:,-1].sum()*(1+SIGMA)<cur_tx[idx,-1]:
            cur_tx[idx,-1]=0
    cond=cur_tx[:,-1]>0
    return cur_tx[cond,:],cur_st[cond],cur_ed[cond]
def income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set,check=True):
    cur_st_set=set(cur_st)
    if cur_st_set==pre_ed_set:
        for cur_node in cur_st_set:
            all_out=cur_tx[cur_st==cur_node,-1].sum()
            all_get=pre_tx[pre_ed==cur_node,-1].sum()
            if all_get<=all_out*(1-SIGMA):
                return False,None
        if check:
            _,cur_st_,_=check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed)
            if len(cur_st)!=len(cur_st_):
                return False,None
        return True,set(cur_ed)
    return False,None
def binary_search(st_amts,ed_amts,batch,nodes,st_tx,st_ed,ed_tx,ed_st):
    amts=np.hstack([st_amts,ed_amts])
    pivot=len(st_amts)
    upbound=min(sum(st_amts),sum(ed_amts))*(1+SIGMA)
    select=[]
    sum_list=[]
    def sovler(bool_list=[], n=0):
        st=sum([amts[i] for i in bool_list if  i< pivot])
        ed=sum([amts[i] for i in bool_list if  i>=pivot],1e-5)
        if max(st,ed)>upbound:
            return
        des=st/ed
        if abs(des-1)<=SIGMA:
            select.append(bool_list)
            sum_list.append(st)
            return
        if n == len(amts) :
            return
        sovler(bool_list+[n], n+1)
        sovler(bool_list, n+1)
    sovler()
    if select:
        for sel_idx,AMOUNT in zip(select,sum_list):
            select_index=np.array(sel_idx)
            st_index=select_index[select_index<pivot]
            ed_index=select_index[select_index>=pivot]-pivot
            pre_tx=st_tx[st_index,:]
            pre_ed=st_ed[st_index]
            lst_tx=ed_tx[ed_index,:]
            lst_st=ed_st[ed_index]
            st_ids=pre_tx[:,0]
            ed_ids=lst_tx[:,0]
            pre_ed_set=set(pre_ed)
            depth=len(nodes[0])
            MID=[0]*(depth-3)
            drop=False
            for mid in range(1,depth-2):
                cur_tx,cur_id=np.unique(batch[:,mid,:],axis=0,return_index=True)
                cur_tx=cur_tx.reshape(-1,4)
                cur_st,cur_ed=nodes[cur_id,mid],nodes[cur_id,mid+1]
                cur_tx,cur_st,cur_ed=check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed)
                amts=cur_tx[:,-1]
                if amts.sum()<(1-SIGMA)*AMOUNT:
                    drop=True
                    break
                mid_indexs=[]
                def sovler1(bool_list=[],n=0,asum=0):
                    des=asum/AMOUNT-1
                    if des>SIGMA:
                        return
                    if abs(des)<=SIGMA:
                        mid_indexs.append(bool_list)
                        return
                    if n == len(amts) :
                        return
                    sovler1(bool_list+[n],n+1,asum+amts[n])
                    sovler1(bool_list,n+1,asum)
                sovler1()
                if mid_indexs:
                    for mid_index in mid_indexs:
                        mid_ids=cur_tx[mid_index,0]
                        cur_tx_tmp=cur_tx[mid_index,:]
                        cur_st_tmp=cur_st[mid_index]
                        cur_ed_tmp=cur_ed[mid_index]
                        PASS,cur_ed_set=income_expenditure_check_out(pre_tx,pre_ed,cur_tx_tmp,cur_st_tmp,cur_ed_tmp,pre_ed_set)
                        if PASS:
                            pre_ed_set=cur_ed_set
                            pre_tx,pre_ed=cur_tx_tmp,cur_ed_tmp
                            MID[mid-1]=tuple(mid_ids)
                            break
                    if not PASS:
                        drop=True
                        break
                else :
                    drop=True
                    break
            if not drop:
                PASS,_=income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set)
                if PASS:
                    yield (int(nodes[0][0]),int(nodes[0][-1]),tuple(st_ids),*MID,tuple(ed_ids),AMOUNT,depth)                 
def fast_search(st_amts,ed_amts,batch,nodes,pre_tx,pre_ed,lst_tx,lst_st):
    AMOUNT=sum(ed_amts)
    if abs(AMOUNT/sum(st_amts,1e-5)-1)<=SIGMA:
        st_ids=pre_tx[:,0]
        ed_ids=lst_tx[:,0]
        pre_ed_set=set(pre_ed)
        depth=len(nodes[0])
        MID=[0]*(depth-3)
        for mid in range(1,depth-2):
            cur_tx,cur_id=np.unique(batch[:,mid,:],axis=0,return_index=True)
            cur_tx=cur_tx.reshape(-1,4)
            cur_st,cur_ed=nodes[cur_id,mid],nodes[cur_id,mid+1]
            cur_tx,cur_st,cur_ed=check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed)
            amts=cur_tx[:,-1]
            if abs(amts.sum()/AMOUNT-1)<=SIGMA:
                mid_ids=cur_tx[:,0]
                PASS,cur_ed_set=income_expenditure_check_out(pre_tx,pre_ed,cur_tx,cur_st,cur_ed,pre_ed_set,False)
                if PASS:
                    pre_ed_set=cur_ed_set
                    pre_tx,pre_ed=cur_tx,cur_ed
                    MID[mid-1]=tuple(mid_ids)
                else:
                    return None
            else:
                return None
        PASS,_=income_expenditure_check_out(pre_tx,pre_ed,lst_tx,lst_st,lst_st,pre_ed_set)
        if not PASS:return None
        return (int(nodes[0][0]),int(nodes[0][-1]),tuple(st_ids),*MID,tuple(ed_ids),AMOUNT,depth)
    return None
def search(iterator):
    bucket=[i[1] for i in iterator]
    length=len(bucket)
    for i,(nds,egs) in enumerate(bucket):
        st_nd,ed_nd,st_dt,ed_dt=nds[0],nds[-1],egs[0][1],egs[-1][1]
        batch=[egs]
        nodes=[nds]
        j=i+1
        while j<length:
            nds_,egs_=bucket[j]
            st_nd_,ed_nd_,st_dt_,ed_dt_=nds_[0],nds_[-1],egs_[0][1],egs_[-1][1]
            if st_nd==st_nd_ and ed_nd==ed_nd_ and ed_dt_<=st_dt+T:
                batch.append(egs_)
                nodes.append(nds_)
                j+=1
            else:
                break
        batch=np.array(batch)
        nodes=np.array(nodes,int)
        st_tx,st_id=np.unique(batch[:, 0,:],axis=0,return_index=True)
        st_ed=nodes[st_id,1]
        ed_tx,ed_id=np.unique(batch[:,-1,:],axis=0,return_index=True)
        ed_st=nodes[ed_id,-2]
        st_amts=st_tx[:,-1]
        ed_amts=ed_tx[:,-1]
        amts_len=len(st_amts)+len(ed_amts)
        if amts_len<=LIMIT :
            for r in binary_search(st_amts,ed_amts,batch,nodes,st_tx,st_ed,ed_tx,ed_st):
                yield r
        else:
            r= fast_search(st_amts,ed_amts,batch,nodes,st_tx,st_ed,ed_tx,ed_st)
            if r is not None:
                yield r
            else:
                batch=batch[:LIMIT,:,:]
                nodes=nodes[:LIMIT,:]
                st_tx,st_id=np.unique(batch[:, 0,:],axis=0,return_index=True)
                st_ed=nodes[st_id,1]
                ed_tx,ed_id=np.unique(batch[:,-1,:],axis=0,return_index=True)
                ed_st=nodes[ed_id,-2]
                st_amts=st_tx[:,-1]
                ed_amts=ed_tx[:,-1]
                for r in binary_search(st_amts,ed_amts,batch,nodes,st_tx,st_ed,ed_tx,ed_st):
                    yield r
srcs_rdd4=srcs_rdd3.mapPartitions(search).distinct()
srcs_rdd3_part=srcs_rdd2.mapPartitions(search).distinct()
result=srcs_rdd4.collect()
results3=srcs_rdd3_part.collect()
r=deque()
r.extend(sorted(result,key=lambda x:-sum(map(len,x[2:-2]))))
r.extend(sorted(results3,key=lambda x:-sum(map(len,x[2:-2]))))
item=r.popleft()
union=lambda x,y:x|y
s=reduce(union,map(set,item[2:-2]))
increase_id=0
RESULT=[[increase_id,id2name[item[0]],id2name[item[1]],float(item[-2]),item[-1],s]]
while r:
    item=r.popleft()
    s=reduce(union,map(set,item[2:-2]))
    not_sub=True
    for S in RESULT:
        if not (s-S[-1]):
            not_sub=False
            break
    if not_sub:
        increase_id+=1
        RESULT.append([increase_id,id2name[item[0]],id2name[item[1]],float(item[-2]),item[-1],s])
RESULT_rdd=sc.parallelize(RESULT,P)
def flatValue(iterator):
    for *i,s in iterator:
        for payid in sorted(s):
            yield i+[int(payid)]
RESULT_rdd2=RESULT_rdd.mapPartitions(flatValue).toDF('''batch_id: int, src: string, dst: string, amount_sum: float, depth: int, id: int''')
RESULT_rdd2.join(df,'id','left').repartition(1).write.parquet("hdfs://localhost:9000/RESULT",mode='overwrite')
