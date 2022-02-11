# Anti-money laundering

Dedect relationship between 'a' and 'e' even 'f', 'g', 'h', etc by tracing through payments with similar amounts and identifying payment chains from the transaction data. It is a pattern graph search problem. 

For example：
 
|  id|accname|  Event_Dt|Tx_Amt|Cntpty_Acct_Name|
|----| ----- | -------- | ---- | ------- |
|5068|      a|2020-01-01|  20.0|            b|
|5069|      a|2020-01-02| 300.0|            b|
|5070|      a|2020-01-03| 180.0|            b|
|5071|      b|2020-01-03|  40.0|            c|
|5072|      b|2020-01-03| 500.0|            c|
|5073|      b|2020-01-04|  10.0|            c|
|5074|      b|2020-01-03| 150.0|            d|
|5075|      c|2020-01-04|  50.0|            e|
|5076|      d|2020-01-04| 150.0|            e|

You can run
`spark-submit  --files some_ideas.cp36-win_amd64.pyd Aml.py` in windows environment or `spark-submit  --files some_ideas.cp36-x86_64-linux.so Aml.py` on linux platform to get the payment chains seem like money laundering. 

![1](https://user-images.githubusercontent.com/24219258/149096741-85d7c637-a8bc-489e-a499-9f4a1eb047ea.png)

|id|chain_id|src|dst|amount|depth|accname|  Event_Dt|Tx_Amt|Cntpty_Acct_Name|
| -- |--- | --- | ---| ---- |--- | ---- | --------- | ----- | ---- |
|5068|       0|  a|  e|     200.0|     3|      a|2020-01-01|  20.0|               b|
|5070|       0|  a|  e|     200.0|     3|      a|2020-01-03| 180.0|               b|
|5071|       0|  a|  e|     200.0|     3|      b|2020-01-03|  40.0|               c|
|5073|       0|  a|  e|     200.0|     3|      b|2020-01-04|  10.0|               c|
|5074|       0|  a|  e|     200.0|     3|      b|2020-01-03| 150.0|               d|
|5075|       0|  a|  e|     200.0|     3|      c|2020-01-04|  50.0|               e|
|5076|       0|  a|  e|     200.0|     3|      d|2020-01-04| 150.0|               e|


Same 'chain_id' indicates these transactions belong to a complete chain. Field 'depth' shows the length of the chain. Enjoy it ! 🤗

## References

\[1\] [Zaharia M, Chowdhury M, Franklin M J, et al. Spark: Cluster computing with working sets[J]. HotCloud, 2010, 10(10-10): 95.](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2010/EECS-2010-53.html)
