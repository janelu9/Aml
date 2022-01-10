# Anti-money laundering

Dedect relationship between 'a' and 'e' by tracing through payments with similar amounts and identifying payment chains from the transaction data.

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
`spark-submit  --files jian_iteration.pyd  aml.py` in windows environment or `spark-submit  --files jian_iteration.so  aml.py` on linux platform to get the payment chains seem like money laundering. Same 'batch_id' indicates these transactions belong to a complete chain. Field 'depth' indicates the length of the chain. 

![image](https://user-images.githubusercontent.com/24219258/148644725-afb26de1-160e-4589-a035-3046a632b098.png)

|id|batch_id|src|dst|amount|depth|accname|  Event_Dt|Tx_Amt|Cntpty_Acct_Name|
| -- |--- | --- | ---| ---- |--- | ---- | --------- | ----- | ---- |
|5068|       0|  a|  e|     200.0|     4|      a|2020-01-01|  20.0|               b|
|5070|       0|  a|  e|     200.0|     4|      a|2020-01-03| 180.0|               b|
|5071|       0|  a|  e|     200.0|     4|      b|2020-01-03|  40.0|               c|
|5073|       0|  a|  e|     200.0|     4|      b|2020-01-04|  10.0|               c|
|5074|       0|  a|  e|     200.0|     4|      b|2020-01-03| 150.0|               d|
|5075|       0|  a|  e|     200.0|     4|      c|2020-01-04|  50.0|               e|
|5076|       0|  a|  e|     200.0|     4|      d|2020-01-04| 150.0|               e|
|5068|       1|  a|  c|     500.0|     3|      a|2020-01-01|  20.0|               b|
|5069|       1|  a|  c|     500.0|     3|      a|2020-01-02| 300.0|               b|
|5070|       1|  a|  c|     500.0|     3|      a|2020-01-03| 180.0|               b|
|5072|       1|  a|  c|     500.0|     3|      b|2020-01-03| 500.0|               c|

Enjoy it ! 🤗

## References

[1] https://github.com/janelu9/aml
