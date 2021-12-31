# aml
anti-money laundering

Dedect relationship between A and E by tracing through payments with similar amounts and identifying payment chains.

For example：


 
|accname|  Event_Dt|Tx_Amt|Cntpty_Acct_Name|daystamp|  id|     lag|      pre|
 | ------- | ---------- | ------ | ---------------- | -------- | ---- | -------- | --------- | 
|      a|2020-01-01|  20.0|               b|    7305|5068|  7306.0|-Infinity|
|      a|2020-01-02| 300.0|               b|    7306|5069|  7307.0|   7305.0|
|      a|2020-01-03| 180.0|               b|    7307|5070|Infinity|   7306.0|
|      b|2020-01-03|  40.0|               c|    7307|5071|  7307.0|-Infinity|
|      b|2020-01-03| 500.0|               c|    7307|5072|  7308.0|   7307.0|
|      b|2020-01-04|  10.0|               c|    7308|5073|Infinity|   7307.0|
|      b|2020-01-03| 150.0|               d|    7307|5074|Infinity|-Infinity|
|      c|2020-01-04|  50.0|               e|    7308|5075|Infinity|-Infinity|
|      d|2020-01-04| 150.0|               e|    7308|5076|Infinity|-Infinity|

You can run
` spark-submit  AML.py`
to get all the payment chains seem like money laundering.

 |  id|batch_id|src|dst|amount_sum|degree|accname|  Event_Dt|Tx_Amt|Cntpty_Acct_Name|
 | ---- | -------- | --- | --- | ---------- | ------ | ------- | ---------- | -------- | -------- |
|5068|       0|  a|  e|     200.0|     4|      a|2020-01-01|  20.0|               b|
|5070|       0|  a|  e|     200.0|     4|      a|2020-01-03| 180.0|               b|
|5071|       0|  a|  e|     200.0|     4|      b|2020-01-03|  40.0|               c|
|5073|       0|  a|  e|     200.0|     4|      b|2020-01-04|  10.0|               c|
|5074|       0|  a|  e|     200.0|     4|      b|2020-01-03| 150.0|               d|
|5075|       0|  a|  e|     200.0|     4|      c|2020-01-04|  50.0|               e|
|5076|       0|  a|  e|     200.0|     4|      d|2020-01-04| 150.0|               e|
|5069|       1|  a|  c|     480.0|     3|      a|2020-01-02| 300.0|               b|
|5070|       1|  a|  c|     480.0|     3|      a|2020-01-03| 180.0|               b|
|5072|       1|  a|  c|     480.0|     3|      b|2020-01-03| 500.0|               c|
|5068|       2|  a|  c|     500.0|     3|      a|2020-01-01|  20.0|               b|
|5069|       2|  a|  c|     500.0|     3|      a|2020-01-02| 300.0|               b|
|5070|       2|  a|  c|     500.0|     3|      a|2020-01-03| 180.0|               b|
|5072|       2|  a|  c|     500.0|     3|      b|2020-01-03| 500.0|               c|


