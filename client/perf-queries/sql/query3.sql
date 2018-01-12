select
  min(l_orderkey),
  min(l_partkey),
  min(l_suppkey),
  min(l_linenumber),
  min(l_quantity),
  min(l_extendedprice),
  min(l_discount),
  min(l_tax),
  min(l_returnflag),
  min(l_linestatus),
  min(l_shipdate),
  min(l_commitdate),
  min(l_receiptdate),
  min(l_shipinstruct),
  min(l_shipmode),
  min(l_comment)
from lineitem;
