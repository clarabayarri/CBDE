-- QUERIES

-- Query 1
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as
sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount)
as avg_disc, count(*) as count_order
FROM lineitem
WHERE l_shipdate <= '30-APR-13'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

--Constraint: ÔdateÕ must be an existing date in the database.
-- date: '30-APR-13'
-- cost original: 1507

-- Query 2

SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone,
s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey AND 
  s_suppkey = ps_suppkey AND 
  p_size = 1000 AND 
  p_type like '%0123456789012' AND 
  s_nationkey = n_nationkey AND 
  n_regionkey = r_regionkey AND 
  r_name = '12345678901234567890123456789012' AND 
  ps_supplycost = (SELECT min(ps_supplycost) 
    FROM partsupp, supplier, nation, region 
    WHERE p_partkey = ps_partkey AND 
    s_suppkey = ps_suppkey AND 
    s_nationkey = n_nationkey AND
    n_regionkey = r_regionkey AND 
    r_name = '12345678901234567890123456789012')
ORDER BY s_acctbal desc, n_name, s_name, p_partkey;

-- Constraint: [size], [type] and [region] must be existing values in the database.
-- size: 1000
-- type: "12345678901234567890123456789012"
-- region: "12345678901234567890123456789012"
-- cost original: 308
-- cost amb MV: 7

-- Query 3

SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = '12345678901234567890123456789012' AND 
  c_custkey = o_custkey AND 
  l_orderkey = o_orderkey AND 
  o_orderdate < '30-APR-13' AND 
  l_shipdate > '20-APR-13'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue desc, o_orderdate;

-- Constraint: [segment] and both dates must be existing values in the database.
-- segment: "12345678901234567890123456789012"
-- date: '30-APR-13'
-- date: '30-APR-13'
-- cost original: 1916



-- Query 4

SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND 
  l_orderkey = o_orderkey AND 
  l_suppkey = s_suppkey AND 
  c_nationkey = s_nationkey AND 
  s_nationkey = n_nationkey AND
  n_regionkey = r_regionkey AND 
  r_name = '12345678901234567890123456789012' AND 
  o_orderdate >= '30-APR-13' AND 
  o_orderdate < '30-APR-14'
GROUP BY n_name
ORDER BY revenue desc;

--Constraint: [date] and [region] must be existing values in the database.
-- region: "12345678901234567890123456789012"
-- date: '30-APR-13'
-- cost original: 1936






-- PAS 1: POSAR PKS!!!!!! (fan indexos)










-- FRAGMENTACIO VERTICAL

create type grup_1 as object (
  cand INTEGER
);
create type grup_1_s as table of grup_1;
create type grup_2 as object (
  pobl INTEGER,
  edat INTEGER,
  val INTEGER
);
create type grup_2_s as table of grup_2;
create table poll_answers (
  ref INTEGER,
  atr_1 grup_1_s,
  atr_2 grup_2_s
)
nested table atr_1 store as atr_1_nt,
nested table atr_2 store as atr_2_nt
PCTFREE 0 ENABLE ROW MOVEMENT;







-- INDEXOS

-- B+
ALTER TABLE name SHRINK SPACE;
create index ind_1 on atr_2_nt(cand, edat) pctfree 33;



-- Bitmap
ALTER TABLE name SHRINK SPACE;
ALTER TABLE table MINIMIZE RECORDS_PER_BLOCK;
create bitmap index ind_1 on atr_2_nt(cand, edat) pctfree 0;

-- Query 1
SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as
sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount)
as avg_disc, count(*) as count_order
FROM lineitem
WHERE l_shipdate <= '30-APR-13'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- No te sentit pq el where es un rang
ALTER TABLE lineitem SHRINK SPACE;
ALTER TABLE lineitem MINIMIZE RECORDS_PER_BLOCK;
create bitmap index ind_1 on atr_2_nt(cand, edat) pctfree 0;



-- Query 2
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone,
s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey AND 
  s_suppkey = ps_suppkey AND 
  p_size = 1000 AND 
  p_type like '%0123456789012' AND 
  s_nationkey = n_nationkey AND 
  n_regionkey = r_regionkey AND 
  r_name = '12345678901234567890123456789012' AND 
  ps_supplycost = (SELECT min(ps_supplycost) 
    FROM partsupp, supplier, nation, region 
    WHERE p_partkey = ps_partkey AND 
    s_suppkey = ps_suppkey AND 
    s_nationkey = n_nationkey AND
    n_regionkey = r_regionkey AND 
    r_name = '12345678901234567890123456789012')
ORDER BY s_acctbal desc, n_name, s_name, p_partkey;

select count(*) from part;
-- 1332
select count(distinct p_size) from part;
-- 1201 -- no
select count(distinct p_partkey) from part;
-- 1332 -- no

select count(*) from supplier;
-- 66

select count(*) from partsupp;
-- 5332
select count(distinct ps_partkey) from partsupp;
-- 1263 -- no
select count(distinct ps_suppkey) from partsupp;
-- 66 -- no




-- Query 3
SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = '12345678901234567890123456789012' AND 
  c_custkey = o_custkey AND 
  l_orderkey = o_orderkey AND 
  o_orderdate < '30-APR-13' AND 
  l_shipdate > '20-APR-13'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue desc, o_orderdate;

select count(*) from customer;
-- 998
select count(distinct c_mktsegment) from customer;
-- 915 -- no

select count(*) from orders;
-- 9998
select count(distinct o_custkey) from orders;
-- 993 -- no

select count(*) from lineitem;
-- 39998
select count(distinct l_orderkey) from lineitem;
-- 9302 -- no



-- Query 4
SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND 
  l_orderkey = o_orderkey AND 
  l_suppkey = s_suppkey AND 
  c_nationkey = s_nationkey AND 
  s_nationkey = n_nationkey AND
  n_regionkey = r_regionkey AND 
  r_name = '12345678901234567890123456789012' AND 
  o_orderdate >= '30-APR-13' AND 
  o_orderdate < '30-APR-14'
GROUP BY n_name
ORDER BY revenue desc;

select count(*) from customer;
-- 998
select count(distinct c_nationkey) from customer;
-- 25 -- no

select count(*) from orders;
-- 9998
select count(distinct o_custkey) from orders;
-- 993 -- no

select count(*) from lineitem;
-- 39998
select count(distinct l_suppkey) from lineitem;
-- 66 -- YES



-- MATERIALIZED VIEWS

CREATE MATERIALIZED VIEW view_query1 ORGANIZATION HEAP PCTFREE 0 
  BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS 
    SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as
        sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount)
        as avg_disc, count(*) as count_order
        FROM lineitem
        WHERE l_shipdate <= '30-APR-13'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus; 
  
CREATE MATERIALIZED VIEW view_query1 ORGANIZATION HEAP PCTFREE 0 
  BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS 
    SELECT l_returnflag, l_linestatus, l_quantity,
        l_extendedprice, l_extendedprice, l_tax,
        l_discount, l_shipdate
        FROM lineitem
        WHERE l_shipdate <= '30-APR-13'; 
  
  
  
  
  
CREATE MATERIALIZED VIEW view_query2 ORGANIZATION HEAP PCTFREE 0 
  BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS  
    SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone,
        s_comment
        FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey AND 
          s_suppkey = ps_suppkey AND 
          p_size = 1000 AND 
          p_type like '%0123456789012' AND 
          s_nationkey = n_nationkey AND 
          n_regionkey = r_regionkey AND 
          r_name = '12345678901234567890123456789012' AND 
          ps_supplycost = (SELECT min(ps_supplycost) 
            FROM partsupp, supplier, nation, region 
            WHERE p_partkey = ps_partkey AND 
            s_suppkey = ps_suppkey AND 
            s_nationkey = n_nationkey AND
            n_regionkey = r_regionkey AND 
            r_name = '12345678901234567890123456789012')
        ORDER BY s_acctbal desc, n_name, s_name, p_partkey;

  
CREATE MATERIALIZED VIEW view_query3 ORGANIZATION HEAP PCTFREE 0 
  BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS  
    SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue,
        o_orderdate, o_shippriority
        FROM customer, orders, lineitem
        WHERE c_mktsegment = '12345678901234567890123456789012' AND 
          c_custkey = o_custkey AND 
          l_orderkey = o_orderkey AND 
          o_orderdate < '30-APR-13' AND 
          l_shipdate > '20-APR-13'
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue desc, o_orderdate;
  
CREATE MATERIALIZED VIEW view_query4 ORGANIZATION HEAP PCTFREE 0 
  BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS  
    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey AND 
          l_orderkey = o_orderkey AND 
          l_suppkey = s_suppkey AND 
          c_nationkey = s_nationkey AND 
          s_nationkey = n_nationkey AND
          n_regionkey = r_regionkey AND 
          r_name = '12345678901234567890123456789012' AND 
          o_orderdate >= '30-APR-13' AND 
          o_orderdate < '30-APR-14'
        GROUP BY n_name
        ORDER BY revenue desc;