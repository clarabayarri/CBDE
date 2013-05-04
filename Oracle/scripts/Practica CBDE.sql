-- Check quota
select bytes/max_bytes * 100 from user_ts_quotas where tablespace_name = 'USERS';



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



---------- INDEXES


---- Bitmap

-- Check if enough repetitions to create bitmap

select count(*)/count(distinct p_size) from part;
-- 1.12 -- no
select count(*)/count(distinct p_partkey) from part;
-- 1 -- no

select count(*) from supplier;
-- 66

select count(*)/count(distinct ps_partkey) from partsupp;
-- 4.3 -- no
select count(*)/count(distinct ps_suppkey) from partsupp;
-- 80.7 -- no


select count(*)/count(distinct c_mktsegment) from customer;
-- 1.1 -- no


select count(*)/count(distinct o_custkey) from orders;
-- 10 -- no


select count(*)/count(distinct l_orderkey) from lineitem;
-- 4.2 -- no


select count(*)/count(distinct c_nationkey) from customer;
-- 40 -- no


select count(*)/count(distinct o_custkey) from orders;
-- 10 -- no


select count(*)/count(distinct l_suppkey) from lineitem;
-- 606 -- YES

-- we can only build a bitmap for lineitem(l_suppkey)

create bitmap index ind_l_suppkey on lineitem(l_suppkey) pctfree 0;


---- B+

create index ind_0_custkey on orders(o_custkey) pctfree 33;
create index ind_l_shipdate on lineitem(l_shipdate) pctfree 33;
create index ind_l_orderkey on lineitem(l_orderkey) pctfree 33;
create index ind_s_nationkey on supplier(s_nationkey) pctfree 33;
create index ind_c_mktsegment on customer(c_mktsegment) pctfree 33;
create index ind_c_nationkey on customer(c_nationkey) pctfree 33;
create index ind_ps_partkey on partsupp(ps_partkey) pctfree 33;
create index ind_p_size on part(p_size) pctfree 33;
create index ind_n_regionkey on nation(n_regionkey) pctfree 33;

-- other options have been discarded as they are not used



-- MATERIALIZED VIEWS

  
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

  
