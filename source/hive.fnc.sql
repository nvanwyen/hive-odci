--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.fnc.sql
--

--
prompt ... running hive.fnc.sql

--
alter session set current_schema = hive;

--
create or replace function hive_q( q varchar2 ) return anydataset pipelined using hive_t;
/

show errors