--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.ctx.sql
--

--
prompt ... running hive.ctx.sql

--
alter session set current_schema = hive;

--
create or replace context hivectx using impl;

--
-- ... done!
--
