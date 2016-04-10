--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.gnt.sql
--

--
prompt ... running hive.gnt.sql

--
alter session set current_schema = hive;

--
grant execute on hive.dbms_hive to hive_admin;
--
grant select, insert, update, delete on hive.param$ to hive_admin;
grant select, insert, update, delete on hive.filter$ to hive_admin;

--
grant execute on hive.hive to hive_user;
--
grant select on hive.dba_hive_params to hive_user;
grant select on hive.dba_hive_filters to hive_user;

--
-- ... done!
--
