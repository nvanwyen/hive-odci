--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.gnt.sql
--

--
prompt ... running hive.gnt.sql

--
alter session set current_schema = hive;

-- hive_user grants
--
grant execute on hive.hive to hive_user;
--
grant execute on hive.attribute to hive_user;
grant execute on hive.attributes to hive_user;
grant execute on hive.data to hive_user;
grant execute on hive.records to hive_user;
grant execute on hive.connection to hive_user;
grant execute on hive.bind to hive_user;
grant execute on hive.binds to hive_user;
grant execute on hive.binding to hive_user;

--
grant select on hive.dba_hive_params to hive_user;
grant select on hive.dba_hive_filters to hive_user;
grant select on hive.dba_hive_log to hive_user;

-- hive_admin grants
--
grant select, insert, update, delete on hive.param$ to hive_admin;
grant select, insert, update, delete on hive.filter$ to hive_admin;
grant select, insert, update, delete on hive.log$ to hive_admin;

--
grant execute on hive.dbms_hive to hive_admin;
grant execute on hive.impl to hive_admin;

--
grant hive_user to hive_admin;
grant hive_admin to dba;

--
-- ... done!
--
