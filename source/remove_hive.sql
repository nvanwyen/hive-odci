--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - remove_hive.sql
--

--
prompt ... running remove_hive.sql

--
drop user hive cascade;

--
drop public synonym hive;
drop public synonym dbms_hive;
drop public synonym dba_hive_params;
drop public synonym dba_hive_filters;

--
drop role hive_user;
drop role hive_admin;

--
exit

--
-- ... done!
--
