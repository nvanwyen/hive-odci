--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.rol.sql
--

--
prompt ... running hive.rol.sql

--
create role hive_user not identified;

--
create role hive_admin not identified;

--
grant hive_user to hive_admin;

--
grant hive_admin to dba;

--
-- ... done!
--
