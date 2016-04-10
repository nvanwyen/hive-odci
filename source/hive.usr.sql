--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.usr.sql
--

--
prompt ... running hive.usr.sql

--
create user hive
    identified by values 'FFFFFFFFFFFFFFFF'
    default tablespace users
    temporary tablespace temp
    account lock;

--
grant resource to hive;

--
alter user hive quota unlimited on users;

--
-- ... done!
--
