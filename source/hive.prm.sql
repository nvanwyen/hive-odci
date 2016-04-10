--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.prm.sql
--

--
prompt ... running hive.prm.sql

--
grant resource to hive;

--
alter user hive quota unlimited on users;

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.net.SocketPermission', '*', 'connect, resolve' );

--
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.io.FilePermission', '*' );
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.lang.RuntimePermission', '*' );

--
grant execute on dbms_session to hive;

--
-- ... done!
--
