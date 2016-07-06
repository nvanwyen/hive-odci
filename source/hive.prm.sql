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
grant create any operator to hive;
grant execute any operator to hive;
grant unlimited tablespace to hive;

--
grant select on dba_tab_columns       to hive;
grant select on dba_views             to hive;
grant select on dba_constraints       to hive;
grant select on dba_tab_partitions    to hive;
grant select on dba_tab_subpartitions to hive;
grant select on dba_triggers          to hive;

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.net.SocketPermission', '*', 'connect, resolve' );

--
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.io.FilePermission', '*' );
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.lang.RuntimePermission', '*' );

--
grant execute on dbms_sql      to hive;
grant execute on dbms_session  to hive;
grant execute on dbms_standard to hive;

--
grant javasyspriv to hive;
grant javadebugpriv to hive;

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'putProviderProperty.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );

--
-- ... done!
--
