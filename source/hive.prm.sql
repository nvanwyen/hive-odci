--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.prm.sql
--

--
prompt ... running hive.prm.sql

--
grant resource to hive;

--
grant create any operator to hive;
grant execute any operator to hive;
grant unlimited tablespace to hive;

--
grant select on sys.user$                   to hive with grant option;
grant select on sys.resource_group_mapping$ to hive with grant option;
grant select on sys.user_astatus_map        to hive with grant option;

--
grant select on dba_tab_columns       to hive;
grant select on dba_views             to hive;
grant select on dba_constraints       to hive;
grant select on dba_tab_partitions    to hive;
grant select on dba_tab_subpartitions to hive;
grant select on dba_triggers          to hive;
grant select on dba_role_privs        to hive;

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.net.SocketPermission', '*', 'connect, resolve' );

--
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.io.FilePermission', '*' );
exec dbms_java.grant_policy_permission( 'HIVE', 'SYS', 'java.lang.RuntimePermission', '*' );

-- runtime, system and security properties
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', '*', null );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', '*', null );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.lang.RuntimePermission', '*', null );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.io.FilePermission', '*', null );

exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', '*', '*' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', '*', '*' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.lang.RuntimePermission', '*', '*' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.io.FilePermission', '*', '*' );

-- specific
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.realm', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.kdc', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.conf', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.auth.login.index', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.auth.login.config', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'sun.security.krb5.debug', 'write' );

-- kerberos login
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'getSubject', '' );

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.JDBC_DRIVER_01', '' );

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
