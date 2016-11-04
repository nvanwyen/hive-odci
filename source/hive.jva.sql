--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.prm.sql
--

/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/

--
prompt ... running hive.jva.sql

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
exec dbms_java.grant_permission( 'JAVA_ADMIN', 'SYS:oracle.aurora.rdbms.security.PolicyTablePermission', '0:javax.security.auth.kerberos.DelegationPermission#*', '');
exec dbms_java.grant_permission( 'JAVA_ADMIN', 'SYS:oracle.aurora.rdbms.security.PolicyTablePermission', '0:javax.security.auth.PrivateCredentialPermission#*', '');

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'putProviderProperty.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.other', '' );

-- kerberos deligation
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', '*', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.JDBC_DRIVER_01', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.Client', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'doAs', '' );

exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.kerberos.ServicePermission', '*', 'initiate' );

exec dbms_java.grant_permission( 'HIVE', -
                                 'SYS:javax.security.auth.kerberos.DelegationPermission', -
                                 '"hive/hive@MTIHQ.com" "krbtgt/kerberos@MTIHQ.com"', -
                                 '' );

exec dbms_java.grant_permission( 'HIVE', -
                                 'SYS:javax.security.auth.PrivateCredentialPermission', -
                                 'javax.security.auth.kerberos.KerberosTicket javax.security.auth.kerberos.KerberosPrincipal "*"', -
                                 'read');

--
-- ... done!
--
