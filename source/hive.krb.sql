--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.krb.sql
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

-- This script helps configure Hive-ODCI DBMS-JAVA Permissions and 
-- with Kerberos Authentication. This script *does not* run as part of the
-- installation, it is provided here only as a helper script

--
prompt ... running hive.krb.sql

set verify off

-- krb5 conf file
accept krb5_conf char -
prompt 'Full path to krb5.conf: '

-- jaas conf file
accept jaas_conf char -
prompt 'Full path to jaas.conf: '

-- jaas index name
accept jaas_idx char -
prompt 'Index name in jaas.conf: '

-- kerberos domain
accept krb_domain char -
prompt 'Kerberos Domain Name: '

-- kerberos kdc server
accept kdc_server char -
prompt 'Kerberos KDC Server: '

-- hive service principle
accept hive_princ char - 
prompt 'Hive Service Principle: '

-- oracle account principle name
accept orcl_princ char - 
prompt 'Oracle User Principle: '

-- verify
prompt 'Full path to krb5.conf:  &&krb5_conf'
prompt 'Full path to jaas.conf:  &&jaas_conf'
prompt 'Index name in jaas.conf: &&jaas_idx'
prompt 'Kerberos Domain Name:    &&krb_domain'
prompt 'Kerberos KDC Server:     &&kdc_server'
prompt 'Hive Service Principle:  &&hive_princ'
prompt 'Oracle User Principle:   &&orcl_princ'

-- 
prompt "Ready?"
accept ready char - 
prompt 'Ready? (emter continues, ctrl+c (^c) aborts) '

--
set serveroutput on
--
declare

    --
    function next_ return number is

        c number := 0;
        n number := 0;

    begin

        --
        select count(0) into c
          from hive.param$
         where name like 'java_property.%';

        --
        for i in 1 .. c loop

            --
            select count(0) into n
              from hive.param$
             where name = 'java_property.' || to_char( i );

            --
            if ( i > 0 ) then n := i; else exit; end if;

        end loop;

        --
        if ( n = 0 ) then n := 1; end if;

        --
        return n;

    end next_;

    --
    function find_( p in varchar2 ) return number is

        n number := 0;
        c number := next_;

    begin

        --
        if ( p is not null ) then

            --
            for i in 1 .. c loop

                --
                select count(0) into n
                  from hive.param$
                 where name = 'java_property.' || to_char( i )
                  and value like p || '=%';

                --
                if ( n > 0 ) then

                    exit;

                end if;

            end loop;

        end if;

        --
        return n;

    end find_;

    --
    procedure set_( n in varchar2, v in varchar2 ) is
    begin

        --
        if ( ( n is not null ) and 
             ( v is not null ) ) then

            i := find_( n );

            --
            if ( i = 0 ) then

                i := next_;

                insert into hive.param$
                    ( name, value )
                values
                    ( 'java_property.' || to_char( i ),  n || '=' || v );

            else

                update hive.param$
                   set value = n || '=' || v
                 where name = 'java_property.' || to_char( i );

            end if;

        end if;

    end set_;

begin

    --
    set_( 'java.security.krb5.kdc',          '&&kdc_server' );
    set_( 'java.security.krb5.realm',        '&&krb_domain' );
    set_( 'java.security.krb5.conf',         '&&krb5_conf' );
    set_( 'java.security.auth.login.config', '&&jaas_conf' );
    set_( 'java.security.auth.login.index',  '&&jaas_idx' );

    --
    commit;

end;
/

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.realm', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.kdc', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.krb5.conf', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.auth.login.index', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'java.security.auth.login.config', 'write' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.util.PropertyPermission', 'sun.security.krb5.debug', 'write' );


--
exec dbms_java.grant_permission( 'HIVE', 'SYS:oracle.aurora.rdbms.security.PolicyTablePermission', '0:javax.security.auth.kerberos.DelegationPermission#*', '');
exec dbms_java.grant_permission( 'HIVE', 'SYS:oracle.aurora.rdbms.security.PolicyTablePermission', '0:javax.security.auth.PrivateCredentialPermission#*', '');

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'putProviderProperty.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', '*', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.other', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.&&jaas_idx', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'createLoginContext.&&jaas_idx', '' );

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'doAs', '' );
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.AuthPermission', 'getSubject', '' );

--
exec dbms_java.grant_permission( 'HIVE', 'SYS:javax.security.auth.kerberos.ServicePermission', '*', 'initiate' );

--
exec dbms_java.grant_permission( 'HIVE', -
                                 'SYS:javax.security.auth.kerberos.DelegationPermission', -
                                 '"&&hive_princ" "krbtgt/&&kdc_server@&&krb_domain"', -
                                 '' );

--
exec dbms_java.grant_permission( 'HIVE', -
                                 'SYS:javax.security.auth.PrivateCredentialPermission', -
                                 'javax.security.auth.kerberos.KerberosTicket javax.security.auth.kerberos.KerberosPrincipal "*"', -
                                 'read');

--
-- ... done!
--
