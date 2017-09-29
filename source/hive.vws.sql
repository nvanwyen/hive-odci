--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.vws.sql
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
prompt ... running hive.vws.sql

--
alter session set current_schema = hive;

-- private views
create or replace view sys_user$
as
select * 
  from sys.user$
/

create or replace view sys_resource_group_mapping$
as
select *
  from sys.resource_group_mapping$
/

create or replace view sys_user_astatus_map$
as
select *
  from sys.user_astatus_map
/

--
create or replace view ora$user$
as
select u.user# id#,
       u.name  name,
       u.type# type
  from sys.user$ u
 where u.type# in ( 0 /* role */,
                    1 /* user */ )
/

--
create or replace view ora$role$priv$
as
select /*+ ordered */
       sa.grantee# grantee#,
       decode( sa.grantee#, 1, 'PUBLIC', u1.name ) grantee,
       u2.name granted_role,
       u2.user# granted_role#
  from sys.sysauth$ sa,
       sys.user$ u1,
       sys.user$ u2
 where u1.user# = sa.grantee#
   and u2.user# = sa.privilege#
/

--
create or replace view dba_hive_params
as
select name,
       case when name not in ( 'application',
                               'version',
                               'license',
                               'encrypted_values',
                               'hive_users',
                               'hive_admin',
                               'hive_jdbc_driver',
                               'query_limit' )
            then nvl( sys_context( 'hivectx', substr( name, 1, 30 ), 4000 ), value ) 
            else null
       end session_value,
       value system_value
  from param$;

--
create or replace view dba_hive_filters
as
select key,
       seq,
       decode( type,  1, 'type_bool',
                      2, 'type_date',
                      3, 'type_float',
                      4, 'type_int',
                      5, 'type_long',
                      6, 'type_null',
                      7, 'type_rowid',
                      8, 'type_short',
                      9, 'type_string',
                     10, 'type_time',
                     11, 'type_timestamp',
                     12, 'type_url',
                         'unknown' ) type,
       decode( scope, 1, 'scope_in',
                      2, 'scope_out',
                      3, 'scope_inout',
                         'unknown' ) scope,
       value
  from filter$
 order by key,
          seq;

--
create or replace view dba_hive_filter_privs
as
select p.key,
       u.name grantee,
       decode( bitand( p.lvl, 1 ), 0, 'NO', 'YES' ) read,
       decode( bitand( p.lvl, 2 ), 0, 'NO', 'YES' ) write
  from priv$ p,
       sys_user$ u left outer join
       sys_resource_group_mapping$ r
       on ( r.attribute = 'ORACLE_USER'
        and r.status = 'ACTIVE'
        and r.value = u.name ),
       sys_user_astatus_map$ m
 where ( ( u.astatus = m.status# )
      or ( u.astatus = ( m.status# + 16 - bitand( m.status#, 16 ) ) ) )
   and u.type# in ( 0, 1 )
   and u.user# = p.id#
 order by 1, 2;

--
create or replace view dba_hive_log
as
select stamp,
       name,
       type,
       decode( type, 0, 'none',
                     1, 'error',
                     2, 'warn',
                     4, 'info',
                     8, 'trace',
                        'unknown' ) tier,
       text
  from log$
 order by stamp;

--
create or replace view user_hive_log
as
select stamp,
       type,
       decode( type, 0, 'none',
                     1, 'error',
                     2, 'warn',
                     4, 'info',
                     8, 'trace',
                        'unknown' ) tier,
       text
  from log$
 where name = user
 order by stamp;

--
create or replace view user_hive_params
as
select name,
       decode( name, 'hive_pass', null, nvl( sys_context( 'hivectx', substr( name, 1, 30 ), 4000 ), value ) ) value
  from param$;

--
create or replace view user_hive_filters
as
select a.*
  from dba_hive_filters a,
       dba_hive_filter_privs b
 where a.key = b.key
   and ( b.grantee = user
      or b.grantee = 'PUBLIC' );

--
create or replace view user_hive_filter_privs
as
select *
  from dba_hive_filter_privs
 where grantee = user 
    or grantee = 'PUBLIC';

--
create or replace view dba_hive_privs
as
select a.tab  as table_name,
       a.opr  as privilege,
       b.name as grantee,
       decode( b.type, 0, 'ROLE',
                       1, 'USER',
                          'UNKNOWN' ) as grantee_type
  from auth$ a,
       ora$user$ b
 where a.id# = b.id#;

--
create or replace view user_hive_privs
as
select a.tab  as table_name,
       a.opr  as privilege,
       b.name as grantee,
       decode( b.type, 0, 'ROLE',
                       1, 'USER',
                          'UNKNOWN' ) as grantee_type
  from auth$ a,
       ora$user$ b
 where a.id# = b.id#
   and a.id# = ( select id#
                   from ora$user$
                 where name = user )
    or a.id# in ( select c.id#
                    from ora$user$ c,
                         ( select /*+ connect_by_filtering */
                                  e.grantee#,
                                  e.granted_role#
                             from ora$role$priv$ e
                          connect by e.grantee# = prior e.granted_role#
                            start with e.grantee = user ) d
                   where c.id# = d.granted_role# );

--
-- ... done!
--
