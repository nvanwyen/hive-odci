--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.vws.sql
--

--
prompt ... running hive.vws.sql

--
alter session set current_schema = hive;

--
create or replace view dba_hive_params
as
select name,
       nvl( sys_context( 'hivectx', substr( name, 1, 30 ), 4000 ), value ) session_value,
       value system_value
  from param$
 order by name;

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
       u.name grantee
  from priv$ p,
       sys.user$ u left outer join
       sys.resource_group_mapping$ r
       on ( r.attribute = 'ORACLE_USER'
        and r.status = 'ACTIVE'
        and r.value = u.name ),
       sys.user_astatus_map m
 where ( ( u.astatus = m.status# )
      or ( u.astatus = ( m.status# + 16 - bitand( m.status#, 16 ) ) ) )
   and u.type# in ( 0, 1 )
   and u.user# = p.id#
 order by 1, 2;

--
create or replace view dba_hive_log
as
select stamp,
       type,
       text
  from log$
 order by stamp;

--
-- ... done!
--
