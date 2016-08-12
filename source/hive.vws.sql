--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.vws.sql
--

/*
 Copyright (c) 2016, Metasystems Technologies Inc (MTI), Nicholas Van Wyen
 All rights reserved.
 
 Redistribution and use in source and binary forms, with or without 
 modification, are permitted provided that the following conditions are met:
 
 1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.
 
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 
 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software
    without specific prior written permission.
 
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
create or replace view dba_hive_params
as
select name,
       case when name not in ( 'application',
                               'version',
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
       text
  from log$
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
-- ... done!
--
