--------------------------------------------------------------------------------
--
-- 2017-03-24, NV - patch.oracle.v0.1.5.37
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
-- This script patches a version v0.1.5.28 of Hive-ODCI up to v0.1.5.37
-- the significant changes are:
--
--      1) Adding BIND() constructors for ease of use
--
--      2) Adding ENV() functionality to BINDING package for dynamic variables
--
-- Patch fixes for Bug# 4 - Need Ad-Hoc binding support for SYS_CONTEXT
--

--
column log new_value log noprint;

--
set termout off;
select sys_context( 'userenv', 'db_name' )
    || '_patch.oracle.v0.1.5.37.'
    || to_char( sysdate, 'YYYYMMDDHH24MISS' )
    || '.log' log
  from dual;
set termout on;

--
spool &&log append

--
prompt ... running patch.oracle.v0.1.5.37

--
whenever oserror  exit 9;
whenever sqlerror exit sql.sqlcode;

--
alter session set current_schema = hive;

--
select current_timestamp "beginning patch"
  from dual;

--
set serveroutput on

--
declare

    ver varchar2( 4000 );

begin

    -- we should use dbms_hive.param( 'version' ) for this
    -- but this patch fixes an issue with the call ... doh!

    select a.value into ver
      from hive.param$ a
     where a.name = 'version';

    if ( ver != 'v0.1.5.28' ) then

        raise_application_error( -20900, 'This patch requires version v0.1.5.28, enountered: '
                                      || nvl( ver, 'NULL' ) );

    else

        dbms_output.put_line( 'Version [' || nvl( ver, 'NULL' ) || '] verified' );

    end if;

end;
/

--
drop view sys_user$;
drop view sys_resource_group_mapping$;
drop view sys_user_astatus_map$;
drop view dba_hive_params;
drop view dba_hive_filters;
drop view dba_hive_filter_privs;
drop view dba_hive_log;
drop view user_hive_params;
drop view user_hive_filters;
drop view user_hive_filter_privs;

--
drop function hive_q;
drop function bitor;
drop function bitxor;
drop function bitnot;
drop function oid;
drop function oname;

--
drop package binding;
drop package dbms_hive;
drop package remote;
drop package impl;

--
drop type hive_t;
drop type binds;
drop type bind;
drop type connection;
drop type records;
drop type data;
drop type attributes;
drop type attribute;

-- views
@@../../source/hive.vws.sql

-- utilities
@@../../source/hive.utl.sql

-- types
@@../../source/hive.ctx.sql
@@../../source/attr.typ.sql
@@../../source/bind.typ.sql

-- implemntation
@@../../source/impl.pks.sql
@@../../source/impl.pkb.sql

-- administrative
@@../../source/dbms_hive.pks.sql
@@../../source/dbms_hive.pkb.sql

-- transient
@@../../source/hive.typ.sql
@@../../source/hive.fnc.sql

-- interface
@@../../source/hive.pks.sql
@@../../source/hive.pkb.sql

-- synonyms
@@../../source/hive.syn.sql

-- obfuscation
@@../../source/wrap.pls.sql

-- (re)validate (e.g. compile) any invalid objects
@@../patch_validate.sql

--
select current_timestamp "completed patch"
  from dual;

--
prompt ... show post patch object errors

--
set linesize 160
set pagesize 50000

--
col name for a20   head "name"
col type for a16   head "type"
col line for 9,990 head "line"
col text for a60   head "text" word_wrap

--
select name,
       type,
       line,
       text
  from all_errors
 where owner = 'HIVE'
   and text not like 'Note: %';

--
declare

    c number := 0;

begin

    select count(0) into c
      from all_errors
     where owner = 'HIVE'
       and text not like 'Note: %';

    if ( c > 0 ) then

        raise_application_error( -20901, to_char( c ) || ' patch error(s) encountered, please review' );

    else

        dbms_output.put_line( 'Patch successful' );

    end if;

end;
/
--
spool off
exit

--
-- ... done!
--
