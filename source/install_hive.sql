--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - install_hive.sql
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
set linesize 160
set pagesize 50000
set trimspool on
set serveroutput on

--
!( ./ver )

--
whenever oserror  exit 9;
whenever sqlerror exit sql.sqlcode;

--
column logfile new_value logfile noprint;

--
set termout off;
select sys_context( 'userenv', 'db_name' ) 
    || '_install_hive_odci.' 
    || to_char( sysdate, 'YYYYMMDDHH24MISS' ) 
    || '.log' logfile
  from dual;
set termout on;

--
spool &&logfile append

--
prompt ... running install_hive.sql

--
select current_timestamp "beginning installation"
  from dual;

-- schema
@@hive.usr.sql

-- roles
@@hive.rol.sql

-- schema permissions
@@hive.prm.sql
@@hive.jva.sql

-- tables
@@hive.tbl.sql
@@hive.idx.sql

-- views
@@hive.vws.sql

-- utilities
@@hive.utl.sql

-- types
@@hive.ctx.sql
@@attr.typ.sql
@@bind.typ.sql

-- implemntation
@@impl.pks.sql
@@impl.pkb.sql

-- administrative
@@dbms_hive.pks.sql
@@dbms_hive.pkb.sql

-- transient
@@hive.typ.sql
@@hive.fnc.sql

-- interface
@@hive.pks.sql
@@hive.pkb.sql

-- synonyms
@@hive.syn.sql

-- grants
@@hive.gnt.sql

-- parameters
@@hive.par.sql

-- -- obfuscation
@@wrap.pls.sql

--
select current_timestamp "completed installation"
  from dual;

--
prompt ... show post installation object errors

--
set linesize 160
set pagesize 50000

--
col name for a30 head "name"
col text for a80 head "text" word_wrap

--
select name,
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

        raise_application_error( -20001, to_char( c ) || ' installation error(s) encountered, please review' );

    else

        dbms_output.put_line( 'Installation successful' );

    end if;

end;
/

--
!( ./java/compile.sh )

--
!( ./jdbc/load-jdbc.sh "sys" "sys" )

-- note: A patch may be required if you are getting O/S Message: No child
--       processes, see MOS: Doc ID 2021977.1, apply Patch 19033356.
--       SQLPLUS WHENEVER OSERROR FAILS REGARDLESS OF OS COMMAND
--       RESULT to resolve this error

--
spool off
exit

--
-- ... done!
--
