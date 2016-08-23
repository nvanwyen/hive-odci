--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - install_hive.sql
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

--
-- note: A patch may be requireed if you are getting "O/S Message: No child\
--       processes", see MOS: Doc ID 2021977.1
--
--       Patch 19033356: SQLPLUS WHENEVER OSERROR FAILS REGARDLESS OF OS COMMAND RESULT.
--

--
spool off
exit

--
-- ... done!
--
