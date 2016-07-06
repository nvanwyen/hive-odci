--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - install_hive.sql
--

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
@@hive.jva.sql
@@hive.typ.sql
@@hive.fnc.sql

-- interface
@@hive.pks.sql
@@hive.pkb.sql

-- grants
@@hive.gnt.sql

-- synonyms
@@hive.syn.sql

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
 where owner = 'HIVE';

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
prompt
prompt Run: jdbc/load-jdbc.sh "sys"
prompt .... policy/load-policy.sh "sys"
prompt

--
spool off
exit

--
-- ... done!
--
