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

set linesize 160
set pagesize 50000

prompt ... running hive.tst.sql

--
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

--
exec dbms_hive.purge_log;
exec hive_remote.session_log_level( 31 );

--
col cust_id    for 9999990
col last_name  for a30
col first_name for a30
col total      for 9,999,990

-- simple query
select * from table( hive_q( 'select cust_id, last_name, first_name from cust',
                             null, 
                             null ) );

-- create a runtime only bind list (q-string example)
select * from table( hive_q( q'[select cust_id, 
                                       last_name, 
                                       first_name 
                                  from cust 
                                 where last_name = ?
                                   and cust_id between ? and ?
                                 order by cust_id asc]',
                             hive_binds( hive_bind( 'Hamada', 9 /* type_string */, 1 /* ref_in */ ),
                                         hive_bind( 1144011,  5 /* type_long */,   1 /* ref_in */ ),
                                         hive_bind( 1337250,  5 /* type_long */,   1 /* ref_in */ ) ) ) );

-- create a view
create or replace view hive.cust ( cust_id, last_name, first_name ) as
select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) )
/

set linesize 80
desc hive.cust

@../util/show_log

exit
