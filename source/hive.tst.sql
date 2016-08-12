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

set linesize 160
set pagesize 50000

prompt ... running hive.tst.sql

--
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

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

exit
