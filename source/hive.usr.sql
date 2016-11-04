--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.usr.sql
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
prompt ... running hive.usr.sql

column defts new_value defts noprint;
column tmpts new_value tmpts noprint;

set term off
select default_tablespace   defts from dba_users where username = 'SYSTEM';
select temporary_tablespace tmpts from dba_users where username = 'SYSTEM';
set term on

set verify off

--
create user hive
    identified by values 'FFFFFFFFFFFFFFFF'
    default tablespace &&defts
    temporary tablespace &&tmpts
    account lock;

--
grant resource to hive;

--
alter user hive quota unlimited on &&defts;

set verify on

--
-- ... done!
--
