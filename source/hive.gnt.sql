--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.gnt.sql
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
prompt ... running hive.gnt.sql

--
alter session set current_schema = hive;

-- hive_user grants
--
grant execute on hive.remote to hive_user;
grant execute on hive.hive_q to hive_user;

--
grant execute on hive.attribute to hive_user;
grant execute on hive.attributes to hive_user;
grant execute on hive.data to hive_user;
grant execute on hive.records to hive_user;
grant execute on hive.connection to hive_user;
grant execute on hive.bind to hive_user;
grant execute on hive.binds to hive_user;
grant execute on hive.binding to hive_user;

--
grant execute on hive_hint to hive_user;

--
grant select on hive.dba_hive_params to hive_admin;
grant select on hive.dba_hive_filters to hive_admin;
grant select on hive.dba_hive_filter_privs to hive_admin;
grant select on hive.dba_hive_log to hive_admin;
grant select on hive.dba_hive_privs to hive_admin;

grant select on hive.user_hive_params to hive_user;
grant select on hive.user_hive_filters to hive_user;
grant select on hive.user_hive_filter_privs to hive_user;
grant select on hive.user_hive_log to hive_user;
grant select on hive.user_hive_privs to hive_user;

-- hive_admin grants
--
grant select, insert, update, delete on hive.param$ to hive_admin;
grant select, insert, update, delete on hive.filter$ to hive_admin;
grant select, insert, update, delete on hive.log$ to hive_admin;

--
grant execute on hive.dbms_hive to hive_admin;
grant execute on hive.impl to hive_admin;

--
grant hive_user to hive_admin;
grant hive_admin to dba;

--
-- ... done!
--
