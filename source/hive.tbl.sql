--------------------------------------------------------------------------------
--
-- 2016-04-07, NV - hive.tbl.sql
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
prompt ... running hive.tbl.sql

--
alter session set current_schema = hive;

--
create table param$
(
    name  varchar2( 256 )  not null,
    value varchar2( 4000 )     null
)
/

--
create table filter$
(
    key   varchar2( 64 )   not null,
    seq   number           not null,
    type  number           not null,
    scope number           not null,
    value varchar2( 4000 )     null
)
/

--
create table priv$
(
    key varchar2( 64 )  not null,
    id# number          not null,
    lvl number          not null
)
/

--
create table log$
(
    stamp timestamp        not null,
    name  varchar2( 256 )  not null,
    type  number           not null,
    text  varchar2( 4000 )     null
)
/

--
create table auth$
(
    tab varchar2( 256 )  not null,
    id# number           not null,
    opr varchar2( 12 )   not null
)
/

--
-- ... done!
--
