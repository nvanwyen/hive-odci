--------------------------------------------------------------------------------
--
-- 2016-04-07, NV - hive.idx.sql
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
prompt ... running hive.idx.sql

--
alter session set current_schema = hive;

--
create unique index param$name
    on param$ ( name )
/

--
create unique index filter$key
    on filter$ ( key, seq )
/

--
create index filter$seq
    on filter$ ( seq, type, scope )
/

--
create unique index priv$key
    on priv$ ( key, id# )
/

--
create index priv$lvl
    on priv$ ( key, id#, lvl )
/

--
-- ... done!
--
