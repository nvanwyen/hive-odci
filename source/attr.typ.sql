--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - attrs.typ.sql
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
prompt ... running attrs.typ.sql

--
alter session set current_schema = hive;

-- column description, marries up to ANYTYPE.ADDATTR
create or replace type attribute is object
(
    name  varchar2( 256 ),
    code  number,
    prec  number,
    scale number,
    len   number,
    csid  number,
    csfrm number
);
/

--
show errors

-- array of column descriptions
create or replace type attributes as table of attribute;
/

--
show errors

-- column data, marries up to ANYDATA
create or replace type data as object
(
    code          number,   -- see also: attribute.code
    --
    val_varchar2  varchar2( 32767 ),
    val_number    number,
    val_date      date,
    val_timestamp timestamp,
    val_clob      clob,
    val_blob      blob
    -- other data type not yet supported
);
/

--
show errors

-- array of column data
create or replace type records as table of data;
/

--
show errors

-- 
create or replace type connection as object
(
    url  varchar2( 4000 ),  -- param: hive_jdbc_url[.x]
    name varchar2( 256 ),   --        hive_user
    pass varchar2( 256 ),   --        hive_pass
    auth varchar2( 40 )     --        hive_auth
);
/

--
show errors

--
-- ... done!
--
