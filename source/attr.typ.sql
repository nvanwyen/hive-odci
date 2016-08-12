--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - attrs.typ.sql
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

-- array of column descriptions
create or replace type attributes as table of attribute;
/

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

show errors

--
-- ... done!
--
