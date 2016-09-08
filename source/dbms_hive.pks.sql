--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - dbms_hive.pks.sql
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
prompt ... running dbms_hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package dbms_hive as

    --
    function exist( name in varchar2 ) return boolean;      -- paraemter exists

    --
    function param( name in varchar2 ) return varchar2;     -- get
    procedure param( name in varchar2, value in varchar2 ); -- set

    --
    procedure remove( name in varchar2 );                   -- unset (e.g. remove)

    --
    procedure purge_log;
    procedure purge_filter( key in varchar2 default null );

    --
    procedure move_ts( ts in varchar2, obj in varchar2 default null );

    ex_zero     exception;
    es_zero     constant varchar2( 256 ) := 'Tablespace does not exist or has a size of zero (0)';
    ec_zero     constant number := -20702;
    pragma      exception_init( ex_zero, -20702 );

    ex_segment  exception;
    es_segment  constant varchar2( 256 ) := 'Table has non-existent segment';
    ec_segment  constant number := -20703;
    pragma      exception_init( ex_segment, -20703 );

    ex_exists   exception;
    es_exists   constant varchar2( 256 ) := 'Table does not exist';
    ec_exists   constant number := -20704;
    pragma      exception_init( ex_exists, -20704 );

    ex_space    exception;
    es_space    constant varchar2( 256 ) := 'Insufficient space found for move operation';
    ec_space    constant number := -20705;
    pragma      exception_init( ex_space, -20705 );

    ex_quota    exception;
    es_quota    constant varchar2( 256 ) := 'Insufficient quota granted for move operation';
    ec_quota    constant number := -20706;
    pragma      exception_init( ex_quota, -20706 );

end dbms_hive;
/

show errors

--
-- ... done!
--
