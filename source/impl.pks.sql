--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pks.sql
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
prompt ... running impl.pks.sql

--
alter session set current_schema = hive;

--
create or replace package impl as

    --
    none  constant number := 0;
    error constant number := 1;
    warn  constant number := 2;
    info  constant number := 4;
    trace constant number := 8;

    --
    procedure log( typ in number, txt in varchar2 );
    --
    procedure log_error( txt in varchar2 );
    procedure log_warn( txt in varchar2 );
    procedure log_info( txt in varchar2 );
    procedure log_trace( txt in varchar2 );

    --
    function session_param( name in varchar2 ) return varchar2;

    --
    procedure session_param( name  in varchar2,
                             value in varchar2 );

    --
    procedure session_log_level( typ in number );

    --
    procedure session_clear;

    -- 
    procedure session( url in varchar2 );

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 );

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 );

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 );

    --
    procedure session( con in connection );

    --
    function session return connection;

    --
    function sql_describe( stm in varchar2,
                           bnd in binds      default null,
                           con in connection default null ) return anytype;

    --
    function sql_describe( typ out anytype,
                           stm in  varchar2,
                           bnd in  binds      default null,
                           con in  connection default null ) return number;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number;

    --
    function sql_open( key out number,
                       stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) return number;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number;

    --
    function sql_close( key in number ) return number;

    --
    procedure sql_dml( stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null );

    --
    procedure sql_ddl( stm in  varchar2,
                       con in  connection default null );

    --
    ex_not_eligible exception;
    es_not_eligible constant varchar2( 256 ) := 'Parameter is not eligible for change at the session level';
    ec_not_eligible constant number          := -20103;

end impl;
/

show errors

--
-- ... done!
--
