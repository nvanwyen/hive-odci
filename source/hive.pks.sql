--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pks.sql
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
prompt ... running hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package remote as

    -- get session level paraemter
    function session_param( name  in varchar2 ) return varchar2;

    -- set session level paraemter
    procedure session_param( name  in varchar2,
                             value in varchar2 );

    -- set the session log level
    procedure session_log_level( typ in number );

    -- clear all session data
    procedure session_clear;

    -- (re)set connection paraemter
    procedure session( url in varchar2 );

    -- (re)set connection paraemters
    procedure session( usr in varchar2,
                       pwd in varchar2 );

    -- (re)set connection paraemters
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 );

    -- (re)set connection paraemters
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 );

    -- (re)set the connection object
    procedure session( con in connection );

    -- current session object
    function session return connection;

    -- execute a remote query
    function query( stm in varchar2,
                    bnd in binds      default null,
                    con in connection default null ) return anydataset pipelined using hive_t;

    -- execute remote DML
    procedure dml( stm in varchar2,
                   bnd in binds      default null,
                   con in connection default null );

    -- execute remote DDL
    procedure ddl( stm in varchar2,
                   con in connection default null );

end remote;
/

show errors

--
-- ... done!
--
