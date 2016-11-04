--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pks.sql
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
