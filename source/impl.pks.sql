--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pks.sql
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
