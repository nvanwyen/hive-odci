--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - dbms_hive.pks.sql
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
    procedure purge_log( usr in varchar2 default null );
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
