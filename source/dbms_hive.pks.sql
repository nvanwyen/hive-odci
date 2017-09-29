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
    procedure remove( name in varchar2 );                   -- remove (unset)

    --
    procedure purge_log( usr in varchar2 default null );
    procedure purge_filter( key in varchar2 default null );

    --
    procedure move_ts( ts in varchar2, obj in varchar2 default null );

    -- grant/revoke follows:
    -- GRANT <operation, operation, ...> ON <table> TO <grantee, grantee, ...>
    --
    -- where "operation" follows the Hive Language guidelines, such as
    -- SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, ...
    -- and "grantee" are local Oracle database users or roles
    --
    -- grant access to hive table
    procedure grant_access( opr in varchar2,   -- operation list
                            tab in varchar2,   -- hive table (case sensitive)
                            gnt in varchar2 ); -- grantee list

    -- revoke access from hive table
    procedure revoke_access( opr in varchar2,
                             tab in varchar2,
                             gnt in varchar2 );
    --
    -- If there are no grants assigned to the Hive table, then access
    -- is considered ALLOW ANY for all accounts. Only when there is at least
    -- one operation granted to iat least one grantee is the Hive table
    -- considered protected, and check for ALLOW/DENY of operations
    --

    --
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

--
show errors

--
-- ... done!
--
