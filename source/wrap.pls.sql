--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - wrap.pls.sql
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
-- this script wraps the package bodies for the SHIELDSYS functionality
-- to protect intellectual property rights of MTI
--

--
prompt ... running wrap.pls.sql

--
alter session set current_schema = hive;

--
set serveroutput on
--
declare

    --
    function is_wrapped_( o in varchar2,
                          n in varchar2,
                          t in varchar2 ) return boolean is

        c number := 0;

    begin

        --
        select count(0) into c
          from dba_source 
         where owner = o
           and name = n
           and type = t
           and line = 1
           and lower( text ) like '% wrapped%';

        return ( c > 0 );

    end is_wrapped_;

    --
    function text_( o in varchar2, 
                    n in varchar2, 
                    t in varchar2 ) return varchar2 is

        txt varchar2( 32767 );

    begin

        --
        for rec in ( select text 
                       from dba_source 
                      where owner = o
                        and name = n
                        and type = t ) loop

            --
            txt := txt || rec.text;

        end loop;

        --
        return txt;

    end text_;

    --
    function spec_( o in varchar2, 
                    n in varchar2 ) return varchar2 is

        t varchar2( 32767 );

    begin

        --
        t := replace( 'create or replace ' || text_( o, n, 'PACKAGE' ), 
                      'create or replace package ' || lower( n ), 
                      'create or replace package ' || lower( o )|| '.' || lower( n ) );

        --
        return t;

    end spec_;

    --
    function body_( o in varchar2, 
                    n in varchar2 ) return varchar2 is

        t varchar2( 32767 );

    begin

        t := replace( 'create or replace ' || text_( o, n, 'PACKAGE BODY' ),  
                      'create or replace package body ' || lower( n ),  
                      'create or replace package body ' || lower( o ) || '.' || lower( n ) );

        --
        return t;

    end body_;

    --  
    function func_( o in varchar2, 
                    n in varchar2 ) return varchar2 is

        t varchar2( 32767 );

    begin

        t := replace( 'create or replace ' || text_( o, n, 'FUNCTION' ),  
                      'create or replace function ' || lower( n ),  
                      'create or replace function ' || lower( o ) || '.' || lower( n ) );

        --
        return t;

    end func_;

    --  
    function proc_( o in varchar2, 
                    n in varchar2 ) return varchar2 is

        t varchar2( 32767 );

    begin

        t := replace( 'create or replace ' || text_( o, n, 'PROCEDURE' ),  
                      'create or replace procedure ' || lower( n ),  
                      'create or replace procedure ' || lower( o ) || '.' || lower( n ) );

        --
        return t;

    end proc_;

    --
    function type_( o in varchar2, 
                    n in varchar2 ) return varchar2 is

        t varchar2( 32767 );

    begin

        t := replace( 'create or replace ' || text_( o, n, 'TYPE' ),  
                      'create or replace type ' || lower( n ),  
                      'create or replace type ' || lower( o ) || '.' || lower( n ) );

        --
        return t;

    end type_;

    --  
    function type_body_( o in varchar2,
                         n in varchar2 ) return varchar2 is
    
        t varchar2( 32767 );
    
    begin
        
        t := replace( 'create or replace ' || text_( o, n, 'TYPE BODY' ),
                      'create or replace type body ' || lower( n ),
                      'create or replace type body ' || lower( o ) || '.' || lower( n ) ); 

        --
        return t;
    
    end type_body_;

    --
    procedure wrap_( t in varchar2 ) is
    begin

        if ( t not like '% wrapped%' ) then

            --
            dbms_ddl.create_wrapped( t );

        end if;

    end wrap_;

begin

    -- package specifications
    begin

        --
        wrap_( spec_( 'HIVE', 'IMPL' ) );
        dbms_output.put_line( 'HIVE.IMPL Spec wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping body [HIVE.IMPL]: ' || sqlerrm );

    end;

    -- package bodies
    begin

        --
        wrap_( body_( 'HIVE', 'IMPL' ) );
        dbms_output.put_line( 'HIVE.IMPL Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping body [HIVE.IMPL]: ' || sqlerrm );

    end;

    begin

        --
        wrap_( body_( 'HIVE', 'REMOTE' ) );
        dbms_output.put_line( 'HIVE.REMOTE Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping body [HIVE.REMOTE]: ' || sqlerrm );

    end;

    -- 
    begin

        --
        wrap_( body_( 'HIVE', 'DBMS_HIVE' ) );
        dbms_output.put_line( 'HIVE.DBMS_HIVE Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping body [HIVE.DBMS_HIVE]: ' || sqlerrm );

    end;

    --
    begin

        --
        wrap_( body_( 'HIVE', 'BINDING' ) );
        dbms_output.put_line( 'HIVE.BINDING Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping body [HIVE.BINDING]: ' || sqlerrm );

    end;

    -- procedures
    -- none

    -- functions
    begin

        wrap_( func_( 'HIVE', 'BITOR' ) );
        dbms_output.put_line( 'HIVE.BITOR Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.BITOR]: ' || sqlerrm );

    end;

    --
    begin

        wrap_( func_( 'HIVE', 'BITXOR' ) );
        dbms_output.put_line( 'HIVE.BITXOR Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.BITXOR]: ' || sqlerrm );

    end;

    --
    begin

        wrap_( func_( 'HIVE', 'BITNOT' ) );
        dbms_output.put_line( 'HIVE.BITNOT Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.BITNOT]: ' || sqlerrm );

    end;

    --
    begin

        wrap_( func_( 'HIVE', 'OID' ) );
        dbms_output.put_line( 'HIVE.OID Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.OID]: ' || sqlerrm );

    end;

    --
    begin

        wrap_( func_( 'HIVE', 'ONAME' ) );
        dbms_output.put_line( 'HIVE.ONAME Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.ONAME]: ' || sqlerrm );

    end;

    --
    begin

        wrap_( func_( 'HIVE', 'HIVE_GENERATE_HINT' ) );
        dbms_output.put_line( 'HIVE.HIVE_GENERATE_HINT Function wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping function [HIVE.HIVE_GENERATE_HINT]: ' || sqlerrm );

    end;

    -- type specifications
    -- none

    -- type bodies
    begin

        --
        wrap_( type_body_( 'HIVE', 'BIND' ) );
        dbms_output.put_line( 'HIVE.BIND Type Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping type body [HIVE.BIND]: ' || sqlerrm );

    end;

    begin

        --
        wrap_( type_body_( 'HIVE', 'HIVE_T' ) );
        dbms_output.put_line( 'HIVE.HIVE_T Type Body wrapped' );

        --
        exception
            when others then
                dbms_output.put_line( 'Error wrapping type body [HIVE.HIVE_T]: ' || sqlerrm );

    end;

end;
/

--
-- ... done!
--
