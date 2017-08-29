--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.fnc.sql
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
prompt ... running hive.fnc.sql

--
alter session set current_schema = hive;

--
create or replace function hive_q( stm in varchar2,
                                   bnd in binds      default null,
                                   con in connection default null ) return anydataset pipelined using hive_t;
/

-- helper function ...
create or replace function hive_hint( own in varchar2,
                                      tab in varchar2,
                                      typ in varchar2 default 'typecast' ) return varchar2 is

    hnt varchar2( 4000 ) := null;

begin

    --
    for rec in ( select column_id,
                        column_name,
                        data_type,
                        data_length,
                        data_precision,
                        data_scale
                   from dba_tab_columns
                 where owner = own
                   and table_name = tab 
                 order by column_id ) loop

        declare

            col varchar2( 4000 );

        begin

            --
            if ( rec.data_type = 'NUMBER' ) then

                if ( ( rec.data_precision is not null )
                 and ( rec.data_scale is not null ) ) then

                    col := rec.column_name || ':' || 
                           rec.data_type   || '(' || to_char( rec.data_precision ) || ',' || rec.data_scale || ')';
                else

                    col := rec.column_name || ':' || rec.data_type;

                end if;

            elsif ( ( rec.data_type = 'VARCHAR2' ) 
                 or ( rec.data_type = 'VARCHAR' ) 
                 or ( rec.data_type = 'CHAR' ) 
                 or ( rec.data_type = 'NVARCHAR' ) 
                 or ( rec.data_type = 'NCHAR' ) 
                 or ( rec.data_type = 'RAW' ) ) then

                col := rec.column_name || ':' || 
                       rec.data_type   || '(' || to_char( rec.data_length ) || ')';

            elsif ( rec.data_type = 'FLOAT' )  then

                col := rec.column_name || ':' || 
                       rec.data_type   || '(' || to_char( rec.data_precision ) || ')';

            elsif ( rec.data_type = 'LONG RAW' ) then

                col := rec.column_name || ':' || 'LONGVARBINARY';

            elsif ( rec.data_type like 'INTERVAL%' ) then

                if ( rec.data_type like 'INTERVAL%YEAR%' ) then

                    col := rec.column_name || ':' || 'INTERVAL_YM' ||
                           '(' || rec.data_precision || ')';

                else

                    col := rec.column_name || ':' || 'INTERVAL_DS' ||
                           '(' || rec.data_precision || ',' || rec.data_scale || ')';

                end if;

            elsif ( rec.data_type like 'TIMESTAMP%' ) then

                if ( rec.data_type like 'TIMESTAMP%LOCAL%' ) then

                    col := rec.column_name || ':' || 'TIMESTAMPLTZ';

                elsif ( rec.data_type like 'TIMESTAMP%ZONE%' ) then

                    col := rec.column_name || ':' || 'TIMESTAMPTZ';

                else

                    col := rec.column_name || ':' || 'TIMESTAMP';

                end if;

                col := col || '(' || rec.data_scale || ')';

            else

                col := rec.column_name || ':' || rec.data_type;

            end if;

            --
            if ( col is not null ) then

                --
                if ( hnt is null ) then

                    hnt := col;

                else

                    hnt := hnt || ' ' || col;

                end if;

            end if;

        end;

    end loop;

    --
    if ( hnt is not null ) then

        --
        hnt := '/*+ ' || hnt || ' */';

    end if;

    --
    return lower( hnt );

    --
    exception when others then return null;

end hive_hint;
/

show errors

--
-- ... done!
--
