--------------------------------------------------------------------------------
--
-- 2017-03-24, NV - patch_validate.sql
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

prompt ... running patch_validate.sql

set linesize 160
set pagesize 50000
set trimspool on

set serveroutput on
begin

    for rec in ( select a.object_id   id,
                        a.object_name name
                   from dba_objects a
                  where a.status = 'INVALID' ) loop

        begin

            dbms_utility.validate( rec.id );
            exception when others then dbms_output.put_line( 'Failed: ' || rec.name || ': ' || sqlerrm );

        end;

        dbms_output.put_line( 'Validated: ' || rec.name );

    end loop;

end;
/

--
-- ...done!
--
