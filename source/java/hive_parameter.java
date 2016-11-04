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


package oracle.mti.odci;

//
import java.io.*;
import java.net.*;
import java.sql.*;
import java.lang.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive_parameter
{
    //
    static public String value( String name )
    {
        //
        String val = null;

        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String sql = "select sys_context( 'hivectx', substr( ?, 1, 30 ), 4000 ) value " +
                           "from dual";

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, name );

            //
            ResultSet rst = stm.executeQuery();

            if ( rst.next() )
                val = rst.getString( "value" );

            //
            rst.close();
            stm.close();

            if ( val == null )
            {
                sql = "select value " +
                        "from param$ " +
                       "where name = ?";

                //
                stm = con.prepareStatement( sql );
                stm.setString( 1, name );

                //
                rst = stm.executeQuery();

                if ( rst.next() )
                    val = rst.getString( "value" );

                //
                rst.close();
                stm.close();
            }
        }
        catch ( SQLException ex )
        {
            //
            log.error( "hive_parameter::value SQLException: " + ex.getMessage() );
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_parameter::value Exception: " + ex.getMessage() );
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();
            }
            catch ( SQLException ex ) 
            {
                log.error( "hive_parameter::value (finally_block) SQLException: " + ex.getMessage() );
            }
            catch ( Exception ex )
            {
                log.error( "hive_parameter::value (finally_block) Exception: " + ex.getMessage() );
            }

            // *** do not close the "default" connection ***
        }

        //
        return val;
    }

    //
    static public String env( String name )
    {
        //
        String val = null;

        //
        Connection con = null;
        OracleCallableStatement stm = null;

        try
        {
            String sql = "begin sys.dbms_system.get_env( ?, ? ); end;";

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            stm = (OracleCallableStatement)con.prepareCall( sql );
            stm.setString( 1, name );
            stm.registerOutParameter( 2, OracleTypes.VARCHAR );

            //
            stm.execute();
            val = stm.getString( 2 );
        }
        catch ( SQLException ex )
        {
            //
            log.error( "hive_parameter::env SQLException: " + ex.getMessage() );
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_parameter::env Exception: " + ex.getMessage() );
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();
            }
            catch ( SQLException ex ) 
            {
                log.error( "hive_parameter::env (finally_block) SQLException: " + ex.getMessage() );
            }
            catch ( Exception ex )
            {
                log.error( "hive_parameter::env (finally_block) Exception: " + ex.getMessage() );
            }

            // *** do not close the "default" connection ***
        }

        //
        return val;
    }
};


