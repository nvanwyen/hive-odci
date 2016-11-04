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
public class log
{
    //
    public static final int NONE            =  0;   // logging level
    public static final int ERROR           =  1;
    public static final int WARN            =  2;
    public static final int INFO            =  4;
    public static final int TRACE           =  8;

    // this is private, because the hive_paramter object
    // will not have been created yet, and there was a need early
    // on to get a paraemter value. This is no longer the case
    // so we can remove this function
    static private String param( String name )
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
            // ... do nothing!
        }
        catch ( Exception ex )
        {
            // ... do nothing!
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
                // ... do nothing!
            }
            catch ( Exception ex )
            {
                // ... do nothing!
            }

            // *** do not close the "default" connection ***
        }

        //
        return val;
    }

    //
    static public void write( int type, String text ) throws SQLException, Exception
    {
        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            if ( con.getAutoCommit() )
                con.setAutoCommit( false );

            //
            String sql = "begin impl.log( ?, ? ); end;";

            //
            stm = con.prepareStatement( sql );
            stm.setInt( 1, type );
            stm.setString( 2, text );

            //
            stm.executeUpdate();
            stm.close();
        }
        catch ( SQLException ex )
        {
            // ... do nothing!
        }
        catch ( Exception ex )
        {
            // ... do nothing!
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                if ( con != null )
                {
                    if ( ! con.getAutoCommit() )
                        con.setAutoCommit( true );
                }

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }
    }

    //
    static public void error( String text )
    {
        try { write( ERROR, text ); } catch ( Exception ex ) {}
    }

    //
    static public void warn( String text )
    {
        try { write( WARN, text ); } catch ( Exception ex ) {}
    }

    //
    static public void info( String text )
    {
        try { write( INFO, text ); } catch ( Exception ex ) {}
    }

    //
    static public void trace( String text )
    {
        try { write( TRACE, text ); } catch ( Exception ex ) {}
    }
};
