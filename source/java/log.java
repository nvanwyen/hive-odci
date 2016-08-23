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

    //
    public static void write( int type, String text ) throws SQLException, Exception
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
    public static void error( String text )
    {
        try { write( ERROR, text ); } catch ( Exception ex ) {}
    }

    //
    public static void warn( String text )
    {
        try { write( WARN, text ); } catch ( Exception ex ) {}
    }

    //
    public static void info( String text )
    {
        try { write( INFO, text ); } catch ( Exception ex ) {}
    }

    //
    public static void trace( String text )
    {
        try { write( TRACE, text ); } catch ( Exception ex ) {}
    }
};
