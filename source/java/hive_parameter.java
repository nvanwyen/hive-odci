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


