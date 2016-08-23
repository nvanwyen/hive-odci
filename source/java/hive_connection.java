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
import java.util.*;

import javax.security.auth.*;
import javax.security.auth.login.*;
import javax.security.auth.callback.*;
import javax.security.auth.kerberos.*;

import oracle.sql.*;
import oracle.jdbc.*;

// stored context records
@SuppressWarnings("deprecation")
public class hive_connection
{
    //
    private static String driver_;

    //
    public hive_session session;

    //
    Connection conn_;

    //
    public hive_connection()
    {
        session = new hive_session();
    }

    //
    public hive_connection( hive_session con )
    {
        session = con;
    }

    //
    public hive_connection( oracle.sql.STRUCT obj )
        throws SQLException
    {
        session = new hive_session( obj );
    }

    //
    public hive_connection( String url )
    {
        session = new hive_session( url );
    }

    //
    public hive_connection( String url, String name, String pass )
    {
        session = new hive_session( url, name, pass );
    }

    //
    public hive_connection( String url, String name, String pass, String auth )
    {
        session = new hive_session( url, name, pass, auth );
    }

    //
    static public void loadDriver() throws hive_exception
    {
        try
        {
            Class.forName( getDriverName() );
        }
        catch ( ClassNotFoundException ex )
        {
            log.error( "loadDriver error: " + ex.getMessage() );
            throw new hive_exception( "Driver class not found: " + getDriverName() );
        }
    }

    //
    static public String getDriverName()
        throws hive_exception
    {
        if ( driver_ == null )
        {
            driver_ = hive_parameter.value( "hive_jdbc_driver" );
            log.trace( "Loaded hive_jdbc_driver: " + driver_ );

            if ( driver_ == null )
                throw new hive_exception( "Could not find parameter value for JDBC driver" );
        }

        return driver_;
    }

    //
    public void setSession( hive_session val ) { session = val; }
    public hive_session getSession() { return session; }

    //
    public void setUrl( String val )  { session.url = val; }
    public void setUser( String val ) { session.name = val; }
    public void setPass( String val ) { session.pass = val; }
    public void setAuth( String val ) { session.auth = val; }

    //
    public String getUser() { return session.name; }
    public String getPass() { return session.pass; }
    public String getAuth() { return session.auth; }

    //
    public String getUrl() throws hive_exception
    {
        int idx = 0;

        if ( session.url.trim().length() == 0 )
        {
            session.url = hive_parameter.value( "hive_jdbc_url" );

            if ( session.url == null )
                throw new hive_exception( "Could not find parameter for Hive URL" );

            while ( true )
            {
                String val = hive_parameter.value( "hive_jdbc_url." + Integer.toString( ++idx ) );

                if ( val != null )
                {
                    if ( val.trim().length() > 0 )
                    {
                        log.trace( "Set URL paraemter [" + "hive_jdbc_url." + Integer.toString( idx ) + "]: " + val );
                        session.url += ";" + val;
                    }
                    else
                        log.trace( "Ignored NULL URL paraemter [" + "hive_jdbc_url." + Integer.toString( idx ) + "]" );
                }
                else
                    break;
            }
        }

        log.info( session.url );
        return session.url;
    }

    //
    public Connection getConnection()
    {
        return conn_;
    }

    //
    public boolean setProperties()
    {
        int idx = 0;

        while ( true )
        {
            String n = hive_properties.name( "java_property." + Integer.toString( ++idx ) );

            if ( n != null )
            {
                String v = hive_properties.value( "java_property." + Integer.toString( idx ) );

                log.trace( "Set system property [" + "java_property." + Integer.toString( idx ) + "]: " +
                           "name: "  + n +
                           "value: " + v );

                System.setProperty( n, v );
            }
            else
                break;
        }

        log.trace( "Set " + Integer.toString( idx ) + " system properties" );
        return ( idx > 1 ); // found at least 1 property to set
    }

    //
    public Connection createConnection() throws SQLException, hive_exception
    {
        if ( getConnection() == null )
        {
            String url = getUrl();

            if ( url.length() > 0 )
            {
                if ( setProperties() )
                {
                    // if no java properties are set, then kerberos cannot be used
                    if ( session.auth.equals( "kerberos" ) )
                        login();

                    log.trace( "createConnection URL: " + url );
                }

                if ( ( session.name.trim().length() == 0 )
                  && ( session.pass.trim().length() == 0 ) )
                {
                    log.trace( "DriverManager.getConnection( " + url + ")" );
                    conn_ = DriverManager.getConnection( url );
                }
                else
                {
                    log.trace( "DriverManager.getConnection( " + url + ", " + session.name.trim() + ", " + session.pass.trim() + ")" );
                    conn_ = DriverManager.getConnection( url, session.name.trim(), session.pass.trim() );
                }
            }
        }

        return conn_;
    }

    //
    public boolean login() throws SQLException, hive_exception
    {
        boolean ok = false;

        try
        {
            String idx = "";

            Subject sub = new Subject();
            LoginContext lc = new LoginContext( idx, sub, new callback_handler() );

            lc.login();
            ok = true;

            log.trace( "kerberos login successful" );
        }
        catch ( LoginException ex )
        {
            ok = false;
            log.error( "kerberos login failed: " + ex.getMessage() );
            throw new hive_exception( "Kerberos exception: " + ex.getMessage() );
        }

        return ok;
    }

    //
    public boolean equals( hive_connection val )
    {
        boolean eq = false;

        if ( val != null )
            eq = session.equals( val.session );

        return eq;
    }
};

