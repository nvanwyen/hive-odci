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
            log.error( "hive_connection::loadDriver error: " + ex.getMessage() );
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
            //log.trace( "hive_connection - Loaded hive_jdbc_driver: " + driver_ );

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
                        //log.trace( "hive_connection - Set URL paraemter [" + "hive_jdbc_url." + Integer.toString( idx ) + "]: " + val );
                        session.url += ";" + val;
                    }
                    else
                    {
                        //log.trace( "hive_connection - Ignored NULL URL paraemter [" + "hive_jdbc_url." + Integer.toString( idx ) + "]" );
                    }
                }
                else
                    break;
            }
        }

        log.info( "hive_connection::getUrl: " + session.url );
        return session.url;
    }

    //
    public Connection getConnection()
    {
        return conn_;
    }

    //
    public String getProperty( String name )
    {
        String val = "";
        int idx = 0;

        while ( true )
        {
            String n = hive_properties.name( "java_property." + Integer.toString( ++idx ) );

            if ( n != null )
            {
                if ( n.equals( name ) )
                {
                    val = hive_properties.value( "java_property." + Integer.toString( idx ) );

                    //log.trace( "hive_connection - Found property [" + name + "] at index: " + Integer.toString( idx ) );
                    break;
                }
            }
            else
                break;
        }

        //log.trace( "hive_connection - Get property [" + name + "]: " + val );
        return val;
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

                //log.trace( "hive_connection - Set system property [" + "java_property." + Integer.toString( idx ) + "]" +
                //           ", name: "  + n +
                //           ", value: " + v );

                System.setProperty( n, v );
            }
            else
                break;
        }

        //log.trace( "hive_connection - Set " + Integer.toString( idx ) + " system properties" );
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
                    //log.trace( "hive_connection::createConnection mode: " + session.auth );

                    // if no java properties are set, then kerberos cannot be used
                    if ( session.auth.equals( "kerberos" ) )
                        login();

                    //log.trace( "hive_connection::createConnection URL: " + url );
                }

                if ( ( session.name.trim().length() == 0 )
                  && ( session.pass.trim().length() == 0 ) )
                {
                    //log.trace( "hive_connection - DriverManager.getConnection( " + url + ")" );
                    conn_ = DriverManager.getConnection( url );
                }
                else
                {
                    //log.trace( "hive_connection - DriverManager.getConnection( " + url 
                    //                                           + ", " + session.name.trim() 
                    //                                           + ", " + session.pass.trim() 
                    //                                           + ")" );

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
            String idx = getProperty( "java.security.auth.login.index" );

            if ( idx.length() > 0 )
            {
                //log.trace( "hive_connection - Using LoginContext index: " + idx );

                Subject sub = new Subject();
                LoginContext lc = new LoginContext( idx, sub, new callback_handler() );

                lc.login();
                ok = true;

                //log.trace( "hive_connection - kerberos login successful" );
            }
            else
            {
                ok = false;
                log.warn( "hive_connection - Property \"java.security.auth.login.index\" not specified in parameter list" );
                throw new hive_exception( "Property \"java.security.auth.login.index\" not specified in parameter list" );
            }
        }
        catch ( LoginException ex )
        {
            ok = false;
            log.error( "hive_connection - kerberos login failed: " + ex.getMessage() );
            throw new hive_exception( "Kerberos exception: " + ex.getMessage() );
        }

        //log.trace( "hive_connection::connection login() returns: " + ( ( ok ) ? "true" : "false" ) );
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

