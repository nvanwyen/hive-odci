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
public class hive_session
{
    //
    public String url;

    // when auth = userIdPassword
    public String name;
    public String pass;

    // auth type
    public String auth;

    // when auth = kerberos
    // see system parameters: java.security.krb5.realm
    //                        java.security.krb5.kdc
    //                        java.security.krb5.conf
    //                        java.security.auth.login.config

    //
    public hive_session()
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "normal";

        url  = "";
        name = "";
        pass = "";

        log.trace( "hive_session default ctor" );
    }

    //
    public hive_session( String u )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "normal";

        url  = u;
        name = "";
        pass = "";

        log.trace( "hive_session ctor - url: " + u );
    }

    //
    public hive_session( String u, String n, String p )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "normal";

        url  = u;
        name = n;
        pass = p;

        log.trace( "hive_session ctor - url: " + u + " , name: " + n + ", pass: " + p );
    }

    //
    public hive_session( String u, String n, String p, String a )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "normal";

        url  = u;
        name = n;
        pass = p;

        if ( a != null )
        {
            if ( a.trim().length() > 0 )
                auth = a;
        }

        log.trace( "hive_session ctor - url: " + u + " , name: " + n + ", pass: " + p );
    }

    //
    public hive_session( oracle.sql.STRUCT obj )
        throws SQLException
    {
        if ( obj != null )
        {
            oracle.sql.Datum[] atr = obj.getOracleAttributes();

            auth = hive_parameter.value( "hive_auth" );

            if ( auth == null )
                auth = "normal";

            if ( atr.length > 0 )
            {
                if ( atr[ 0 ] != null )
                    url = atr[ 0 ].toString();
                else
                    url = "";
            }
            else
                url = "";

            if ( atr.length > 1 )
            {
                if ( atr[ 1 ] != null )
                    name = atr[ 1 ].toString();
                else
                    name = "";
            }
            else
                name = "";

            if ( atr.length > 2 )
            {
                if ( atr[ 2 ] != null )
                    pass = atr[ 2 ].toString();
                else
                    pass = "";
            }
            else
                pass = "";

            if ( atr.length > 3 )
            {
                if ( atr[ 3 ] != null )
                    auth = atr[ 3 ].toString();
                else
                    auth = "normal";
            }

            log.trace( "hive_session ctor - oracle.sql.STRUCT: " + obj.toString() );
        }
        else
            log.info( "hive_session ctor - oracle.sql.STRUCT: NULL" );
    }

    //
    public String toString()
    {
        String str = "";

        str += "url:  " + url + "\n";
        str += "name: " + name + "\n";
        str += "pass: " + pass + "\n";
        str += "auth: " + auth + "\n";

        log.trace( "hive_session toString: " + str );
        return str;
    }

    //
    public boolean equals( hive_session val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( url.equals( val.url )
              && name.equals( val.name )
              && pass.equals( val.pass )
              && auth.equals( val.auth ) )
                eq = true;
        }

        log.trace( "hive_session equals: " + ( ( eq ) ? "TRUE" : "FALSE" ) );
        return eq;
    }
};


