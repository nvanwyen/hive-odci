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
        }
    }

    //
    public String toString()
    {
        String str = "";

        str +=                                                               "\n";
        str += "... url:  " + ( ( url.length()  > 0 ) ? url  : "{empty}" ) + "\n";
        str += "... name: " + ( ( name.length() > 0 ) ? name : "{empty}" ) + "\n";
        str += "... pass: " + ( ( pass.length() > 0 ) ? pass : "{empty}" ) + "\n";
        str += "... auth: " + ( ( auth.length() > 0 ) ? auth : "{empty}" ) + "\n";

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

        return eq;
    }
};


