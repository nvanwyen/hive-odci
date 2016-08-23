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
import java.math.*;
import java.util.*;
import java.text.*;
// import java.security.*;
// import java.util.regex.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive_bind
{
    //
    public static final int UNKNOWN        =  0;
    //
    public static final int SCOPE_IN       =  1;
    public static final int SCOPE_OUT      =  2;
    public static final int SCOPE_INOUT    =  3;
    //
    public static final int TYPE_BOOL      =  1;
    public static final int TYPE_DATE      =  2;
    public static final int TYPE_FLOAT     =  3;
    public static final int TYPE_INT       =  4;
    public static final int TYPE_LONG      =  5;
    public static final int TYPE_NULL      =  6;
    public static final int TYPE_ROWID     =  7;
    public static final int TYPE_SHORT     =  8;
    public static final int TYPE_STRING    =  9;
    public static final int TYPE_TIME      = 10;
    public static final int TYPE_TIMESTAMP = 11;
    public static final int TYPE_URL       = 12;

    //
    public String value;
    public int   type;
    public int   scope;

    //
    public hive_bind()
    {
        value = "";
        type  = UNKNOWN;
        scope = UNKNOWN;
    }

    //
    public hive_bind( String v, int t, int s )
    {
        value = v;
        type  = t;
        scope = s;
    }

    //
    public hive_bind( oracle.sql.STRUCT obj )
        throws SQLException
    {
        if ( obj != null )
        {
            oracle.sql.Datum[] atr = obj.getOracleAttributes();

            if ( atr.length > 0 )
            {
                if ( atr[ 0 ] != null )
                    value = atr[ 0 ].toString();
                else
                    value = "";
            }
            else
                value = "";

            if ( atr.length > 1 )
            {
                if ( atr[ 1 ] != null )
                    type = atr[ 1 ].intValue();
                else
                    type = UNKNOWN;
            }
            else
                type = UNKNOWN;

            if ( atr.length > 2 )
            {
                if ( atr[ 2 ] != null )
                    scope = atr[ 2 ].intValue();
                else
                    scope = UNKNOWN;
            }
            else
                scope = UNKNOWN;
        }
    }

    //
    public String toString()
    {
        String str = "";

        str += "value: " + value + "\n";
        str += "type:  " + type  + "\n";
        str += "scope: " + scope + "\n";

        return str;
    }

    //
    public boolean equals( hive_bind val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( value.equals( val.value )
              && type == val.type
              && scope == val.scope )
                eq = true;
        }

        return eq;
    }

    //
    public boolean toBool()
    {
        boolean val = false;

        if ( value != null )
        {
            if ( value.equalsIgnoreCase( "Y" )
              || value.equalsIgnoreCase( "T" )
              || value.equalsIgnoreCase( "YES" )
              || value.equalsIgnoreCase( "TRUE" ) )
                val = true;
        }


        log.trace( "hive_bind::toBool: " + toString() );
        return val;
    }

    //
    public java.sql.Date toDate()
    {
        java.sql.Date val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "date_format" );

                if ( par == null )
                    par = new String( "YYYY-MM-DD" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = (java.sql.Date) fmt.parse( value );
            }
            catch ( Exception /*ParseException*/ ex ) 
            {
                log.warn( "hive_bind::toDate ParseException: " + ex.getMessage() );
            }
        }

        log.trace( "hive_bind::toDate: " + toString() );
        return val;
    }

    //
    public float toFloat()
    {
        float val = 0;

        if ( value != null )
        {
            val = Float.valueOf( value );
        }

        log.trace( "hive_bind::toFloat: " + toString() );
        return val;
    }

    //
    public int toInt()
    {
        int val = 0;

        if ( value != null )
        {
            val = Integer.valueOf( value );
        }

        log.trace( "hive_bind::toInt: " + toString() );
        return val;
    }

    //
    public long toLong()
    {
        long val = 0;

        if ( value != null )
        {
            val = Long.valueOf( value );
        }

        log.trace( "hive_bind::toLong: " + toString() );
        return val;
    }

    //
    public String toRowid()
    {
        log.trace( "hive_bind::toRowid: " + toString() );
        return toVarchar();
    }

    //
    public short toShort()
    {
        short val = 0;

        if ( value != null )
        {
            val = Short.valueOf( value );
        }

        log.trace( "hive_bind::toShort: " + toString() );
        return val;
    }

    //
    public String toVarchar()
    {
        log.trace( "hive_bind::tovarchar: " + toString() );
        return value;
    }

    //
    public Time toTime()
    {
        Time val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "time_format" );

                if ( par == null )
                    par = new String( "hh:mm a" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = new Time( ( (java.sql.Date) fmt.parse( value ) ).getTime() );
            }
            catch ( ParseException ex ) 
            {
                log.warn( "hive_bind::toTime ParseException: " + ex.getMessage() );
            }
        }

        log.trace( "hive_bind::toTime: " + toString() );
        return val;
    }

    //
    public Timestamp toTimestamp()
    {
        Timestamp val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "timestamp_format" );

                if ( par == null )
                    par = new String( "yyyy-MM-dd hh:mm:ss.SSS" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = new Timestamp( ( (java.sql.Date) fmt.parse( value ) ).getTime() );
            }
            catch ( ParseException ex ) 
            {
                log.warn( "hive_bind::toTimestamp ParseException: " + ex.getMessage() );
            }
        }

        log.trace( "hive_bind::toTimestamp: " + toString() );
        return val;
    }

    //
    public URL toUrl()
    {
        URL val = null;

        try
        {
            val = new URL( value );
        }
        catch ( MalformedURLException ex )
        {
            log.warn( "hive_bind::toUrl MalformedURLException: " + ex.getMessage() );
        }

        log.trace( "hive_bind::toUrl: " + toString() );
        return val;
    }
};

