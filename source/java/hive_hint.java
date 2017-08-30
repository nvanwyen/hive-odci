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

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings( { "deprecation", "unchecked" } )
public class hive_hint
{
    private static final int DEFTYPE = -999;

    //
    public String name;
    public String data;

    public String type; // parsed "data" maps to "data type"
    public String size; //               ... "length" or "precision"
    public String opts; //               ... and scale

    public int code;    // hive_types.TYPECODE or hive_types.TYPEJDBC values
    public int jdbc;    // (pre)parsed here to save cycles ...

    //
    public hive_hint()
    {
        load( "", "", "", "", "", DEFTYPE, DEFTYPE );
    }

    //
    public hive_hint( String n )
    {
        load( n, "", "", "", "", DEFTYPE, DEFTYPE );
    }

    //
    public hive_hint( String n, String d )
    {
        load( n, d, "", "", "", DEFTYPE, DEFTYPE );
    }

    //
    public hive_hint( String n, String d, String t, String s, String o )
    {
        load( n, d, t, s, o, DEFTYPE, DEFTYPE );
    }

    //
    public hive_hint( String n, String d, String t, String s, String o, int c, int j )
    {
        load( n, d, t, s, o, c, j );
    }

    //
    public void load( String n, String d, String t, String s, String o, int c, int j )
    {
        name = n;
        data = d;
        type = t;
        size = s;
        opts = o;

        code = c;
        jdbc = j;
    }

    //
    public void set_types()
    {
        if ( type.length() > 0 )
        {
            code =  hive_types.to_typecode( type );
            jdbc =  hive_types.to_typejdbc( type );
        }
        else
        {
            code = DEFTYPE;
            jdbc = DEFTYPE;
        }
    }

    //
    public boolean equals( String n )
    {
        return name.equalsIgnoreCase( n );
    }

    //
    public String toString()
    {
        return                     "\n" +
               "+ name: " + name + "\n" +
               "+ data: " + data + "\n" +
               "+ type: " + type + "\n" +
               "+ size: " + size + "\n" +
               "+ opts: " + opts + "\n" +
               "+ code: " + code + "\n" +
               "+ jdbc: " + jdbc;
    }
};
