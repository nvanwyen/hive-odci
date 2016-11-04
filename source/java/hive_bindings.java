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
@SuppressWarnings("deprecation")
public class hive_bindings
{
    public ArrayList<hive_bind> binds;

    //
    public hive_bindings()
    {
        binds = new ArrayList<hive_bind>();
    }

    //
    public hive_bindings( ArrayList<hive_bind> b )
    {
        binds = b;
    }

    //
    public hive_bindings( oracle.sql.ARRAY obj )
        throws SQLException
    {
        binds = new ArrayList<hive_bind>();

        if ( obj != null )
        { 
            Datum[] dat = obj.getOracleArray();

            for ( int i = 0; i < dat.length; ++i )
            {
                if ( dat[ i ] != null )
                    binds.add( new hive_bind( (STRUCT)dat[ i ] ) );
            }
        }
    }

    //
    public long size()
    {
        return binds.size();
    }

    //
    public String toString()
    {
        String str = "";

        for ( hive_bind bnd : binds )
            str += bnd.toString();

        return str;
    }

    //
    public boolean equals( hive_bindings val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( val.binds != null )
            {
                if ( binds.size() == val.binds.size() )
                {
                    eq = true;

                    for ( int i = 0; i < binds.size(); i++ )
                    {
                        if ( ! binds.get( i ).equals( val.binds.get( i ) ) )
                        {
                            eq = false;
                            break;
                        }
                    }
                }
            }
        }

        return eq;
    }
};

