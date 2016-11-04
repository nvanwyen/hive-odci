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

// properties parsing class
@SuppressWarnings("deprecation")
public class hive_properties
{
    //
    static public String property( String prop )
    {
        return hive_parameter.value( prop );
    }

    //
    static public String name( String prop )
    {
        String val = null;
        String dat = property( prop );

        if ( dat != null )
        {
            String[] ary = dat.split( "=" );

            if ( ary.length > 0 )
                val = ary[ 0 ];
        }

        return val;
    }

    //
    static public String value( String prop )
    {
        String val = null;
        String dat = property( prop );

        if ( dat != null )
        {
            String[] ary = dat.split( "=" );

            if ( ary.length > 1 )
            {
                if ( ary.length > 2 )
                    val = dat.substring( dat.indexOf( "=" ) + 1 );
                else
                    val = ary[ 1 ];
            }
        }

        return val;
    }
};


