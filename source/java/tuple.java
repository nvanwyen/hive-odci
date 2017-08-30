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
import java.lang.*;

//
@SuppressWarnings( { "deprecation", "unchecked" } )
//
public class tuple<X,Y>
{
    //
    public X x;
    public Y y;

    //
    public tuple() { /* ctor */ }

    //
    public tuple( X x, Y y )
    {
        this.x = x;
        this.y = y;
    }

    //
    public String toString()
    {
        String str = "";

        if ( x != null )
        {
            str += "x: " + x.toString();

            if ( y != null )
                str += ", y: " + y.toString();
        }

        return str;
    }
};
