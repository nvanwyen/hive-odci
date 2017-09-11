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
@SuppressWarnings("deprecation")
public class hive_attribute
{
    public String name;
    public int    code;
    public int    prec;
    public int    scale;
    public int    len;
    public int    csid;
    public int    csfrm;

    hive_attribute()
    {
        name  = "";
        code  = -1;
        prec  = -1;
        scale = -1;
        len   = -1;
        csid  = -1;
        csfrm = -1;
    }

    public String toString()
    {
        String str = new String();

        str +=                                                                 "\n";
        str += "... name:  " +  ( ( name.length() > 0 ) ? name : "{empty}" ) + "\n";
        str += "... code:  " +  code                                         + "\n";
        str += "... prec:  " +  prec                                         + "\n";
        str += "... scale: " +  scale                                        + "\n";
        str += "... len:   " +  len                                          + "\n";
        str += "... csid:  " +  csid                                         + "\n";
        str += "... csfrm: " +  csfrm                                        + "\n";

        return str;
    }
};
