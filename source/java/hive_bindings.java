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

