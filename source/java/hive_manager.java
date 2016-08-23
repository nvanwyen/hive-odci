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

import oracle.sql.*;
import oracle.jdbc.*;

// stored context manager (since the Oracle one seems broken)
@SuppressWarnings("deprecation")
public class hive_manager
{
    private static BigDecimal key_;
    private static HashMap<BigDecimal, hive_context> map_;

    //
    public hive_manager()
    {
        key_ = new BigDecimal( 0 );
        map_ = new HashMap<BigDecimal, hive_context>();
    }

    //
    private BigDecimal nextKey()
    {
        key_ = key_.add( new BigDecimal( 1 ) );
        return key_;
    }

    //
    public BigDecimal findContext( hive_context ctx )
    {
        BigDecimal key = new BigDecimal( 0 );

        for ( Map.Entry<BigDecimal, hive_context> ent : map_.entrySet() )
        {
            hive_context itm = ent.getValue();

            if ( itm.equals( ctx ) )
            {
                key = ent.getKey();
                break;
            }
        }

        return key;
    }

    //
    public BigDecimal createContext( hive_context ctx )
    {
        BigDecimal key = findContext( ctx );

        if ( key.intValue() == 0 )
        {
            key = nextKey();
            map_.put( key, ctx );

            log.trace( "hive_manager new map size: " + map_.size() );
        }
        else
        {
            log.trace( "hive_manager found existing context" );
        }

        log.trace( "hive_manager createContext return: " + key );
        return key;
    }

    //
    public hive_context getContext( BigDecimal key )
        throws hive_exception
    {
        hive_context ctx = map_.get( key );

        if ( ctx == null )
            throw new hive_exception( "Invalid context key: " + key.intValue() );

        return ctx;
    }

    //
    public hive_context removeContext( BigDecimal key )
    {
        hive_context ctx = null;

        try
        {
            ctx = getContext( key );

            if ( ctx != null )
                map_.remove( key );
        }
        catch ( hive_exception ex )
        {
            // nothing to do ...
            log.warn( "hive_manager::removeContext hive_exception: " + ex.getMessage() );
        }

        return ctx;
    }
};


