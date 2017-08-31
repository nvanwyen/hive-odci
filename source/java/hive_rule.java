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
public class hive_rule
{
    //
    public static final String[] SUPPORTED = { "typecast" };

    //
    private static final int OPEN  = 0;
    private static final int CLOSE = 1;

    //
    private static final String[][] BLOCK = { { "/*+", "*/" },
                                              { "--+", "\n" } };


    // list of currently supported hints
    //
    private static final String[] PAREN = { "(", ")" };
    private static final String[] SEPAR = { ",", " ", ":" };

    //
    private String sql_;
    private HashMap<String, HashMap<String,hive_hint>> map_;

    //
    public hive_rule()
    {
        map_ = new HashMap<String, HashMap<String,hive_hint>>();
    }

    //
    public hive_rule( String sql )
    {
        map_ = new HashMap<String, HashMap<String,hive_hint>>();
        add( sql );
    }

    //
    public String sql()
    {
        return sql_;
    }

    //
    public HashMap<String, HashMap<String,hive_hint>> map()
    {
        return map_;
    }

    //
    public String add( String sql )
    {
        for ( String[] blk : BLOCK )
        {
            String hnt;

            if ( ( hnt = hint( sql, blk ) ).length() > 0 )
            {
                for ( String sup : SUPPORTED )
                {
                    HashMap<String,hive_hint> rul;

                    if ( ( rul = process( hnt, sup ) ) != null )
                        map_.put( sup, rul );
                }

                sql = prune( sql, blk );
                break;
            }
        }

        sql_ = sql;
        return sql;
    }

    //
    public int size()
    {
        int sz = 0;

        if ( map_ == null )
            sz = -1;
        else
            sz = map_.size();

        return sz;
    }

    //
    public int size( String key )
    {
        int sz = 0;
        HashMap<String,hive_hint> val = value( key );

        if ( val == null )
            sz = -1;
        else
            sz = val.size();

        return sz;
    }

    //
    public HashMap<String,hive_hint> value( String key )
    {
        HashMap<String,hive_hint> val = null;

        if ( ( map_ != null ) && ( map_.size() > 0 ) )
            val = map_.get( key );

        return val;
    }

    //
    public ArrayList<hive_hint> list( String key )
    {
        ArrayList<hive_hint> val = null;

        if ( ( map_ != null ) && ( map_.size() > 0 ) )
        {
            HashMap<String,hive_hint> map = value( key );

            if ( ( map != null ) && ( map.size() > 0 ) )
                val = new ArrayList<hive_hint>( map.values() );
        }

        return val;
    }

    //
    public hive_hint item( String key, String name )
    {
        hive_hint val = null;
        HashMap<String,hive_hint> map = value( key );

        if ( ( map != null ) && ( map.size() > 0 ) )
            val = map.get( name );

        return val;
    }

    //
    public void clear()
    {
        // do not reuse the hashmap, so the following is
        // faster and more effcient than map_.clear() && map_.resize()
        map_ = null;
        map_ = new HashMap<String, HashMap<String,hive_hint>>();
    }

    //
    public String toString()
    {
        String str = "";

        if ( map_ != null )
        {
            Iterator itr = map_.entrySet().iterator();

            while ( itr.hasNext() )
            {
                Map.Entry ent = (Map.Entry)itr.next();
                HashMap<String,hive_hint> map = (HashMap<String,hive_hint>)ent.getValue();

                str += ent.getKey() + "\n";

                if ( ( map != null ) && ( map.size() > 0 ) )
                {
                    ArrayList<hive_hint> val = new ArrayList<hive_hint>( map.values() );

                    for ( hive_hint v : val )
                        str += v.toString() + "\n";
                }

                str += "\n";
            }
        }

        return str;
    }

    //
    private String hint( String sql, String[] blk )
    {
        String val = "";
        int bgn = 0;
        int end = 0;

        //
        if ( ( sql != null ) 
        && ( ( bgn = sql.indexOf( blk[ OPEN ] ) ) > 3 ) // must be past DML/DDL "first" operator
        && ( ( end = sql.indexOf( blk[ CLOSE ] ) ) > 3 ) )
        {
            val = sql.substring( bgn + blk[ OPEN ].length(), end - 1 );
        }

        return val;
    }

    //
    private String prune( String sql, String[] blk )
    {
        String val = "";
        int bgn = 0;
        int end = 0;

        //
        if ( ( sql != null ) 
        && ( ( bgn = sql.indexOf( blk[ OPEN ] ) ) > 3 ) // must be past DML/DDL "first" operator
        && ( ( end = sql.indexOf( blk[ CLOSE ] ) ) > 3 ) )
        {
            val = sql.substring( bgn, end + blk[ CLOSE ].length() );
            sql = sql.replace( val, "" );
        }
        else
            sql = val;

        return sql;
    }

    //
    private HashMap<String,hive_hint> process( String hnt, String sup )
    {
        int pos = 0;
        HashMap<String,hive_hint> itm = null;

        if ( ( pos = hnt.indexOf( sup ) ) > -1 )
        {
            if ( ( itm = new HashMap<String,hive_hint>() ) != null )
            {
                // ensure that the number of open parens equals the number of closing parens
                if ( countChar( hnt, '(' ) == countChar( hnt, ')' ) )
                {
                    int opn = 0;

                    if ( ( opn = hnt.indexOf( PAREN[ OPEN ], pos + sup.length() ) ) > -1 )
                    {
                        int cls = 0;

                        if ( ( cls = hnt.lastIndexOf( PAREN[ CLOSE ] ) ) > -1 )
                        {
                            // fully qualifed hint "/*+ ... *\" or "--+ ... \n" ...
                            String val = hnt.substring( opn + 1, cls ); /// do not replace "," with " "

                            if ( val.length() > 0 )
                            {
                                //
                                ArrayList<String> ary = null;

                                //
                                if ( ( ary = new ArrayList<String>() ) != null )
                                {
                                    // tokenize on " " ... logic based on "mti/vpd/src/vpd.pol.sql:hints_()"
                                    String[] tok = val.split( SEPAR[ 1 ] );
                                    String str = "";
                                    int idx = 0;

                                    while ( idx < tok.length )
                                    {
                                        // opening "(" ...
                                        if ( tok[ idx ].contains( PAREN[ 0 ] ) )
                                        {
                                            for ( int j = idx; j < tok.length; ++j )
                                            {
                                                str += tok[ j ];

                                                // closing ")" ...
                                                if ( tok[ idx ].contains( PAREN[ 1 ] ) )
                                                    break;

                                                ++idx;
                                            }
                                        }
                                        else
                                            str += tok[ idx ].trim();

                                        if ( str.length() > 0 )
                                        {
                                            str = str.trim();

                                            if ( str.charAt( str.length() - 1 ) == ',' )
                                                str = str.substring( 0, str.length() - 1 );

                                            // is this token a continuation of the last one added?
                                            if ( str.charAt( 0 ) == '(' )
                                            {
                                                if ( ary.size() > 0 )
                                                {
                                                    String tmp = ary.get( ary.size() - 1 );

                                                    tmp += str;
                                                    ary.set( ary.size() - 1, tmp );
                                                }
                                            }
                                            else
                                                ary.add( str );

                                            str = "";
                                        }

                                        ++idx;
                                    }

                                    if ( ary.size() > 0 )
                                    {
                                        // process each individual hint in the array
                                        for ( String dat : ary )
                                        {
                                            hive_hint rul = pair( dat );

                                            if ( rul != null )
                                            {
                                                if ( rul.data.length() > 0 )
                                                {
                                                    tuple<String,String> prt = part( rul.data );

                                                    if ( prt != null )
                                                    {
                                                        rul.type = prt.x;

                                                        if ( ( prt.y != null ) && ( prt.y.length() > 0 ) )
                                                        {
                                                            tuple<String,String> par = vals( rul.data );

                                                            if ( par != null )
                                                            {
                                                                if ( ( par.x != null ) && ( par.x.length() > 0 ) )
                                                                    rul.size = par.x;

                                                                if ( ( par.y != null ) && ( par.y.length() > 0 ) )
                                                                    rul.opts = par.y;
                                                            }
                                                        }
                                                    }
                                                }

                                                if ( rul.name.length() > 0 )
                                                {
                                                    if ( rul.type.length() > 0 )
                                                        rul.set_types();

                                                    itm.put( rul.name, rul );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return itm;
    }

    // paring splits "left" + ":" + "right" (colon seperated string)
    // into a hive_hint object where hive_hint.name = "left" and 
    // hive_hint.data = "right"
    private hive_hint pair( String str )
    {
        hive_hint rul = null;

        if ( ( str != null ) && ( str.length() > 0 ) )
        {
            String[] tok = str.split( SEPAR[ 2 ] );

            if ( tok.length > 0 )
            {
                if ( ( rul = new hive_hint() ) != null )
                {
                    rul.name = tok[ 0 ];

                    if ( tok.length > 1 )
                        rul.data = tok[ 1 ];
                }
            } 
        }

        return rul;
    }

    // part splits base value from paraemters "type( A, B )", returning "type" 
    // and "( A, B )" as a tuple ...
    private tuple<String,String> part( String par )
    {
        tuple<String,String> tup = null;

        if ( ( par != null ) && ( par.length() > 0 ) )
        {
            if ( ( tup = new tuple<String,String>() ) != null )
            {
                if ( ( par.contains( PAREN[ 0 ] ) )
                  && ( countChar( par, '(' ) == countChar( par, ')' ) ) )
                {
                    tup.x = par.substring( 0, par.indexOf( PAREN[ 0 ] ) );
                    tup.y = par.substring( par.indexOf( PAREN[ 0 ] ) );
                }
                else
                    tup.x = par; // tuple.y is empty/unavailable
            }
        }

        return tup;
    }

    // param splits values of "type( A, B )" or "( A, B )", returning A and B
    // as a tuple ...
    private tuple<String,String> vals( String par )
    {
        tuple<String,String> tup = null;

        if ( ( par != null ) && ( par.length() > 0 ) )
        {
            if ( ( tup = new tuple<String,String>() ) != null )
            {
                // tuple is populated only when parens are present
                if ( ( par.contains( PAREN[ 0 ] ) )
                  && ( countChar( par, '(' ) == countChar( par, ')' ) ) )
                {
                    String val = par.substring( par.indexOf( PAREN[ OPEN ] ) + 1, par.lastIndexOf( PAREN[ CLOSE ] ) );

                    if ( val.length() > 0 )
                    {
                        String[] tok = val.replace( ",", " " ).split( SEPAR[ 1 ] );

                        if ( tok.length > 0 )
                        {
                            tup.x = tok[ 0 ].trim();

                            if ( tok.length > 1 )
                                tup.y = tok[ 1 ].trim();
                        }
                    }
                }
            }
        }

        return tup;
    }

    // helper util
    private int countChar( String str, char chr )
    {
        int cnt = 0;

        for ( int i = 0, l = str.length(); i < l; ++i )
        {
            if ( str.charAt( i ) == chr )
                ++cnt;
        }        

        return cnt;
    }
};

