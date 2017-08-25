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
    public static final String[] SUPPORTED = { "use_map" };

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
    private HashMap<String, ArrayList<String>> map_;

    //
    public hive_rule()
    {
        map_ = new HashMap<String, ArrayList<String>>();
    }

    //
    public hive_rule( String sql )
    {
        map_ = new HashMap<String, ArrayList<String>>();
        add( sql );
    }

    //
    public String sql()
    {
        return sql_;
    }

    //
    public HashMap<String, ArrayList<String>> map()
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
                    ArrayList<String> itm;

                    if ( ( itm = process( hnt, sup ) ) != null )
                        map_.put( sup, itm );
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
        if ( map_ == null )
            return -1;

        return map_.size();
    }

    //
    public int size( String key )
    {
        ArrayList<String> val = values( key );

        if ( val == null )
            return -1;

        return val.size();
    }

    //
    public ArrayList<String> values( String key )
    {
        return map_.get( key );
    }

    //
    public String value( String key, String val )
    {
        String ret = "";

        if ( ( map_ != null ) && ( map_.size() > 0 ) )
        {
            ArrayList<String> itm = values( key );

            if ( itm != null )
            {
                for ( String i : itm )
                {
                    hive_tuple<String, String> v = pair( i );

                    if ( v != null )
                    {
                        if ( ( v.x != null ) && ( v.x.length() > 0 ) )
                        {
                            if ( v.x.equalsIgnoreCase( val ) )
                            {
                                ret = i;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return ret;
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
    private ArrayList<String> process( String hnt, String sup )
    {
        int pos = 0;
        ArrayList<String> itm = null;

        if ( ( pos = hnt.indexOf( sup ) ) > -1 )
        {
            if ( ( itm = new ArrayList<String>() ) != null )
            {
                // ensure that the number of open parens equalscloseing parens
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
                                        itm = (ArrayList<String>)ary.clone();
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
    // into a tuple ...
    public hive_tuple<String, String> pair( String key, int index )
    {
        hive_tuple<String, String> tup = null;
        ArrayList<String> val = values( key );

        if ( ( val != null ) && ( index < val.size() ) )
        {
            String[] tok = val.get( index ).split( SEPAR[ 2 ] );

            if ( tok.length > 0 )
            {
                if ( ( tup = new hive_tuple<String, String>() ) != null )
                {
                    tup.x = tok[ 0 ];

                    for ( int i = 1; i < tok.length; ++i )
                    {
                        if ( ( tup.y != null ) && ( tup.y.length() > 0 ) )
                            tup.y += SEPAR[ 2 ] + tok[ i ];
                        else
                            tup.y = tok[ i ];
                    }
                }
            } 
        }

        return tup;
    }

    // paring splits "left" + ":" + "right" (colon seperated string)
    // into a tuple ... osa  known string (e.g. not an index - see above)
    public hive_tuple<String, String> pair( String str )
    {
        hive_tuple<String, String> tup = null;

        if ( ( str != null ) && ( str.length() > 0 ) )
        {
            String[] tok = str.split( SEPAR[ 2 ] );

            if ( tok.length > 0 )
            {
                if ( ( tup = new hive_tuple<String, String>() ) != null )
                {
                    tup.x = tok[ 0 ];

                    for ( int i = 1; i < tok.length; ++i )
                    {
                        if ( ( tup.y != null ) && ( tup.y.length() > 0 ) )
                            tup.y += SEPAR[ 2 ] + tok[ i ];
                        else
                            tup.y = tok[ i ];
                    }
                }
            } 
        }

        return tup;
    }

    // part splits base value from paraemters "type( A, B )", returning "type" 
    // and "( A, B )" as a tuple ...
    public hive_tuple<String, String> part( String par )
    {
        hive_tuple<String, String> tup = null;

        if ( ( par != null ) && ( par.length() > 0 ) )
        {
            if ( ( tup = new hive_tuple<String, String>() ) != null )
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
    public hive_tuple<String, String> vals( String par )
    {
        hive_tuple<String, String> tup = null;

        if ( ( par != null ) && ( par.length() > 0 ) )
        {
            if ( ( tup = new hive_tuple<String, String>() ) != null )
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

    //
    public void clear()
    {
        // do not reuse the hashmap, so the following is
        // faster and more effcient than map_.clear() && map_.resize()
        map_ = null;
        map_ = new HashMap<String, ArrayList<String>>();
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
                ArrayList<String> val = (ArrayList<String>)ent.getValue();

                str += ent.getKey() + "\n";

                for ( String v : val )
                    str += "+ " + v + "\n";

                str += "\n";
            }
        }

        return str;
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

