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
import java.lang.*;
import java.security.*;

import javax.security.auth.*;
import javax.security.auth.login.*;
import javax.security.auth.callback.*;
import javax.security.auth.kerberos.*;

// define an empty callback handler, which sets both the
// username and password data to an empty string (as there
// is no ability to provide a prompted response from a user)
@SuppressWarnings("deprecation")
public class callback_handler implements CallbackHandler
{
    public void handle( Callback[] cb )
        throws IOException, UnsupportedCallbackException
    {
        for ( int i = 0; i < cb.length; i++ )
        {
            if ( cb[ i ] instanceof NameCallback )
            {
                NameCallback nc = (NameCallback)cb[ i ];
                nc.setName( "" );
            }
            else if ( cb[ i ] instanceof PasswordCallback )
            {
                PasswordCallback pc = (PasswordCallback)cb[ i ];
                pc.setPassword( ( new String( "" ) ).toCharArray() );
            }
            else
                throw new UnsupportedCallbackException( cb[ i ], "Unrecognised callback" );
        }
    }
};


