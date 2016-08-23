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


