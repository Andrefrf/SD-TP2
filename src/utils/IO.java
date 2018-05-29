package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;

final public class IO {

	public static void write( OutputStream out, char data ) {
		try {
			out.write( data );			
		} catch( IOException x ) {
		}
	}
	
	
	public static void write( OutputStream out, byte[] data ) {
		try {
			out.write( data );			
		} catch( IOException x ) {
		}
	}
	
	public static void write( OutputStream out, byte[] data, int off, int len ) {
		try {
			out.write( data, off, len );			
		} catch( IOException x ) {
		}
	}
	
	public static String readLine( BufferedReader reader ) {
		try {
			return reader.readLine();
		} catch (IOException e) {
			return null;
		}
	}
}
