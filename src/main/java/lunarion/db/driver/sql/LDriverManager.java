
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
package lunarion.db.driver.sql;

public class LDriverManager {

	private final static String driver_prefix = "jdbc:lunarion:thin:@";
	/*
	 * e.g.
	 * getConnection("jdbc:oracle:thin:@127.0.0.1:1521:db_name", "scott","tiger");
	 */
	public static LunarDBConnection getConnection(String url, String user, String pw) throws NumberFormatException, Exception
	{
		if(!url.startsWith(driver_prefix))
			return null;
		
		String url_and_db = url.trim().substring(driver_prefix.length(), url.length());
		String[] arr = url_and_db.split(":");
		if(arr.length != 3)
			return null;
		
		String ip = arr[0];
		String port = arr[1];
		String db = arr[2];
		
		return new LunarDBConnection(ip, port, db);
	}
	
	 
	public static void main(String[] args) throws NumberFormatException, Exception {
		String str = "jdbc:lunarion:thin:@127.0.0.1:60001:db_name";
		LunarDBConnection con = LDriverManager.getConnection(str, "", "");
		
		System.out.println(con.toString());
	}

}
