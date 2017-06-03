
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

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class LunarDBConnection {
	
	private String ip;
	private String port;
	private String db;
	 
	private  LunarDBClient client ;
	public LunarDBConnection(String _ip, String _port, String _db) throws NumberFormatException, Exception
	{
		ip = _ip.trim();
		port = _port.trim();
		db = _db.trim();
		
		client = new LunarDBClient();
		client.connect(ip, Integer.parseInt(port));
	} 
	
	public Statement createStatement()
	{
		return new Statement(client, db );
	}
	public String toString()
	{
		return ip + ":" + port + ":" + db;
	}

}
