
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
package lunarion.cluster.coordinator.test;

import io.netty.channel.ChannelFuture;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.requester.LunarDBClient;

public class sendRequest {
	
	public static void main(String[] args) throws Exception 
	{
		/*
		 * use lunarDBClient to send request to the coordinator
		 */
		int port = 9090;
        if (args != null && args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
            }
        }
        LunarDBClient client = new LunarDBClient();
        client.connect("127.0.0.1", port);
        
        try
        {
        	
         
        	ChannelFuture cf = client.channel_list.get(0); 
        	CMDEnumeration.command cmd = CMDEnumeration.command.createTable;
        	String[] params = new String[2];
        	params[0] = "RTSeventhDB";
        	params[1] = "node_table"; 
        		
        	client.sendRequest(cmd, params);  
        
        
        	cf.channel().closeFuture().sync();
        
        } finally {
            
        	client.shutdown();
        }
	}

}
