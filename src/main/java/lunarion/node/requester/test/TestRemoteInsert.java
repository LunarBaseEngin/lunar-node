
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
package lunarion.node.requester.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.requester.LunarDBClient;

public class TestRemoteInsert {
	public static void main(String[] args) throws Exception {
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
        	for(int i = 0;i<300;i++)
        	{  
        		CMDEnumeration.command cmd = CMDEnumeration.command.insert;
        		String[] params = new String[5];
        		params[0] = "CorpusDB";
        		params[1] = "textTable_remote_1";
        		params[2] = "{content=[\" this is the test content " + i+".\"]}";
        		params[3] = "{content=[\" this is the test content " + (i*10)+".\"]}";
        		params[4] = "{content=[\" 是共同的强，大家都有份,,.  \"]}"; 
        	 	
        		MessageResponse resp_from_svr = client.sendRequest(cmd, params); 
           	 
            	System.out.println("LunarNode responded command: "+ resp_from_svr.getCMD());
        		System.out.println("LunarNode responded UUID: "+ resp_from_svr.getUUID());
        		System.out.println("LunarNode responded suceed: "+ resp_from_svr.isSucceed());
        		for(int j=0;j<resp_from_svr.getParams().length;j++)
        		{
        			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[j]);
        		}
        	}
        
        
        	/*
        	 * it's ok, use condition.await(10*1000, TimeUnit.MILLISECONDS)
        	 * at MessageClientWatcher 
        	 * to block the thread waiting for server response.
        	 */
        	 cf.channel().closeFuture().sync();
        
        } finally {
            
        	client.shutdown();
        }
        
    }
}
