
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
package lunarion.cluster.coordinator;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.db.local.shell.CMDEnumeration; 
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.requester.MessageClientWatcher;

public class TaskSendReqestToNode implements Callable<RemoteResult>  {

	 
	private LunarDBClient client;
	private CMDEnumeration.command cmd;
	private String[] params;
	 
	public TaskSendReqestToNode( LunarDBClient _client, CMDEnumeration.command _cmd, 
								String[] _params )
	{
		 
		client = _client;
		cmd = _cmd;
		params = _params;
		 
	}
	@Override 
	public RemoteResult call() { 
		RemoteResult resp_from_svr; 
		 try {
			 
			 resp_from_svr = client.sendRequest(cmd, params);
       	 
			/* 
			System.out.println("received cmd: "+ resp_from_svr.getCMD());
			System.out.println("received UUID: "+ resp_from_svr.getUUID());
			System.out.println("received succeed: "+ resp_from_svr.isSucceed());
			for(int i=0;i<resp_from_svr.getParams().length;i++)
			{
				 System.out.println("received succeed: "+ resp_from_svr.getParams()[i]);
			} 
			  */
			// response_map.put(partition, resp_from_svr);
			
			
		 } catch (InterruptedException e) {
			// TODO Auto-generated catch block
		 	e.printStackTrace();
			
		 	return null;
		 }  
		 
		 return resp_from_svr ;
	}

}
