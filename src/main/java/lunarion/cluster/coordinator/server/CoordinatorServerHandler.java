
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
package lunarion.cluster.coordinator.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import LCG.DB.API.Result.FTQueryResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lunarion.cluster.coordinator.Coordinator;
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.TaskHandlingMessage;
import lunarion.node.EDF.NodeTaskCenter;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class CoordinatorServerHandler extends ChannelInboundHandlerAdapter {
 	 

	private Coordinator co; 
	private final Logger logger; 
	/*
	 * <request_uuid, ResponseCollector>
	 */
			
	private ConcurrentHashMap<String, ResponseCollector> response_map = new ConcurrentHashMap<String, ResponseCollector>();
	private ConcurrentHashMap<String, ResultSet> sql_result_map = new ConcurrentHashMap<String, ResultSet>();
			    
	public CoordinatorServerHandler(Coordinator _co,  Logger _logger ) 
	{
		this.co = _co; 
		this.logger = _logger;  
	}
		   
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
		           throws Exception 
	{ 
		  
		    	try{
		    		ByteBuf buf = (ByteBuf) msg;
		    		 
		    		MessageRequest request = new MessageRequest();
		    		request.read(buf); 
		        	 
		    		System.out.println("Coordinator received command: "+ request.getCMD());
		    		System.out.println("Coordinator received UUID: "+ request.getUUID());
		    		for(int i=0;i<request.getParams().length;i++)
		    		{
		    			System.out.println("Coordinator received: "+ request.getParams()[i]);
		    		}
		    		
		    		Resource res = co.getResource(request.getParams()[0]); 
		    		 
		    		ResponseCollector rc = null;
			    		
		    			rc = res.sendRequest(request.getCMD(), request.getParams());
		    			if(CMDEnumeration.needNotify(request.getCMD()))
			    			res.notifySlavesUpdate(rc);
			    		 
			    		rc.printResponse();
			    		if(ctx!=null)
			    		{
			    			MessageResponse resp = new MessageResponse();
			    			resp.setCMD(request.getCMD());
			    			resp.setUUID(request.getUUID());
			    			resp.setSucceed(rc.isSucceed()); 
			    			
			    			String[] resp_uuid = new String[1];
			    			resp_uuid[0] = UUID.randomUUID().toString();
			    			
			    			resp.setParams(resp_uuid);	
			    			response_map.put(resp_uuid[0], rc);
			    			 int len = resp.size();
			                 ByteBuf response_buff = Unpooled.buffer(4+len);
			                 response_buff.writeInt(len);
			                 resp.write(response_buff);
			               
			             	ctx.writeAndFlush(response_buff).addListener(new ChannelFutureListener() {
			                     public void operationComplete(ChannelFuture channelFuture) throws Exception {
			                         System.out.println("[COORDINATOR INFO]: coordinator responsed the request with message id:" + request.getUUID());
			                     }
			                 });
			    	    }
		    	 
		    		
		    		
					 
					 
					/* 
		    		TaskHandlingMessage recvTask = new TaskHandlingMessage(request , 
		    																node_tc.getActiveServer(), 
		    																ctx, 
		    																logger, 
		    																result_map);
		            */
		           
		          
		    		/*
		        	MessageSource response = new MessageSource();
		            response.setCMD(request.getCMD());
		            response.setUUID(request.getUUID());
		            response.setParams(request.getParams());
		        	int len = response.size();
		            ByteBuf response_buff = Unpooled.buffer(4+len);
		            response_buff.writeInt(len);
		            response.write(response_buff);
		          
		        	ctx.writeAndFlush(response_buff);
		        	*/
		        }
		        finally
		        {
		        	ReferenceCountUtil.release(msg);
		        }
		    }

		    @Override
		    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		        
		        /*
		         * flushes first, then send data to socketChannel
		         */
		        ctx.flush(); 
		    }

		    @Override
		    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		            throws Exception {
		        System.out.println("Exception caught @exceptionCaught..");
		        ctx.close();
		    }
		    
		    public static void main(String[] args) {
				// TODO Auto-generated method stub

			}
    
		
}

 