
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class CoordinatorServerHandler extends ChannelInboundHandlerAdapter {
 	 

	private Coordinator co; 
	private final Logger logger; 
	
	private final int parallel = Runtime.getRuntime().availableProcessors() ; 
	protected  ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	
	/*
	 * <request_uuid, ResponseCollector>
	 */
			
	private ConcurrentHashMap<String, ResponseCollector> response_map = new ConcurrentHashMap<String, ResponseCollector>();
	//private ConcurrentHashMap<String, ResultSet> sql_result_map = new ConcurrentHashMap<String, ResultSet>();
			    
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
		        	/* 
		    		System.out.println("Coordinator received command: "+ request.getCMD());
		    		System.out.println("Coordinator received UUID: "+ request.getUUID());
		    		for(int i=0;i<request.getParams().length;i++)
		    		{
		    			System.out.println("Coordinator received: "+ request.getParams()[i]);
		    		}
		    		*/
		    		Resource res = co.getResource(request.getParams()[0]); 
		    		 
		    		
		    		TaskRedirectMessage recvTask = new TaskRedirectMessage(request , 
		    				res, 
							ctx, 
							logger, 
							response_map); 
		    		
		    		thread_executor.submit(recvTask);
		    	 
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
		        System.out.println("[EXCEPTION]: coordinator has Exception caught @exceptionCaught");
		        ctx.close();
		        logger.info(Timer.currentTime() + " [EXCEPTION]: coordinator has Exception caught @exceptionCaught");
			 	
		    }
		    
		    public static void main(String[] args) {
				// TODO Auto-generated method stub

			}
    
		
}

 