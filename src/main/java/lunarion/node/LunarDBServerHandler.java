
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
package lunarion.node;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lunarion.node.EDF.NodeTaskCenter;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse; 

public class LunarDBServerHandler extends ChannelInboundHandlerAdapter {

	private final NodeTaskCenter node_tc;
	private final Logger logger;

    public LunarDBServerHandler(NodeTaskCenter task_map, Logger _logger  ) {
        this.node_tc = task_map;
        this.logger = _logger;
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {  
    	 
  
    	try{
    		ByteBuf buf = (ByteBuf) msg;
    		 
    		MessageRequest request = new MessageRequest();
    		request.read(buf); 
        	 
    		System.out.println("LunarNode received command: "+ request.getCMD());
    		System.out.println("LunarNode received UUID: "+ request.getUUID());
    		for(int i=0;i<request.getParams().length;i++)
    		{
    			System.out.println("LunarNode received: "+ request.getParams()[i]);
    		}
    		//MessageResponse response = new MessageResponse();
    		//TaskHandlingMessage recvTask = new TaskHandlingMessage(request, response, node_tc, ctx);
    		TaskHandlingMessage recvTask = new TaskHandlingMessage(request , node_tc, ctx, logger);
            
            node_tc.getActiveServer().submit(recvTask);
          
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
}
