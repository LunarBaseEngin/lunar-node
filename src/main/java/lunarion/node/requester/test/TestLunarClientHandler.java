
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

import java.util.logging.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class TestLunarClientHandler extends ChannelInboundHandlerAdapter {

		    private static final Logger logger = Logger
		            .getLogger(TestLunarClientHandler.class.getName());

		    private final ByteBuf firstMessage;

		    public TestLunarClientHandler() {
		        byte[] req = "QUERY TIME ORDER".getBytes();
		        firstMessage = Unpooled.buffer(req.length);
		        firstMessage.writeBytes(req);
		    }

		    @Override
		    public void channelActive(ChannelHandlerContext ctx) throws Exception {
		       
		        System.out.println("client @channelActive..");
		        ctx.writeAndFlush(firstMessage); 
		        
		        for(int i = 0; i< 10; i++)
		        {
		        	byte[] req = ("messenge " + i).getBytes();
			        ByteBuf nextMessage =  ctx.alloc().buffer(req.length);
			        
			        nextMessage = Unpooled.buffer(req.length);
			        nextMessage.writeBytes(req);
		        	ctx.writeAndFlush(nextMessage); // (3)
		        	//Thread.sleep(2000);
		        }
		    }

		    @Override
		    public void channelRead(ChannelHandlerContext ctx, Object msg)
		            throws Exception {
		    	ByteBuf buf = (ByteBuf) msg;
		    	try
		    	{
		    		System.out.println("client @channelRead..");
		    		/*
		    		 * server returns message
		    		 */
		    		
		    		byte[] req = new byte[buf.readableBytes()];
		    		buf.readBytes(req);
		    		String body = new String(req, "UTF-8");
		    		System.out.println("Now is :" + body);
		    	}
		    	finally
		    	{
		    		buf.release();
		    	}
		    }

		    @Override
		    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
		            throws Exception {
		        System.out.println("client exception caught @exceptionCaught..");
		       
		        logger.warning("Unexpected exception from downloading data stream @exceptionCaught: "
		                + cause.getMessage());
		        ctx.close();
		    }
}
