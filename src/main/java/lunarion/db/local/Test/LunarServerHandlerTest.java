
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
package lunarion.db.local.Test;

import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class LunarServerHandlerTest  extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
    	try{
        	ByteBuf buf = (ByteBuf) msg;
        	byte[] req = new byte[buf.readableBytes()];
        	buf.readBytes(req);
        	String body = new String(req, "UTF-8");
        	System.out.println("LunarNode receive msg: " + body);
        	 
        	//ByteBuf resp = Unpooled.copiedBuffer(body.getBytes());
        	int len = body.getBytes().length;
            ByteBuf response = Unpooled.buffer(4+len);
            response.writeInt(len);
            response.writeBytes(body.getBytes()); 
        	ctx.write(response);
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