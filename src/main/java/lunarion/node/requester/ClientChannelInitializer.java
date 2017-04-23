
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
package lunarion.node.requester;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

	/*
	 * if the messager decoder is the LengthFieldBasedFrameDecoder, 
	 * the first 4 bytes are the length of the message
	 */
    final public static int MESSAGE_LENGTH = 4;
    final ClientHandler client_handler;
    
    public ClientChannelInitializer(ChannelInboundHandlerAdapter _client_handler)
    {
    	this.client_handler = (ClientHandler) _client_handler;
    }
    @Override
    protected void initChannel(SocketChannel socketChannel ) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        
        pipeline.addLast("decoder", 
        		new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, ClientChannelInitializer.MESSAGE_LENGTH, 0, ClientChannelInitializer.MESSAGE_LENGTH));
        pipeline.addLast(this.client_handler);
    }

	 

}
