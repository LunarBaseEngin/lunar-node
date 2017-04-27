
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

import java.util.Map;

import org.apache.log4j.Logger;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lunarion.node.EDF.NodeTaskCenter; 

public class LunarServerChannelInitializer extends ChannelInitializer<SocketChannel> {

  
	/*
	 * if the messager decoder is the LengthFieldBasedFrameDecoder, 
	 * the first 4 bytes are the length of the message
	 */
    final public static int MESSAGE_LENGTH = 4;
     
    private NodeTaskCenter node_tc;
    private final Logger logger;

    LunarServerChannelInitializer(NodeTaskCenter task_center, Logger _logger) {
        this.node_tc = task_center;
        logger = _logger;
    } 

    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
         
        pipeline.addLast("decoder",
        		new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, LunarServerChannelInitializer.MESSAGE_LENGTH, 0, LunarServerChannelInitializer.MESSAGE_LENGTH));
        pipeline.addLast(new LunarServerHandler(this.node_tc, logger ));
    }
}
