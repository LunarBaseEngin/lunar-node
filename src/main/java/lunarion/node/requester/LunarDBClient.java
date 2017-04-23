
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

import java.lang.reflect.Method;
import java.util.Date;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class LunarDBClient { 
	public Vector<ChannelFuture > channel_list = new Vector<ChannelFuture >();
	
	EventLoopGroup group = null;  
	ClientHandler client_handler = null ;
	
	AtomicBoolean connected = new AtomicBoolean(false);
	String connected_host_ip = null;
	int connected_port = -1;
	//private final int parallel = Runtime.getRuntime().availableProcessors() ;
	   
	//private static ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) NodeThreadPool.getExecutor(16, -1);
	//protected ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	   
	public ChannelFuture connect(String host, int port) throws Exception { 
		group = new NioEventLoopGroup();    
		client_handler = new ClientHandler() ;
		
	        try {
	            Bootstrap client_bootstrap = new Bootstrap();
	            client_bootstrap.group(group)
	            		.channel(NioSocketChannel.class)
	                    //.option(ChannelOption.TCP_NODELAY, true)
	            		.option(ChannelOption.SO_KEEPALIVE, true)
	                    .handler(new ClientChannelInitializer(client_handler));
	       
	            //ChannelFuture cf = client_bootstrap.connect(host, port).sync();
	            ChannelFuture cf = client_bootstrap.connect(host, port) ;
	            channel_list.add(cf );
	          
	            cf.addListener(new ChannelFutureListener(){
	            	public void operationComplete(final ChannelFuture channelFuture) throws Exception 
	 	            {
	 	                if (channelFuture.isSuccess()) 
	 	                {
	 	                	ClientHandler handler = channelFuture.channel().pipeline().get(ClientHandler.class);
	 	                	client_handler = handler;
	 	                	
	 	                }
	 	            } 
	            });
	          
	            connected.set(true);
	            connected_host_ip = host;
	            connected_port = port;
	             
	            return cf;
	        } finally {
	            
	           // group.shutdownGracefully();
	        }
	    }
	
	public boolean isConnected()
	{
		return this.connected.get();
	}
	
	public String getConnectedHostIP()
	{
		return connected_host_ip;
	}
	
	public int getConnectedPort()
	{
		return connected_port;
	}
	public void shutdown()
	{
		//thread_executor.shutdown();
		 
		connected_host_ip = null;
		connected_port = -1;
		connected.set(false);
		client_handler.close();
		group.shutdownGracefully();
	}
	
	public MessageResponse sendRequest(CMDEnumeration.command cmd, Object[] args) throws InterruptedException
	{
		MessageRequest request = new MessageRequest();
        request.setUUID(UUID.randomUUID().toString()); 
        request.setCMD(cmd);
        request.setParams((String[])args);
       
        ChannelFuture cf = channel_list.get(0);
        
        return client_handler.sendRequest(request, cf);
        
	}
	
}
