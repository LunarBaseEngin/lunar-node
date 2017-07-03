
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

 
import java.util.UUID;
import java.util.Vector; 
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.Bootstrap; 
import io.netty.channel.AdaptiveRecvByteBufAllocator; 
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener; 
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup; 
import io.netty.channel.socket.nio.NioSocketChannel;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;

public class LunarDBClient { 
	public Vector<ChannelFuture > channel_list = new Vector<ChannelFuture >();
	
	EventLoopGroup group = null;  
	ClientHandler client_handler = null ;
	
	AtomicBoolean connected = new AtomicBoolean(false);
	String connected_host_ip = null;
	int connected_port = -1;
	
	/*
	 * if internal, then this client is used between data nodes and coordinator,
	 * while if it is used in a driver client, internal is false; 
	 */
	boolean internal = true;
	//private final int parallel = Runtime.getRuntime().availableProcessors() ;
	   
	//private static ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) NodeThreadPool.getExecutor(16, -1);
	//protected ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	 
	public LunarDBClient( )
	{
		internal = true;
	}
	public LunarDBClient(boolean _internal)
	{
		internal = _internal;
	}
	public ChannelFuture connect(String host, int port) throws Exception { 
		group = new NioEventLoopGroup();    
		client_handler = new ClientHandler(internal) ;
		
	        try {
	            Bootstrap client_bootstrap = new Bootstrap();
	            client_bootstrap.group(group)
	            		.channel(NioSocketChannel.class)
	            		.option(ChannelOption.SO_SNDBUF,  1024 * 1024)
	            		.option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64, 1024, 65536*512))
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
		if(connected.get())
		{
			connected_host_ip = null;
			connected_port = -1;
			connected.set(false);
			client_handler.close();
			group.shutdownGracefully();
		}
		
	}
	
	public RemoteResult sendRequest(CMDEnumeration.command cmd, Object[] args, int waiting_in_milliseconds) throws InterruptedException
	{
		MessageRequest request = new MessageRequest();
        request.setUUID(UUID.randomUUID().toString()); 
        request.setCMD(cmd);
        request.setParams((String[])args);
       
        ChannelFuture cf = channel_list.get(0);
        MessageResponse resp = client_handler.sendRequest(request, cf, waiting_in_milliseconds);
        if(resp == null)
        	return null;
        
        return new RemoteResult(this, resp);
        
	}
	
	public RemoteResult sendRequest(CMDEnumeration.command cmd, Object[] args ) throws InterruptedException
	{ 
		/*
		 * by default waiting 5 seconds.
		 */
        return sendRequest( cmd, args, 5*1000);
        		
	}
	
	public MessageResponse internalQuery(CMDEnumeration.command cmd, Object[] args, int waiting_in_milliseconds) throws InterruptedException
	{
		MessageRequest request = new MessageRequest();
        request.setUUID(UUID.randomUUID().toString()); 
        request.setCMD(cmd);
        request.setParams((String[])args);
       
        ChannelFuture cf = channel_list.get(0);
        //System.err.println("internal query sent");
        return  client_handler.sendRequest(request, cf, waiting_in_milliseconds) ;
        
	}
	
}
