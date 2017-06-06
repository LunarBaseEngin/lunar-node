
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

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForDriver;

public class ClientHandler extends ChannelInboundHandlerAdapter {

    private ConcurrentHashMap<String, MessageClientWatcher> watcher_map = new ConcurrentHashMap<String, MessageClientWatcher>();

    private volatile Channel channel;
    private SocketAddress remoteAddr;

    private boolean internal;
    
    public ClientHandler(boolean _internal)
    {
    	internal = _internal;
    }
    
    public Channel getChannel() {
        return channel;
    }

    public SocketAddress getRemoteAddr() {
        return remoteAddr;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddr = this.channel.remoteAddress();
    }

    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        this.channel = ctx.channel();
    }
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
     
    	try{
    		ByteBuf buf = (ByteBuf) msg;
    		MessageResponse from_svr = null;
    		if(internal)
    		{
    			from_svr = new MessageResponse();
	    		from_svr.read(buf); 
    		}
    		else
    		{
    			from_svr = new MessageResponseForDriver();
    			from_svr.read(buf);  
    		}
	    		/*
	    		 System.out.println("LunarNode responded command: "+ from_svr.getCMD());
				 System.out.println("LunarNode responded UUID: "+ from_svr.getUUID());
				 System.out.println("LunarNode responded suceed: "+ from_svr.isSucceed());
				 for(int i=0;i< from_svr.getParams().length;i++)
				 {
					 System.out.println("LunarNode responded: "+ from_svr.getParams()[i]);
				 } 
				 
				 */
	    		String message_id = from_svr.getUUID();
	            MessageClientWatcher callBack = watcher_map.get(message_id);
	            if (callBack != null) {
	                watcher_map.remove(message_id);
	                callBack.finish(from_svr);
	            } 
    	 
            
            /*
             * if do not call this, this connection will keep alive, and not close.
             * we need it keeping connected.
             */
            //ctx.close();
        }
        finally
        {
        	ReferenceCountUtil.release(msg);
        } 
    	 
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }

    public void close() {
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    public MessageResponse sendRequest(MessageRequest request, ChannelFuture cf, int waiting_in_milliseconds ) throws InterruptedException {
        MessageClientWatcher watcher = new MessageClientWatcher(request.getUUID());
        watcher_map.put(request.getUUID(), watcher);
        
        int len = request.size();
        ByteBuf nextMessage = Unpooled.buffer(4+len);
        nextMessage.writeInt(len); 
        request.write(nextMessage);
         
        
        cf.channel().writeAndFlush(nextMessage);
        return watcher.start(waiting_in_milliseconds);
        
    }

}
