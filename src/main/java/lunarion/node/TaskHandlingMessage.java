
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List; 
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger; 
import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.DBStatus.DBRuntimeStatus;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.MemoryIndex.IndexTypes;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.EDF.ExecutorCenter;
import lunarion.node.logger.LogCMDConstructor;
import lunarion.node.logger.TableOperationLogger;
import lunarion.node.logger.Timer; 
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;
import lunarion.node.requester.MessageClientWatcher;
import lunarion.node.utile.ControllerConstants;

public class TaskHandlingMessage implements Runnable {

	
	
    private MessageRequest request = null;
    private MessageResponse response = null;
	private ExecutorCenter node_tc = null;
    
    
    private ChannelHandlerContext ctx = null; 
    
    public MessageResponse getResponse() {
        return response;
    }

    public MessageRequest getRequest() {
        return request;
    }

    public void setRequest(MessageRequest request) {
        this.request = request;
    }

    TaskHandlingMessage(MessageRequest request , 
    						ExecutorCenter node_executor_centor, 
    						ChannelHandlerContext ctx ) {
        this.request = request; 
        this.node_tc = node_executor_centor; 
        this.ctx = ctx; 
    }
    
    TaskHandlingMessage(MessageRequest request , 
    					LunarDBServerStandAlone _l_db_ssa,  
						Logger _logger) {
    		this.request = request; 
    		this.node_tc = _l_db_ssa.getExecutorCenter(); 
    		this.ctx = null; 
    }

    public void run() { 
        
    	response = node_tc.dispatch(request);
        
        if(ctx!=null)
        { 	
        	 int len = response.size();
             ByteBuf response_buff = Unpooled.buffer(4+len);
             response_buff.writeInt(len);
             response.write(response_buff);
           
             ctx.writeAndFlush(response_buff).addListener(new ChannelFutureListener() {
                 public void operationComplete(ChannelFuture channelFuture) throws Exception {
                     //System.out.println("[NODE INFO]: LunarNode responsed the request with message id:" + request.getUUID());
                 }
             });
        }
       
    }

    
   
    
}
