
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
package lunarion.cluster.coordinator.test.startnodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarNode; 
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;

public class TaskAddNode implements Runnable {

    private LunarNode node = null;
    private String[] arges = null;


    TaskAddNode(LunarNode _node_instance ) {
        this.node = _node_instance; 
    }

    public void run() { 
    	System.out.println("[INFO]: db node added " );
    	 
    		try {
				node.start();
				//Thread.currentThread().join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
    		 
    	 
    	
        
    }

     
}
