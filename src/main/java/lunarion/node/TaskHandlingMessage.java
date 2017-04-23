
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
import java.util.Map;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.EDF.NodeTaskCenter;
import lunarion.node.EDF.events.VNodeIncomingRecords;
import lunarion.node.logger.LogCMDConstructor;
import lunarion.node.logger.LogTable;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseQuery;

public class TaskHandlingMessage implements Runnable {

    private MessageRequest request = null;
    private MessageResponse response = null;
    private NodeTaskCenter node_tc= null;
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

    TaskHandlingMessage(MessageRequest request , NodeTaskCenter task_center, ChannelHandlerContext ctx) {
        this.request = request;
         
        this.node_tc = task_center;
        this.ctx = ctx;
    }

    public void run() { 
       
        execute(request); 
        
      
        int len = response.size();
        ByteBuf response_buff = Unpooled.buffer(4+len);
        response_buff.writeInt(len);
        response.write(response_buff);
      
    	ctx.writeAndFlush(response_buff).addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                System.out.println("[NODE INFO]: LunarNode responsed the request with message id:" + request.getUUID());
            }
        });
    }

    private void execute(MessageRequest request) {
        
        CMDEnumeration.command cmd = request.getCMD();
        String[] params = (String[])request.getParams();
        
        switch(cmd)
        {
        	case createTable:
        		createTable(params);
        		break;
        	case addFulltextColumn:
        		addFulltextColumn( params);
        		break;
        	case insert: 
        		insert( params);
        		break;
        	case ftQuery: 
        		ftQuery( params);  
        		break;
        	case fetchRecords: 
        		fetchRecords( params);  
        		break;
        	case fetchLog: 
        		if(params.length != 4)
        		{
        			System.err.println("[NODE ERROR]: wrong parameters for fetching log.");
        			responseError(CodeSucceed.wrong_parameters);
                	return ; 
        		}
        		params[1] = LogCMDConstructor.getLogTableName(params[1]); 
        		fetchRecords( params);  
        		break;
        	default:
        		break;
        }
        return;
        		
    }
    
    private void createTable(String[] params)
    {
    	boolean suc = true;
		if(params.length < 2)
		{ 
			System.err.println("[NODE ERROR]: creating a table needs at least 2 parameters: db name and table name.");
			suc = false ; 
			responseError(CodeSucceed.wrong_parameters);
			return;
		}
		String db = params[0];
		String table = params[1];
		if(db == null || "".equals(db.trim()) 
				||table == null || "".equals(table.trim()))
			suc = false ;
		
		String[] resp = new String[4];
		  	resp[0] = db;
		  	resp[1] = table;
		  	
		
        LunarDB l_DB = node_tc.getActiveServer().getDBInstant(db);
        
        if(l_DB == null)
        {
        	suc = false;
        	responseError(CodeSucceed.db_does_not_exist);
        	return;
        }
        else
        {
        	if(suc && !l_DB.hasTable(table))
            {	
            	suc = l_DB.createTable(table);
            	l_DB.openTable(table);
            	
            	String log_table = LogCMDConstructor.getLogTableName(table);
            	
            	boolean log_table_created = false;
            	log_table_created = l_DB.createTable(log_table);
            	l_DB.openTable(log_table);
            	
            	if(suc)
            		resp[2] = CodeSucceed.create_table_succeed;
            	else
            		resp[2] = CodeSucceed.create_table_failed_exception;
            	
            	if(log_table_created)
            		resp[3] = CodeSucceed.create_log_table_succeed;
            	else
            		resp[3] = CodeSucceed.create_log_table_failed_exception;
            	
            	LogTable.logCreatingTable( db, table, log_table, l_DB);
            }
        	else
            {
            	suc = false;
            	resp[2] = CodeSucceed.create_table_failed_already_exists;
            }
            
            
        }
        
        response = new MessageResponse();
        response.setUUID(request.getUUID());
        response.setCMD(request.getCMD());
        response.setSucceed(suc); 
        response.setParams(resp);  
    }

   
   
    private void addFulltextColumn(String[] params)
    {
    	boolean suc = true;
		if(params.length < 3)
		{
			System.err.println("[NODE ERROR]: addFulltextColumn needs at least 3 parameters: db name, table name and column name");
			suc = false ;
			responseError(CodeSucceed.wrong_parameters);
			return;
		}
		String db = params[0];
		String table = params[1];
		String log_table = LogCMDConstructor.getLogTableName(table);
		String column = params[2];
		if(db == null || "".equals(db.trim()) 
				||table == null || "".equals(table.trim()))
			suc = false ;
		
		String[] resp = new String[4];
		resp[0] = db;
		resp[1] = table;
		  	
		
        LunarDB l_DB = node_tc.getActiveServer().getDBInstant(db);
        if(l_DB == null)
        {
        	suc = false;
        	responseError(CodeSucceed.db_does_not_exist);
        	return;
        }
        else
        {
        	if(suc && !l_DB.hasTable(table))
            {	
            	suc = l_DB.createTable(table);
            	l_DB.openTable(table);
            	if(suc)
            		resp[2] = CodeSucceed.create_table_succeed;
            	else
            		resp[2] = CodeSucceed.create_table_failed_exception;
            	
            	
            	
            	boolean log_table_created = false;
            	log_table_created = l_DB.createTable(log_table);
            	l_DB.openTable(log_table);
            	if(log_table_created)
            		resp[3] = CodeSucceed.create_log_table_succeed;
            	else
            		resp[3] = CodeSucceed.create_log_table_failed_exception;
            	
            	LogTable.logCreatingTable( db, table, log_table, l_DB);
            	
            }
            if(suc)
            {
            	LunarTable tt = l_DB.getTable(table); 
         		if(tt.addFulltextSearchable(column))
         		{
         			resp[3] = CodeSucceed.add_fulltext_column_succeed;
         		}
         		else
         		{
         			resp[3] = CodeSucceed.add_fulltext_failed;
         			suc = false;
         		}
         		 
        		TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
        		tt.registerTokenizer(t_e);
                
        		LogTable.logAddingFulltextColumn( db, table, column, l_DB);
        	    
            }
            else
            {
            	 
            }
        } 
        
        response = new MessageResponse();
        response.setUUID(request.getUUID());
        response.setCMD(request.getCMD());
        response.setSucceed(suc); 
        response.setParams(resp);  
    }
    
    
    private void insert(String[] params)
    {
    	if(params.length < 3)
		{
			System.err.println("[NODE ERROR]: wrong parameters for inserting records.");
			responseError(CodeSucceed.wrong_parameters);
			return ;
		}
    	
    	String db = params[0];
        String table = params[1];
        String[] recs_insert = new String[params.length-2];
        for(int i=0;i<params.length-2;i++)
        {
        	recs_insert[i] = params[i+2];
        }
		//return node_tc.dispatch(new VNodeIncomingRecords(db,table,recs));
        LunarDB l_DB = node_tc.getActiveServer().getDBInstant(db);
        
        
        if(l_DB == null)
        { 
        	responseError(CodeSucceed.db_does_not_exist);
        	return ;
        }
        LunarTable t_table = l_DB.getTable(table);
        if(t_table == null)
        {
        	response = new MessageResponse();
		  	response.setUUID(request.getUUID());
		  	response.setCMD(request.getCMD());
		  	response.setSucceed(false); 
		  	String[] resp = new String[1];
		  	resp[0] = CodeSucceed.table_does_not_exist;
		  	response.setParams(resp); 
        	return ;
        }
        
		TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
		t_table.registerTokenizer(t_e);
		
		
		
		
		Record32KBytes[] results = l_DB.insertRecord(table, recs_insert);
		boolean suc = true;  	
		  	
		String[] result_str = new String[results.length];
		for(int i=0;i<results.length;i++)
		{
			if(results[i].getID()<0)
			{
				result_str[i] = results[i].recData();
				suc = false;
			}
			else
				result_str[i] = null;
		}
		if(suc)
		{
		  	LogTable.logInsert(db, table, recs_insert, l_DB);	
		}
			  	
		response = new MessageResponse();
		response.setUUID(request.getUUID());
		response.setCMD(request.getCMD());
		response.setSucceed(suc);
		response.setParams(result_str); 
		 
    }

    private void ftQuery(String[] params)
    {
    	if(params.length != 5)
		{
			System.err.println("[NODE ERROR]: wrong parameters for a query request.");
			responseError(CodeSucceed.wrong_parameters);
			return ;
		}
		String db = params[0];
        String table = params[1];
        String statement = params[2];
        int from = Integer.parseInt(params[3]);
        int count = Integer.parseInt(params[4]);
         
		//return node_tc.dispatch(new VNodeIncomingRecords(db,table,recs));
        LunarDB l_DB = node_tc.getActiveServer().getDBInstant(db);
        if(l_DB == null)
        {   
        	responseError(CodeSucceed.db_does_not_exist);
        	return ;
        }
        
        
        LunarTable t_table = l_DB.getTable(table);
        if(t_table == null)
        {
        	response = new MessageResponse();
		  	response.setUUID(request.getUUID());
		  	response.setCMD(request.getCMD());
		  	response.setSucceed(false); 
		  	String[] resp = new String[1];
		  	resp[0] = CodeSucceed.table_does_not_exist;
		  	response.setParams(resp); 
        	return ;
        }
        
        int latest_count = 0; //get all records of the query.
        FTQueryResult result = null;
        ArrayList<Record32KBytes> recs = null;
        try {
		  		result = l_DB.queryFullText(table, statement, latest_count);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  	if(result != null )
		  	{
		  		try {
				recs = result.fetchRecords(from, count);
				response = new MessageResponseQuery();
				response.setUUID(request.getUUID());
		        response.setCMD(request.getCMD());
		        response.setSucceed(true);
				response.setParams(recs);
				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		  	}
		  	else
		  	{
		  		response = new MessageResponseQuery();
				response.setUUID(request.getUUID());
		        response.setCMD(request.getCMD());
		        response.setSucceed(false);
				response.setParams(recs);
		  	} 
		  	
		  	//return respond;
		   
    }
    
    private void responseError(String error_code)
    {
    	response = new MessageResponse();
	  	response.setUUID(request.getUUID());
	  	response.setCMD(request.getCMD());
	  	response.setSucceed(false); 
	  	String[] resp = new String[1];
	  	resp[0] = error_code;
	  	response.setParams(resp); 
    }
    
  
    private void fetchRecords(String[] params)
    {
    	if(params.length != 4)
		{
			System.err.println("[NODE ERROR]: wrong parameters for fetching records.");
			responseError(CodeSucceed.wrong_parameters);
        	return ; 
		}
		String db = params[0];
        String table = params[1];
        
        int from = Integer.parseInt(params[2]);
        int count = Integer.parseInt(params[3]);
         
		 
        LunarDB l_DB = node_tc.getActiveServer().getDBInstant(db);
        if(l_DB == null)
        {  
		  	responseError(CodeSucceed.db_does_not_exist);
        	return ;
        } 
        
        LunarTable t_table = l_DB.getTable(table);
        if(t_table == null)
        { 	
		  	responseError(CodeSucceed.table_does_not_exist);
        	return ;
        }
        
         
        ArrayList<Record32KBytes> recs = null;
        try {
        	recs = l_DB.fetchRecords(table, from, count);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if(recs != null )
        {
        	response = new MessageResponseQuery();
        	response.setUUID(request.getUUID());
        	response.setCMD(request.getCMD());
        	response.setSucceed(true);
        	response.setParams(recs); 
        }
        else
        { 
        	responseError(CodeSucceed.no_records_found);
        } 
		  	
		  	//return respond;
    }
}
