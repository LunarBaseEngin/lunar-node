
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
package lunarion.node.EDF.executors;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.logger.TableOperationLogger;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageToWrite;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.utile.ControllerConstants;

public class CreateTable implements ExecutorInterface{
	
	 
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 
		
		 return createTable(l_db_ssa, msg, logger);
	}
	
	 /*
     * @param  
     * params[0]: db name;
     * params[1]: name of table_i on partition i to be created;
     * 
     * Response:
     * resp[0] = db;
     * resp[1] = table_i on partition i created. 
     * resp[2] = CodeSucceed.create_table_succeed, or failure message.
     * resp[3] = CodeSucceed.create_log_table_succeed, or failure message.  
     */
    private MessageResponse createTable(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
    {
    	String[] params = (String[])request.getParams();
    	 
    	MessageResponse response = null;
    	boolean suc = true;
		if(params.length < 2)
		{ 
			System.err.println("[NODE ERROR]: creating a table needs at least 2 parameters: db name and table name.");
			logger.info("[NODE ERROR]: creating a table needs at least 2 parameters: db name and table name.");
			suc = false ; 
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			return response;
		}
		String db = params[0];
		String table = params[table_name_index];
		if(db == null || "".equals(db.trim()) 
				||table == null || "".equals(table.trim())
				)
		{
			suc = false ;
			response = ExecutorInterface.responseError(request, db, table, CodeSucceed.empty_name );
			return response;
		}
		
		if(ControllerConstants.isIllegalTableName(table))
		{
			suc = false ;
			response = ExecutorInterface.responseError(request, db, table, CodeSucceed.illegal_table_name);
			return response;
		}
		
		String[] resp = new String[5];
		resp[0] = db;
		resp[table_name_index] = table;
		  	
		
        LunarDB l_DB = l_db_ssa.getDBInstant(db);
        
        if(l_DB == null)
        {
        	suc = false;
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
        	return response;
        }
        else
        {
        	if(suc && !l_DB.hasTable(table))
            {	
            	suc = l_DB.createTable(table);
            	l_DB.openTable(table);
            	
            	String log_table = ControllerConstants.getLogTableName(table);
            	
            	boolean log_table_created = false;
            	log_table_created = l_DB.createTable(log_table);
            	l_DB.openTable(log_table);
            	
            	if(suc)
            	{
            		resp[2] = CodeSucceed.create_table_succeed;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_table_succeed); 
            	}
            	else
            	{
            		resp[2] = CodeSucceed.create_table_failed_exception;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_table_failed_exception); 
            	}
            	
            	if(log_table_created)
            	{
            		resp[3] = CodeSucceed.create_log_table_succeed;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_table_succeed); 
            	}
            	else
            	{
            		resp[3] = CodeSucceed.create_log_table_failed_exception;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_log_table_failed_exception); 
            	}
            	
            	TableOperationLogger.logCreatingTable( db, table, log_table, l_DB);
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
        
        return response;
    }

	 
}
