
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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import LCG.DB.API.Result.FTQueryResult;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class CloseQueryResult implements ExecutorInterface{


	private ConcurrentHashMap<String, FTQueryResult> result_map;
	
	public CloseQueryResult(  ConcurrentHashMap<String, FTQueryResult> _result_map)
	{
		result_map = _result_map;
	}
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD(); 
		
		 return closeQueryResult(l_db_ssa, msg, logger);
	}
	 
	private MessageResponse closeQueryResult(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
	{
		String[] params = (String[])request.getParams();
   	 
    	MessageResponse response = null;
	    	if(params.length <3 )
			{
				System.err.println("[NODE ERROR]: wrong parameters for closing a query result.");
				response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
				return response;
			}
			String db = params[0];
	        String table = params[table_name_index];
	        String query_result_uuid = params[2];
	        
	        FTQueryResult result = this.result_map.remove(query_result_uuid);
	        if(result!= null)
	        {  
	        	response = new MessageResponse();
			  	response.setUUID(request.getUUID());
			  	response.setCMD(request.getCMD());
			  	response.setSucceed(true); 
			  	String[] resp = new String[3];
			  	resp[0] = db; 
			  	resp[1] = table;  
			  	resp[2] = CodeSucceed.result_removed_succeed; 
			  	response.setParams(resp); 
			  	return response;
	        } 
	        else
	        {
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.result_removed_failed);   
	        	return response;
	        } 
			  	
	       
			   
	    }
}
