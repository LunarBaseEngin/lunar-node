
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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;

public class FetchRecords implements ExecutorInterface{
	
	boolean if_DESC = false;
	public FetchRecords(boolean if_desc)
	{
		this.if_DESC = if_desc;
	}
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 
		
		 return fetchRecords(l_db_ssa, msg, logger);
	}
	
	private MessageResponse fetchRecords(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger )
    {
		String[] params = (String[])request.getParams();
		return fetchRecords( l_db_ssa , params, request, logger );
    }
	
	protected MessageResponse fetchRecords(LunarDBServerStandAlone l_db_ssa , String[] params, MessageRequest request, Logger logger )
    { 
    	 
    	MessageResponse response = null; 
  
    	if(params.length != 4)
		{
			System.err.println("[NODE ERROR]: wrong parameters for fetching records, can not be: " + params.length);
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
        	return response; 
		}
		String db = params[0];
        String table = params[table_name_index];
        
        int from = Integer.parseInt(params[2]);
        int count = Integer.parseInt(params[3]);
         
		 
        LunarDB l_DB = l_db_ssa.getDBInstant(db);
        if(l_DB == null)
        {  
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
        	return response;
        } 
        
        LunarTable t_table = l_DB.getTable(table);
        if(t_table == null)
        { 	
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.table_does_not_exist);
        	return response;
        }
        
         
        ArrayList<Record32KBytes> recs = null;
        try {
        	if(if_DESC)
        		recs = l_DB.fetchRecords(table, from, count);
        	else
        		recs = l_DB.fetchRecordsASC(table, from, count);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if(recs != null && !recs.isEmpty())
        {
        	response = new MessageResponseForQuery();
        	response.setUUID(request.getUUID());
        	response.setCMD(request.getCMD());
        	response.setSucceed(true);
        	response.setParamsFromNode(db, table, recs); 
        }
        else
        { 
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.no_records_found);
        } 
		  	
        return response;
    }
    

}
