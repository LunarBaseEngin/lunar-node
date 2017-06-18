
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
import lunarion.node.logger.TableOperationLogger;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class Insert implements ExecutorInterface{

	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 
		
		 return insert(l_db_ssa, msg, logger);
	}
	
	 /*
     * @param  
     * params[0]: db name;
     * params[1]: table name;
     * params[2 ... n]: records to be inserted; 
     * 
     * Response:
     * if succeed:
     * resp[0] = db;
     * resp[1] = table;
     * resp[2] = total result;
     * resp[3] = CodeSucceed.insert_succeed;
     * 
     * else
     * resp[0] = db;
     * resp[1] = table;
     * resp[2] = total result;
     * resp[3] = CodeSucceed.insert_failed;
     * params[3 ... n]: records that failed; 
     */
    private MessageResponse insert(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
    {
    	String[] params = (String[])request.getParams();
   	 
    	MessageResponse response = null;
    	
    	if(params.length < 3)
		{
			System.err.println("[NODE ERROR]: wrong parameters for inserting records.");
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			return response;
		}
    	
    	String db = params[0];
        String table = params[table_name_index];
        String[] recs_insert = new String[params.length-2];
        for(int i=0;i<params.length-2;i++)
        {
        	recs_insert[i] = params[i+2];
        }
		//return node_tc.dispatch(new VNodeIncomingRecords(db,table,recs));
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
		
		
		Record32KBytes[] results = l_DB.insertRecord(table, recs_insert);
		int total_rec = l_DB.recordsCount(table);
		boolean suc = true;  	
		
		try {
			t_table.save();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//String[] result_str = new String[results.length];
		ArrayList<String> failed_rec = new ArrayList<String>();
		int failed = 0;
		for(int i=0;i<results.length;i++)
		{
			if(results[i].getID()<0)
			{
				failed++;
				failed_rec.add(recs_insert[i] );
				suc = false;
			}
			else
				;
		}
		String[] resp_param = null;
		if(failed_rec.size()>0)
		{
			resp_param = new String[failed_rec.size()+4];
			resp_param[0] = db;
			resp_param[table_name_index] = table;
			resp_param[2] = total_rec +""; 
			resp_param[3] = CodeSucceed.insert_failed; 
			for(int k=0;k<failed_rec.size();k++)
			{
				resp_param[k+4] = failed_rec.get(k);  
			}
		}
		else
		{
			resp_param = new String[4];
			resp_param[0] = db;
			resp_param[table_name_index] = table;
			resp_param[2] = total_rec +""; 
			resp_param[3] = CodeSucceed.insert_succeed;
		}
		if(suc)
		{
		  	TableOperationLogger.logInsert(db, table, recs_insert, l_DB);	
		}
			  	
		response = new MessageResponse();
		response.setUUID(request.getUUID());
		response.setCMD(request.getCMD());
		response.setSucceed(suc);
		
		 
		logger.info(Timer.currentTime() + " [NODE INFO]: insert succeed."); 
		response.setParams(resp_param); 
		
		return response;
		 
    }
}
