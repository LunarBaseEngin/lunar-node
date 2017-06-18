
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;

public class GetColumns implements ExecutorInterface{
	
	 
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD(); 
		
		 return getColumns(l_db_ssa, msg, logger);
	}

	
	private MessageResponse getColumns(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
	{
		 String[] params = (String[])request.getParams();
    	 
		 MessageResponse response = null;
		 if(params.length != 2)
		 {
				System.err.println("[NODE ERROR]: wrong parameters for fetching table columns with given suffix.");
				response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameters_for_feteching_name_with_suffix);
				return response; 
		 }
			String db = params[0];
	        String table = params[1];
	        
	        
			 
	        LunarDB l_DB = l_db_ssa.getDBInstant(db);
	        if(l_DB == null)
	        {  
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
	        	return response;
	        } 
	        
	        Iterator<String> names = l_DB.listTable();
	        if(names == null)
	        {
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.no_table_found);	
	        	return response;
	        }
	        List<String> t_names = new ArrayList<String>();
	        
	        String[] columns_found = null; 
	        while(names.hasNext())
	        {
	        	String t_name = names.next();
	        	if(t_name.equalsIgnoreCase(table))
	        	{
	        		String[] cols = l_DB.getTable(t_name).tableColumns();
	        		columns_found = new String[cols.length * 2];
	        		for(int i=0;i<cols.length;i++)
	        		{
	        			columns_found[i*2] = cols[i];
	        			columns_found[i*2+1] = l_DB.getTable(t_name).columnDataType(cols[i]).toString();
	        		} 
	        	} 
	        }
	        
	        if(columns_found != null)
	        {     
	             response = new MessageResponseForQuery();
	             response.setUUID(request.getUUID());
	             response.setCMD(request.getCMD());
	             response.setSucceed(true);
	             response.setParams(columns_found); 
	             return response;
	        }
	        else
	        {
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.no_column_found);	
	        	return response;
	        }    
	    }
	    
}
