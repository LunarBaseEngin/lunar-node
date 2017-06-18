
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

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.utile.ControllerConstants;

public class NotifySlavesUpdate implements ExecutorInterface{

	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD(); 
		
		 return notifySlavesUpdate(l_db_ssa, msg, logger);
	}
	
	private MessageResponse notifySlavesUpdate(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
    {
		String[] params = (String[])request.getParams();
    	 
		MessageResponse response = null;
		
    	boolean suc = true;
		if(params.length < 2)
		{
			System.err.println("[NODE ERROR]: " + CodeSucceed.wrong_parameters_for_notifying_update);
			logger.info("[NODE ERROR]: " + CodeSucceed.wrong_parameters_for_notifying_update );
			
			suc = false ;
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameters_for_notifying_update);
			return response;
		} 

		String db_name = params[0];
		String table = params[table_name_index];
		 
		int partition = ControllerConstants.parsePartitionNumber(table);
		if(partition >=0 )
		{
			String partition_name = ControllerConstants.patchNameWithPartitionNumber(db_name,partition);
			
			System.err.println("[NODE INFO]: notify slaves.........");
			
			l_db_ssa.notifyUpdate(partition_name, params);
			System.err.println("[NODE INFO]: notification ok.........");
			response = new MessageResponse();
			response.setUUID(request.getUUID());
			response.setCMD(request.getCMD());
			response.setSucceed(suc); 
			String[]  resp = new String[3];
			resp[0] = params[0];
			resp[1] = params[1];
			resp[3] = CodeSucceed.notify_slaves_succeed;
			response.setParams(resp);  
			return response;
		}
		response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.notify_slaves_failed);
		return response;
    }
   
   
}
