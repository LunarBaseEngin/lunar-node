
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
package lunarion.node.remote.protocol;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.requester.LunarDBClient;

public class RemoteResult  {
	
	private final LunarDBClient client;
	private final  MessageResponse message;
	 
	public RemoteResult(LunarDBClient _client, MessageResponse _mess)
	{
		client =_client;
		message = _mess;
	}
	/*
	 * never fetch a count greater than the maximum value of an integer.
	 */
	public  String[] fetchQueryResult( long from, int count) throws InterruptedException
	{
		if(message.getCMD() == command.ftQuery || message.getCMD() == command.rgQuery 
												|| message.getCMD() == command.ptQuery
												|| message.getCMD() == command.filterForWhereClause
												|| message.getCMD() == command.sqlSelect)
		{
			 String[] params = new String[5];
			 
			 params[0] = this.message.getParams()[0]; /* db name */
			 params[1] = this.message.getParams()[1]; /* table name */
			 params[2] = this.message.getParams()[2]; /* intermediate query result uuid */
			 params[3] = ""+from;
			 params[4] = ""+count;
			 
			 MessageResponse resp_from_svr = client.internalQuery(command.fetchQueryResultRecs, params, 5*1000);
			 if(resp_from_svr == null)
				 return null;
			 
			 return resp_from_svr.getResultRecords(0, count); 
			
		}
		if(message.getCMD() == command.fetchQueryResultRecs || message.getCMD() == command.fetchRecordsDESC
														|| message.getCMD() == command.fetchRecordsASC)
		{
			/*
			 * since we never fetches records more than the integer.maxVal, here transfer long to int directly
			 */
			if(from > Integer.MAX_VALUE)
				return null;
			
			return message.getResultRecords((int)from, count);
		}
		
		
		return null;
	}
	
	public MessageResponse closeQuery() throws InterruptedException
	{
		if(message.getCMD() == command.ftQuery || message.getCMD() == command.rgQuery 
				|| message.getCMD() == command.ptQuery
				|| message.getCMD() == command.filterForWhereClause
				|| message.getCMD() == command.sqlSelect)
		{
			String[] params = new String[3];
			 
			params[0] = this.message.getParams()[0]; /* db name */
			params[1] = this.message.getParams()[1]; /* table name */
			params[2] = this.message.getIntermediateResultUUID() ; /* intermediate query result uuid */
		 
			MessageResponse resp_from_svr = client.internalQuery(command.closeQueryResult, params, 5* 1000);
			return resp_from_svr ; 
		}
		
		return null;
	}
	
	public int getResultCount()
	{  
		return message.getResultCount();
	}
	
	public command getCMD()
	{
		return message.getCMD();
	}
	
	public String getUUID()
	{
		return message.getUUID();
	}
	
	public boolean isSucceed()
	{
		return message.isSucceed();
	}
	
	public String[] getParams()
	{
		return message.getParams();
	}
	
	public String[] getResultRecords(int from, int count)
	{
		return message.getResultRecords(from, count);
	}
	
	public String getIntermediateResultUUID()
	{
		return message.getIntermediateResultUUID();
	}
	
	public String getTableName()
	{ 
		return message.getTableName();
	}
	public String getDBName()
	{
		return message.getDBName() ;
	}
	
	public int[] getRecCountInEveryPage()
	{
		return message.getRecCountInEveryPage();
	}
}
