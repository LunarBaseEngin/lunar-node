
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
package lunarion.cluster.coordinator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.node.remote.protocol.MessageResponse;

public class ResponseCollector {
	
	/*
	 * <message UUID, MessageResponse >
	 */
	private ConcurrentHashMap<String, MessageResponse> response_map  ;
	private boolean succeed = true;
	
	public ResponseCollector(ConcurrentHashMap<String, MessageResponse> _map)
	{
		response_map = _map;
		
		Iterator<String> uuids = response_map.keySet().iterator();
		while(uuids.hasNext())
		{
			MessageResponse mr = response_map.get(uuids.next());
			if(mr == null || !mr.isSucceed())
				succeed = false;
		}
		
	}
	
	public boolean isSucceed()
	{
		return succeed;
	}
	
	public ArrayList<String[]> getUpdatedTables()
	{
		if(succeed)
		{
			ArrayList<String[]> updated_tables = new ArrayList<String[]>();
			Iterator<String> key_partitions = response_map.keySet().iterator();
			
			/*
			 * at least has one response.
			 */  
			while(key_partitions.hasNext())
			{
				String partition = key_partitions.next();
				MessageResponse mr = response_map.get(partition);
				if(mr!=null)
				{  
					System.out.println("updated table on partition " +partition+ " is: "+ mr.getParams()[1]);
					 
					String[]  db_and_table = new String[2];	 
					db_and_table[0] = mr.getParams()[0];
					db_and_table[1] = mr.getParams()[1];
					updated_tables.add(db_and_table);
				}  
			}
			
			return updated_tables;
		}
		else
			return null;
	}
	
	public void printResponse()
	{
		Iterator<String> uuids = response_map.keySet().iterator();
		while(uuids.hasNext())
		{
			String uuid = uuids.next();
			MessageResponse mr = response_map.get(uuid);
			if(mr!=null)
			{
				System.out.println( uuid+ " responded: command "+ mr.getCMD());
				System.out.println( uuid+ " responded: UUID "+ mr.getUUID());
				System.out.println( uuid+ " responded: suceed "+ mr.isSucceed());
				for(int i=0;i<mr.getParams().length;i++)
				{
					 System.out.println( uuid+ " responded: "+ mr.getParams()[i]);
				} 
			}
			
		}
		
	}
}
