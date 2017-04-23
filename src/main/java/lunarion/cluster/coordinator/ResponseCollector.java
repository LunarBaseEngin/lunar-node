
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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import lunarion.node.remote.protocol.MessageResponse;

public class ResponseCollector {
	
	/*
	 * <partition_name, MessageResponse >
	 */
	private ConcurrentHashMap<String, MessageResponse> response_map  ;
	private boolean succeed = false;
	
	public ResponseCollector(ConcurrentHashMap<String, MessageResponse> _map)
	{
		response_map = _map;
		
		Iterator<String> key_partitions = response_map.keySet().iterator();
		while(key_partitions.hasNext())
		{
			MessageResponse mr = response_map.get(key_partitions.next());
			if(mr == null || !mr.isSucceed())
				succeed = false;
		}
		
	}
	
	public boolean isSucceed()
	{
		return succeed;
	}
	
	public void printResponse()
	{
		Iterator<String> key_partitions = response_map.keySet().iterator();
		while(key_partitions.hasNext())
		{
			String partition = key_partitions.next();
			MessageResponse mr = response_map.get(partition);
			if(mr!=null)
			{
				System.out.println("partition " +partition+ " responded: command "+ mr.getCMD());
				System.out.println("partition " +partition+ " responded: UUID "+ mr.getUUID());
				System.out.println("partition " +partition+ " responded: suceed "+ mr.isSucceed());
				for(int i=0;i<mr.getParams().length;i++)
				{
					 System.out.println("partition " +partition+ " responded: "+ mr.getParams()[i]);
				} 
			}
			
		}
		
	}
}
