
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
package lunarion.node.replicator;

import java.util.concurrent.atomic.AtomicLong;

import LCG.DB.API.LunarDB;
import lunarion.node.requester.LunarDBClient;

public class DBReplicator {
	
	private AtomicLong in_replication = new AtomicLong(0);
	private final LunarDB db_inst;
	private final LunarDBClient client;
	
	public DBReplicator(LunarDB _db_instance)
	{
		this.db_inst = _db_instance;
		client = new LunarDBClient();
	}
	
	public void startReplicatingFromMaster(String ip, int port, int partition) throws Exception
	{
		
		while(in_replication.get() >0)
			;
		in_replication.incrementAndGet();
		try
		{
			client.connect(ip, port);
			
		}
		finally
		{
			in_replication.decrementAndGet();
		}
		
	}
	
	public void close()
	{
		
	}

}
