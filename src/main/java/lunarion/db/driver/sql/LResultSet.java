
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
package lunarion.db.driver.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.cluster.coordinator.adaptor.converter.RecordConverter;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class LResultSet {

	private LunarDBClient l_client ;
	private RemoteResult r_result;
	
	private String current;
	/*
	 * begin with -1, otherwise, lost the first record
	 */
	private AtomicLong current_rec_id = new AtomicLong(-1);
	  
	 
	private long total_results;
	
	final long maximum_cached = 1024*4;
	final long maximum_cached_mask = (~(maximum_cached-1));
	long cached_from = 0; 
	String[] cached_recs = null;
	int current_cached = 0;
	
	AtomicInteger accumulated_failure = new AtomicInteger(0); 
	final int maximum_failure = 5;
	
	public LResultSet(LunarDBClient _client, RemoteResult _result)
	{
		this.l_client = _client;
		this.r_result = _result;
		
		if(r_result == null)
			total_results = 0;
		else
			total_results = r_result.getResultCount() < maximum_cached ? maximum_cached: r_result.getResultCount();
		 
	}
	
	public String current() throws InterruptedException {
		   
		if(current_rec_id.get() >= cached_from 
				&& current_rec_id.get() < (cached_from + current_cached) 
				&& cached_recs != null)
		{
			String rec = cached_recs[(int)(current_rec_id.get() - cached_from)];
			if(rec != null && !rec.equals(""))
				current = rec;
		}
		else
		{
			cached_from = ((current_rec_id.get())&maximum_cached_mask);
			//result = db_inst.fetchRecords( db_inst.getDBName() , table_name, cached_from, (int)maximum_cached, true);
			cached_recs = r_result.fetchQueryResult(cached_from, (int)maximum_cached);
			
			  	
			if (cached_recs ==null ) 
			{  
				current = null;
				accumulated_failure.incrementAndGet();
				if(accumulated_failure.get() > maximum_failure)
				{
					total_results = 0;
					accumulated_failure.set(0);
					return "";
				}
			}
			else
			{
				current_cached = cached_recs.length;
				if(current_cached == 0 || (current_rec_id.get() - cached_from ) >= current_cached)
				{
					current = null;
					accumulated_failure.incrementAndGet();
					if(accumulated_failure.get() > maximum_failure)
					{
						total_results = 0;
						accumulated_failure.set(0);
						return "";
					}
					return "";
				} 
				
				
				current = cached_recs[(int)(current_rec_id.get() - cached_from)];
				 
			} 
		}
		if(current == null)
			return "";
	    return current;
	  
	}
	public boolean next() 
	{ 
		//return current_rec_id.incrementAndGet() < db_inst.recsCount(table_name);
		return current_rec_id.incrementAndGet() < total_results;
	}
	
	public void close() throws InterruptedException
	{
		if(r_result != null)
			r_result.closeQuery();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
