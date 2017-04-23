
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
package lunarion.cluster.utile;

import java.util.concurrent.atomic.AtomicLong;

public class ControllerConstants {
	
	public static final String STATE_MODEL_NAME = "LunarDBStateModel";
	
	private int max_records_per_partition = 1024;
	private int current_partition_for_inserting = 0;
	private AtomicLong max_global_rec_id = new AtomicLong(0);
	
	public int getMaxRecsPerPartition()
	{
		return this.max_records_per_partition;
	}
	
	public long increaseGlobalRecID(int num)
	{
		max_global_rec_id.addAndGet(num);
		current_partition_for_inserting = (int)(max_global_rec_id.get() % max_records_per_partition);
		
		return max_global_rec_id.get();
	}
	
	public static int parsePartitionNumber(String partition_name)
	{
		String[] arr = partition_name.split("_");
		try
		{
			int num = Integer.parseInt(arr[arr.length-1]);
			return num;
		}
		catch(Exception e)
		{
			return -1;
		}
		
	}
	
	public static String patchNameWithPartitionNumber(String name, int partition_number)
	{
		return name+"_"+partition_number;
		
	}

}
