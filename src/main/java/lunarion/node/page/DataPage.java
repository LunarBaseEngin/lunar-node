
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
package lunarion.node.page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class DataPage {
	
	public static final int data_page = 1024;// 2^10
	public static final int data_page_bit_len = 10 ;// 2^10 
	public static final int data_page_mask = (~(data_page-1));

	/*
	 * low efficient, even for merely 400 million integers, because of both HashMap and ArrayList. 
	 * calc the page level is quick, takes 2.3 ms for 400 million integers.
	 */
	static public HashMap calcDataPages(int[] rec_ids)
	{
		HashMap<Integer, ArrayList<Integer>> page_leve_ids = new HashMap<Integer, ArrayList<Integer>>();
		for(int i=0;i<rec_ids.length;i++)
		{
			int page_level = (rec_ids[i] & data_page_mask) >> data_page_bit_len;
		 	ArrayList<Integer> level_id_array = page_leve_ids.get(page_level);
		 	if(level_id_array == null)
		 	{
		 		level_id_array = new ArrayList<Integer>();
		 		page_leve_ids.put(page_level, level_id_array);
		 	}
		 	
		 	level_id_array.add(rec_ids[i]);
		 	
		}
		
		return page_leve_ids;
	}
	
	static public int[] calcPagesRecs(int[] rec_ids, int max_value)
	{
		 int max_level = (max_value & data_page_mask) >> data_page_bit_len;
		 int[] levels = new int[max_level+1];
		for(int i=0;i<rec_ids.length;i++)
		{
			int page_level = (rec_ids[i] & data_page_mask) >> data_page_bit_len;
			levels[page_level] ++;
		}
		
		return levels;
	}
	
	public static void main(String[] args)
	{
		int size = 1<< 21;
		int[] id_array = new int[size];
		for(int i=0;i<size;i++)
			id_array[i] = i;
		
		long start = System.nanoTime();
		for(int j = 0; j< size;j++)
		{
			int page_level = (id_array[j] & data_page_mask) >> data_page_bit_len; 
		}
		
		long end = System.nanoTime();
		System.err.println("total time for simply calc page level for " + size + " elements is: " + (end-start) + " ns");
		
		start = System.nanoTime();
		int[] count = DataPage.calcPagesRecs(id_array, size-1);
		end = System.nanoTime();
		
		for(int k=0;k<count.length;k++)
		{
			//System.out.println(count[k]);
		}
		System.err.println("total time for calcPagesRecs of " + size + " elements is: " + (end-start) + " ns");
		
		/*
		start = System.nanoTime();
		HashMap<Integer, ArrayList<Integer>> page_leve_ids 
					= DataPage.calcDataPages(id_array);
		
		end = System.nanoTime();
		
		
		levels = page_leve_ids.keySet().iterator();
		while(levels.hasNext())
		{
			levels.next();
			//System.out.println(page_leve_ids.get(levels.next()).size());
		}
		
		System.err.println("total time for calculating " + size + " elements is: " + (end-start) + " ns");
		*/
	}
}
