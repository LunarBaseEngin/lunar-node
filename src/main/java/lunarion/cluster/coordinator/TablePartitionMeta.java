
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import lunarion.node.utile.ControllerConstants;

public class TablePartitionMeta {

	 
	Properties prop ; 
	//FileOutputStream o_file ; 
	RandomAccessFile o_file;
	AtomicInteger rec_count_in_current_partition ;
	
	String latest_partition ;  /* =table_name + \"_\" + latest_partition_number*/
	String table_name;
	AtomicInteger latest_partition_number;
	public TablePartitionMeta()
	{
		
	}
	
	public void loadMeta(String _meta_file_name ) 
	{
		prop = new Properties();    
		try {
			BufferedInputStream inn = new BufferedInputStream (new FileInputStream(_meta_file_name)); 
			prop.load(inn); 
			inn.close();
			latest_partition = prop.getProperty("CURRENT_PARITION") ;//正在使用的分区
			String count = prop.getProperty("REC_COUNT");
			if(latest_partition != null && count != null)
			{
				latest_partition = latest_partition.trim();
				table_name = ControllerConstants.parseTableName(latest_partition);
				latest_partition_number = new AtomicInteger(ControllerConstants.parsePartitionNumber(latest_partition));
				
				rec_count_in_current_partition = new AtomicInteger(0);
				rec_count_in_current_partition.set(Integer.parseInt(count.trim()));//分区中记录数
				
				//o_file = new FileOutputStream(_meta_file_name, false);/* true for appending mode */
				o_file = new RandomAccessFile(_meta_file_name, "rw");  
				prop.setProperty("CURRENT_PARITION", latest_partition);
		    	prop.setProperty("REC_COUNT", rec_count_in_current_partition +"");  
		    	
		    	//String[] prop = makeProperties(latest_partition, rec_count_in_current_partition.get())  ;
		    	
		    	//o_file.seek(0);
		    	//o_file.write(prop[0].getBytes());
		    	//o_file.write(prop[1].getBytes());
		    	
		    	//prop.store(o_file, "properties updated");
		    	//o_file.flush();
			}
			
			 
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// TODO verify each partition to get the latest information and fill up the 4 maps in this class.			
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String[] makeProperties(String partition, int count)
	{
		prop.setProperty("CURRENT_PARITION", latest_partition);
    	prop.setProperty("REC_COUNT", rec_count_in_current_partition +"");  
    	
    	
		String[] pp = new String[2];
		pp[0] = "CURRENT_PARITION" + "=" + partition + "\r\n";
		pp[1] = "REC_COUNT" + "=" + count + "\r\n"; 
		return pp;
    	
	}
	public void createMeta(String _meta_file_name, String _table) throws IOException
	{
		prop = new Properties();    
		File file = new File(_meta_file_name);  
        if(file.exists()) { 
        	//o_file = new FileOutputStream(_meta_file_name, false);/* true for appending mode */ 
        	o_file.close();
        	o_file = new RandomAccessFile(_meta_file_name, "rw");  
			
        }
        else
        {
        	file.createNewFile();
        	//o_file = new FileOutputStream(_meta_file_name, false); 
        	o_file = new RandomAccessFile(_meta_file_name, "rw");  
			
        }
		
			 
		latest_partition = ControllerConstants.patchNameWithPartitionNumber(_table, 0) ; 
		table_name = _table;
		latest_partition_number = new AtomicInteger(0); 
		rec_count_in_current_partition = new AtomicInteger(0);  
		
		//prop.setProperty("CURRENT_PARITION", latest_partition);
    	//prop.setProperty("REC_COUNT", rec_count_in_current_partition +"");  
    	//prop.store(o_file, "properties updated");
    	//o_file.flush();
    	
    	String[] prop = makeProperties(latest_partition, rec_count_in_current_partition.get())  ;
    	o_file.setLength(0);
    	//o_file.seek(0);
    	o_file.write(prop[0].getBytes());
    	o_file.write(prop[1].getBytes());
	}
	
	public void updateMeta(int _latest_partition_number, AtomicInteger rec_count_in_latest_partition) throws IOException
	{
		latest_partition = ControllerConstants.patchNameWithPartitionNumber(table_name, _latest_partition_number) ; 
	
		rec_count_in_current_partition.set(rec_count_in_latest_partition.get());   
		
		latest_partition_number.set(_latest_partition_number) ;
		 
    	  
    	//prop.store(o_file, "properties updated");
    	//o_file.flush();
    	
    	String[] prop = makeProperties(latest_partition, rec_count_in_current_partition.get())  ;
    	o_file.setLength(0);
    	//o_file.seek(0);
    	o_file.write(prop[0].getBytes());
    	o_file.write(prop[1].getBytes());
	}
	public int getLatestPartitionNumber()
	{
		return this.latest_partition_number.get();
	}
	
	public String getLatestPartition()
	{
		return this.latest_partition;
	}
	public String getTableName()
	{
		return this.table_name;
	}
	
	public AtomicInteger getRecCountInCurrentPartition()
	{
		return this.rec_count_in_current_partition;
	}
	
	public void printMeta()
	{ 
		System.out.println("CURRENT_PARITION" + ":" + prop.getProperty("CURRENT_PARITION"));
		System.out.println("REC_COUNT" + ":" + prop.getProperty("REC_COUNT"));
	}
	public void close()
	{ 
			try {
				//o_file.flush();
				o_file.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	}
	public static void main(String[] args) throws IOException {
		String meta = "/home/feiben/DBCluster/CoordinatorMeta/copy of node_table.meta.info";
		
		TablePartitionMeta tm = new TablePartitionMeta();
		tm.loadMeta(meta);
		tm.printMeta();
		
		System.out.println("after update ===================" );
		tm.updateMeta(122200, new AtomicInteger(1));
		tm.printMeta();

	}

}
