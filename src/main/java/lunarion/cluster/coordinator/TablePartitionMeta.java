
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
import java.util.concurrent.atomic.AtomicLong;

import lunarion.node.utile.ControllerConstants;

public class TablePartitionMeta {

	 
	Properties prop ; 
	//FileOutputStream o_file ; 
	RandomAccessFile o_file;
	AtomicInteger rec_count_in_current_partition ;
	AtomicLong total_rec_count ;
	
	
	String latest_partition ;  /* =table_name + \"_\" + latest_partition_number*/
	String table_name;
	AtomicInteger latest_partition_number;
	
	final String prop_current_partition = "CURRENT_PARITION"; 
	final String prop_rec_in_current_partition = "REC_COUNT_IN_CURRENT";
	final String prop_totla_rec = "TOTAL_RECS";
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
			latest_partition = prop.getProperty(prop_current_partition) ;//正在使用的分区
			String count_in_current_partition = prop.getProperty(prop_rec_in_current_partition);
			String total_recs = prop.getProperty(prop_totla_rec);
			
			if(latest_partition != null && count_in_current_partition != null && total_recs != null)
			{
				latest_partition = latest_partition.trim();
				table_name = ControllerConstants.parseTableName(latest_partition);
				latest_partition_number = new AtomicInteger(ControllerConstants.parsePartitionNumber(latest_partition));
				
				rec_count_in_current_partition = new AtomicInteger(0);
				rec_count_in_current_partition.set(Integer.parseInt(count_in_current_partition.trim()));//分区中记录数
				
				total_rec_count = new AtomicLong(0);
				
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
	
	private String[] makeProperties(String partition, int count_in_current, long total_recs)
	{
		prop.setProperty(prop_current_partition, latest_partition);
    	prop.setProperty(prop_rec_in_current_partition, rec_count_in_current_partition +"");  
    	prop.setProperty(prop_totla_rec, total_rec_count +"");  
    	
    	
		String[] pp = new String[3];
		pp[0] = prop_current_partition + "=" + partition + "\r\n";
		pp[1] = prop_rec_in_current_partition + "=" + count_in_current + "\r\n"; 
		pp[2] = prop_totla_rec + "=" + total_recs + "\r\n"; 
		
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
		total_rec_count = new AtomicLong(0);  
		
		//prop.setProperty("CURRENT_PARITION", latest_partition);
    	//prop.setProperty("REC_COUNT", rec_count_in_current_partition +"");  
    	//prop.store(o_file, "properties updated");
    	//o_file.flush();
    	
    	String[] prop = makeProperties(latest_partition, rec_count_in_current_partition.get(), total_rec_count.get())  ;
    	o_file.setLength(0);
    	//o_file.seek(0);
    	o_file.write(prop[0].getBytes());
    	o_file.write(prop[1].getBytes());
    	o_file.write(prop[2].getBytes());
	}
	
	public void updateMeta(int _latest_partition_number, AtomicInteger rec_count_in_latest_partition, long total_recs) throws IOException
	{
		latest_partition = ControllerConstants.patchNameWithPartitionNumber(table_name, _latest_partition_number) ; 
	
		rec_count_in_current_partition.set(rec_count_in_latest_partition.get());   
		
		latest_partition_number.set(_latest_partition_number) ;
		 
		total_rec_count.set(total_recs );  
    	//prop.store(o_file, "properties updated");
    	//o_file.flush();
    	
    	String[] prop = makeProperties(latest_partition, rec_count_in_current_partition.get(), total_recs ) ;
    	o_file.setLength(0);
    	//o_file.seek(0);
    	o_file.write(prop[0].getBytes());
    	o_file.write(prop[1].getBytes());
    	o_file.write(prop[2].getBytes());
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
	
	public AtomicLong getTotalRecs()
	{
		return this.total_rec_count;
	}
	
	public void printMeta()
	{ 
		System.out.println(prop_current_partition + ":" + prop.getProperty(prop_current_partition));
		System.out.println(prop_rec_in_current_partition + ":" + prop.getProperty(prop_rec_in_current_partition)); 
		System.out.println(prop_totla_rec + ":" + prop.getProperty(prop_totla_rec));  
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
		tm.updateMeta(122200, new AtomicInteger(1), 10000000 );
		tm.printMeta();

	}

}
