
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
package lunarion.node.logger;

import java.io.IOException;

import LCG.DB.API.LunarDB;
import LCG.RecordTable.StoreUtile.Record32KBytes;

public class LogTable {
	
	 public static void logAddingFulltextColumn(String db, String table, String col, LunarDB l_db)
	 {
		 String log_table = LogCMDConstructor.getLogTableName(table);
		 String[] recs = new String[1];
		 recs[0] = LogCMDConstructor.contructLogAddingFulltextColumn( db, table, col);
		 Record32KBytes[] recs_inserted = l_db.insertRecord(log_table, recs);
		 try {
	    		l_db.getTable(log_table).markCMDSucceed(recs_inserted[0].getID()); 
	    		l_db.getTable(log_table).save();
		 } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		 }
	       
	 }
	 
	
	 public static void logCreatingTable(String db, String table, String log_table, LunarDB l_db)
	 {
	    	String create_table_cmd = LogCMDConstructor.contructLogCreate(db, table);
	    	String[] recs = new String[1];
	    	recs[0] = create_table_cmd;
	    	Record32KBytes[] recs_inserted = l_db.insertRecord(log_table, recs);
	    	try {
	    		l_db.getTable(log_table).markCMDSucceed(recs_inserted[0].getID()); 
	    		l_db.getTable(log_table).save();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}   
	 }
	  
	 public static void logInsert(String db, String table, String[] recs, LunarDB l_db)
	 {
		 String log_table = LogCMDConstructor.getLogTableName(table);
		 String[] insert_cmd = LogCMDConstructor.contructLogInsert(db, table, recs);
	    	 
	    	Record32KBytes[] recs_inserted = l_db.insertRecord(log_table, insert_cmd);
	    	try {
	    		for(int i=0;i<recs_inserted.length;i++)
	    		{
	    			l_db.getTable(log_table).markCMDSucceed(recs_inserted[i].getID());  
	    		}
	    		
	    		l_db.getTable(log_table).save();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    

}
