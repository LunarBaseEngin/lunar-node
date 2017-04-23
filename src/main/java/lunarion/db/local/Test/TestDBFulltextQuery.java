
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
package lunarion.db.local.Test;

import java.io.IOException;
import java.util.ArrayList;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.RecordTable.StoreUtile.Record32KBytes;

public class TestDBFulltextQuery {

	public static void main(String[] args) throws Exception {
		
	String db_root = "/home/feiben/DBTest/LunarNode/CorpusDB";
	
	LunarDB l_db = LunarDB.getInstance();
	//String table = "profile_one_tread"; 
	String table = "textTable"; 
	
	String column = "content"; 
	 
	l_db.openDB(db_root);  
		 
		try {
			
			//String query = "purchases";
			 
			String query = column + " against(\" content \")";
			 
			int latest =  0;
			 
			long start_time = System.nanoTime(); 
			
			FTQueryResult result = l_db.queryFullText(table, query , latest); 
			
			long end_time = System.nanoTime();
		 	 
			double duration_ms  = (end_time - start_time) / 1000000.0 ;
			//ArrayList<Record32KBytes> recs = result.fetchRecords();
			ArrayList<Record32KBytes> recs = result.fetchRecords(1,280 );
			for(int i=0 ;i<recs.size()  ;i++)
			{
				if(recs.get(i).getID() == 724 
						|| recs.get(i).getID() == 725
						|| recs.get(i).getID() == 768
						|| recs.get(i).getID() == 769
						|| recs.get(i).getID() == 770)
					System.err.println("attention");
				
				System.out.println(recs.get(i).getID() + ": "+recs.get(i).recData());
			}
			
			System.err.println("full text search has records: " + result.resultCount());
			System.err.println("query costs "+ duration_ms + " (ms)");
			 
			
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	 
		if(l_db!=null)
		{
			try {
				l_db.save();
				l_db.closeDB();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 

	} 
}
