
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
package LCG.DB.Local.NLP.WordSeg;

import java.io.IOException;
import java.util.ArrayList;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.RecordTable.StoreUtile.Record32KBytes;

public class TestQueryCorpusDB {
	
	public static void main(String[] args) throws IOException{
	String corpus_db = "/home/feiben/DBTest/LunarNode/CorpusDB";
	
	
	LunarDB l_db = LunarDB.getInstance();
	String table = "text";  
 	String column = "content";
	l_db.openDB(corpus_db); 

	try {
			long start_time = System.nanoTime(); 
			
			FTQueryResult qr = l_db.queryFullText(table, column,"社会",0);
			
			long end_query_id_time = System.nanoTime(); 
			
			if(qr.resultCount() <= 0)
			{
				System.out.println("no results found");
				l_db.closeDB();
				return;
			} 
			
			ArrayList<Record32KBytes> recs = qr.fetchRecords( );
			
			long end_time = System.nanoTime();
			double duration_get_id  = (end_query_id_time - start_time) / 1000000.0 ;
			 
			double duration_ms  = (end_time - start_time) / 1000000.0 ;
		 
			for(int i=0 ;i<recs.size();i++)
			{
				System.out.println(recs.get(i).getID() + ": "+recs.get(i).recData());
			}
			
			System.out.println("full text search has records: " + qr.resultCount());
			System.out.println("Getting ids costs "+ duration_get_id + " (ms)");
			System.out.println("Fetching records costs "+ duration_ms + " (ms)");
			
			
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
