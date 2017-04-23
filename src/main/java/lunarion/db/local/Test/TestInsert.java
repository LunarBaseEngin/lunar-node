package lunarion.db.local.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.db.local.shell.CMDEnumeration; 

public class TestInsert {

	public static void main(String[] args) throws InterruptedException {
		
		String db_root = "/home/feiben/DBTest/LunarNode/CorpusDB";
		
		LunarDB l_db = LunarDB.getInstance();
		//String table = "profile_one_tread"; 
		String table = "textTable"; 
		
		String column = "content"; 
		
		l_db.openDB(db_root);  
			
			if(!l_db.hasTable(table))
			{	
				l_db.createTable(table); 
				l_db.openTable(table);
				LunarTable tt = l_db.getTable(table);
				//tt.addSearchable("string", "name");
				tt.addFulltextSearchable(column);
			} 
			
			LunarTable t_table = l_db.getTable(table);
			TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
			t_table.registerTokenizer(t_e);
        	
			for(int i = 0;i<300;i++)
	    	{  
	    		 
	    		String[] params = new String[3]; 
	    		params[0] = "{content=[\" this is the test content " + i+".\"]}";
	    		params[1] = "{content=[\" this is the test content " + (i*10)+".\"]}";
	    		params[2] = "{content=[\" this is the test content " + (i*100)+".\"]}";
	    		Record32KBytes[] results = l_db.insertRecord(table, params);
       		  	
	    	 
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
