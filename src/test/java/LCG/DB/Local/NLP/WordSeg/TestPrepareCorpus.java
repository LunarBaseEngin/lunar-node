
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.DB.Local.NLP.Word2Vex.Word2Vec;

public class TestPrepareCorpus {

	 public static void loadCorpus(String corpus_file,  
				LunarDB corpus_db,
				String corpus_table, 
				String corpus_column){


		 Word2Vec wv = new Word2Vec.Factory()
				 		.setMethod(Word2Vec.Method.skip_gram)
				 		.setNumOfThread(1)
				 		.setDBConnection(corpus_db, corpus_table, corpus_column)
				 		.build();

		 try (BufferedReader br =
				 new BufferedReader(new FileReader(corpus_file))) {
			 
			 TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
			 int line_count = 0;
			 
			 int iter = 10000;	
			 int count = 0;
			 String[] lines = new String[iter];
			 for (String line = br.readLine(); line != null; line = br.readLine())
			 {
				 if(count < iter)
				 {
					 lines[count] = line; 
					 count++;
				 }
				 else
				 { 
					 wv.prepareData(t_e, lines );
					 count = 0;
					 lines = new String[iter];
					 System.out.println("Line " + line_count + " loaded."); 
				 }
				 line_count++;
			 }

		 } catch (IOException ioe) {
			 ioe.printStackTrace();
		 }

		 wv.commitData(); 
		 
	}

	
	public static void main(String[] args) throws IOException {
		
		String corpus_file = "/home/feiben/EclipseWorkspace/lunarbase-node/corpus/corpus_porn.txt";
		     
		String creation_conf = "/home/feiben/EclipseWorkspace/lunarbase-node/conf/creation.conf";
	    	
		/*
		 * this corpus_db is from the above creation_conf.
		 */
		String corpus_db_name = "CorpusDB";
		String corpus_db_root = "/home/feiben/DBTest/LunarNode/";
		String table = "text";
		String column = "content";
		
		LunarDB l_db = new LunarDB();	 
		l_db.createDB(corpus_db_root, creation_conf);  
			 
		l_db.openDB(corpus_db_root, corpus_db_name);
		if(!l_db.hasTable(table))
		{	
			l_db.createTable(table);  
			l_db.openTable(table); 
		}
			
		LunarTable tt = l_db.getTable(table);
			 
		/*
		 * add a fulltext searchable column
		 */
		tt.addFulltextSearchable(column);
		TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
		tt.registerTokenizer(column, t_e); 
			
		loadCorpus(corpus_file, l_db, table, column);
		
		l_db.save();
		l_db.closeDB();

	}

}
