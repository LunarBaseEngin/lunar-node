
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
package Lunarion.Node.Coordinator;

import java.io.IOException;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;

public class PrepareNode {
	
	public static void main(String[] args) throws IOException {	
		String creation_conf = "/home/feiben/EclipseWorkspace/lunarbase-node/conf/creation.conf";
	
		/*
		 * this db_root is from the above creation_conf.
		 */
		String db_root = "/home/feiben/DBTest/LunarNode/";
		String db_name = "CorpusDB";
		LunarDB l_db = new LunarDB();	 
		l_db.createDB(db_root, creation_conf); 
		
		 
		l_db.openDB(db_root+db_name);
		String table = "textTable";
		String column = "content";
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
		 
			
		l_db.closeDB();
		 
	}
}
