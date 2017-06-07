
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
package lunarion.db.driver.sql.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DriverManager;
import java.sql.ResultSet;

import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.db.driver.sql.LDriverManager;
import lunarion.db.driver.sql.LResultSet;
import lunarion.db.driver.sql.LunarDBConnection;
import lunarion.db.driver.sql.Statement;

public class TestCreateTable {

	public static void main(String[] args) {
		String url = "jdbc:lunarion:thin:@localhost:60001:RTSeventhDB";
		
		String user = "";
		String pw = "";
		LunarDBConnection l_connection = null;
		try {
			l_connection = LDriverManager.getConnection(url, user, pw);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		String table = "node_table"; 
		String column1 = "content";
		String column2 = "name";
		String column3 = "age";
		String column4 = "product";
		String column5 = "price";
		String column6 = "time";
		String column7 = "unknowncolumn";
		
		
		
		if(l_connection!=null)
		{
			Statement stmt = l_connection.createStatement();
			LResultSet rs = null;
			try {
				stmt.createTable(table);
				
				rs = stmt.addFulltextColumn(table, column1);
				rs.printParams();
				System.out.println("====================================");
				rs = stmt.addAnalyticColumn(table, column2, "varchar");
				rs.printParams();
				System.out.println("====================================");
				
				rs = stmt.addAnalyticColumn(table, column3, "long");
				rs.printParams();
				System.out.println("====================================");
				
				rs = stmt.addAnalyticColumn(table, column4, "varchar");
				rs.printParams();
				System.out.println("====================================");
				
				rs = stmt.addAnalyticColumn(table, column5, "long");
				rs.printParams();
				System.out.println("====================================");
				
				rs = stmt.addAnalyticColumn(table, column7, "unkowntypeeeeeeeee");
				rs.printParams();
				System.out.println("====================================");
				
				/*
				 * translate time to long.
				 */
				rs = stmt.addAnalyticColumn(table, column6, "long");
				rs.printParams();
				System.out.println("====================================");
				
				
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			long startTime = 0;
			long endTime = 0;
			 
			 
				 
			endTime=System.currentTimeMillis();  
				 
			while(rs.next())
			{
				try {
					System.out.println(rs.current());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			 
			System.out.println("sql query totally costs: " + (endTime -startTime) + " ns");
			try {
				 /*
				  * have to close to release server resource.
				  */
				rs.close();
			} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
			 
			l_connection.close();
	 
		}
	}

}
