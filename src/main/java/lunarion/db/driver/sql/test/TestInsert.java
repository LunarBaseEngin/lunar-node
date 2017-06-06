
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

public class TestInsert {

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
		if(l_connection!=null)
		{
			Statement stmt = l_connection.createStatement();
			LResultSet rs = null;
			
			long startTime = 0;
			long endTime = 0;
			 
			int length = 1000;
			long time_per1000=0;
			String[] recs = new String[length]; 
			int i=0;	
			int m=0;
			startTime=System.currentTimeMillis();  
			for(int j=0; j<1; j++) {
						BufferedReader br = null;
						try {
							br = new BufferedReader(new InputStreamReader(new FileInputStream("/home/feiben/EclipseWorkspace/lunarbase-node/corpus/Test_20000line.txt"), "utf-8"));
							String line = null;
							
							while ((line = br.readLine()) != null) {
								
								//System.out.println(StringFilter(line)+i);
								recs[i] = "{content=[\" this is the test content " +line+".\"]}";
								i++;
								if(i%1000==0) { 
									rs = stmt.insert(table, recs);	 
									m++; 
									i=0;
									long time=endTime-startTime;
									System.out.println("*"+time+"*"+"#"+m+"#");
									time_per1000=time_per1000+time;
									System.out.println(time_per1000);
									//continue;
								}
							}
							
						} catch (Exception e) {
						} finally {
							try {
								if (br != null)
									br.close();
							} catch (IOException e) {
							}
						}
			}
				 
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
