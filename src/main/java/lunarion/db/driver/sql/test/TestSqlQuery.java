
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

import java.sql.DriverManager;
import java.sql.ResultSet;

import lunarion.db.driver.sql.LDriverManager;
import lunarion.db.driver.sql.LResultSet;
import lunarion.db.driver.sql.LunarDBConnection;
import lunarion.db.driver.sql.Statement;

public class TestSqlQuery {

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
		
		if(l_connection!=null)
		{
			Statement stmt = l_connection.createStatement();
			
			 String query0 = "select \"content\" from \"node_table\" as S where S.\"content\" like '大家' "; 
			 String query1 = "select \"content\" from \"node_table\" as S ";

			 LResultSet rs = null;
			 long startTime = 0;
			 long endTime = 0;
			 try {
				 startTime=System.currentTimeMillis();  
				 rs = stmt.executeQuery(query0);
				 endTime=System.currentTimeMillis();  
				 
				
			 } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			 }
			 
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
