
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

import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
 

public class LoggerFactory {
	
	 
	public static Logger getLogger(String name)
	{ 
		Logger g_logger = Logger.getLogger(name); 
		
		SimpleLayout layout = new SimpleLayout();
		 
    	FileAppender fa = null;
		try {
			fa = new  FileAppender(layout, name+".log.txt", true);
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		g_logger.addAppender(fa);
		
		return g_logger;
	}

}
