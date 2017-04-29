
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
package lunarion.node.requester.test.server;

import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.LunarNode;
import lunarion.node.logger.LoggerFactory;

public class StartDBServer {
	public static void main(String[] args) throws Exception {
        int port = 9090;
        if (args != null && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {

            }
        }
        
        String svr_root = "/home/feiben/DBTest/LunarNode/";
		 
        LunarDBServerStandAlone lsa = new LunarDBServerStandAlone();
        lsa.startServer(svr_root, LoggerFactory.getLogger("LunarNode")); 
        try {
        	lsa.bind(port);
        } finally { 
        	lsa.closeServer(); 
        }
    }
}
