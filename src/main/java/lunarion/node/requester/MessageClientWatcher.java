
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
package lunarion.node.requester;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class MessageClientWatcher {
	 
	private final String message_uuid;
    private MessageResponse response;
    private Lock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    public MessageClientWatcher(String uuid) {
    	message_uuid = uuid;
    }

    public MessageResponse start() throws InterruptedException {
        try {
            lock.lock(); 
            /*
             * If exceeds the threshold of time waiting,  
             * returns null; 
             */
           // condition.await(60*1000, TimeUnit.MILLISECONDS);
            condition.await();
            System.out.println("interrupted by finish(...)");
            
            //System.out.println(this.response.getParams()[1]); 
            return this.response ;
            
        } finally {
            lock.unlock();
        }
    }

    public void finish(MessageResponse reponse) {
        try {
            lock.lock();
            this.response = reponse;
            /*
             * notify to interrupt the await() called in start() 
             */
            condition.signal();
            System.out.println("send signal");
        } finally {
            lock.unlock();
        }
    }
}
