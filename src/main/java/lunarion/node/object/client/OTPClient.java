
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
package lunarion.node.object.client;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

/*
 * object transfer protocol
 */
public class OTPClient {
	/*
	 * params:
	 * server_ip: the server ip address;
	 * server_port: the port that the server response;
	 * file_save_to: where to save the file that downloaded from the server.
	 */
    public void Get(String server_ip, int server_port, String file_save_to) throws Exception {  
        Socket socket = new Socket();  
        // 建立连接  
        socket.connect(new InetSocketAddress(server_ip, server_port));  
        // 获取网络流  
        OutputStream out = socket.getOutputStream();  
        InputStream in = socket.getInputStream();  
        // 文件传输协定命令  
        byte[] cmd = "get".getBytes();  
        out.write(cmd);  
        out.write(0);// 0分隔符  
        int startIndex = 0;  
        /*
         * the file needs to be write to 
         */
        File file = new File(file_save_to);  
        if(file.exists()){  
            startIndex = (int) file.length();  
        }  
        System.out.println("[INFO]: Client startIndex : " + startIndex);  
        // 文件写出流  
        RandomAccessFile access = new RandomAccessFile(file,"rw");  
        // 断点  
        out.write(String.valueOf(startIndex).getBytes());  
        out.write(0);  
        out.flush();  
        // 文件长度  
        int temp = 0;  
        StringWriter sw = new StringWriter();  
        while((temp = in.read()) != 0){  
            sw.write(temp);  
            sw.flush();  
        }  
        int length = Integer.parseInt(sw.toString());  
        System.out.println("Client fileLength : " + length);  
         
        byte[] buffer = new byte[1024*10];  
        /*
         * length remainder  
         */
        int total = length - startIndex;  
        //  
        access.skipBytes(startIndex);  
        while (true) {  
            /*
             * if remains 0, then finish 
             */
            if (total == 0) {  
                break;  
            }  
            /*
             * this time read the remainder 
             */
            int len = total;  
            /*
             * if bigger than the buffer size.  
             */
            if (len > buffer.length) {   
                len = buffer.length;  
            }  
            /*
             * read the file, gets the real length.  
             */
            int rlength = in.read(buffer, 0, len);  
            // 将剩余要读取的长度减去本次已经读取的  
            total -= rlength;  
            // 如果本次读取个数不为0则写入输出流，否则结束  
            if (rlength > 0) {  
                // 将本次读取的写入输出流中  
                access.write(buffer, 0, rlength);  
            } else {  
                break;  
            }  
            System.out.println("finish : " + ((float)(length -total) / length) *100 + " %");  
        }  
        System.out.println("finished!");  
        // 关闭流  
        access.close();  
        out.close();  
        in.close();  
    }  
  
    public static void main(String[] args) {  
        OTPClient client = new OTPClient();  
        try {  
            client.Get("127.0.0.1", 8888, "/home/feiben/DBTest/TheMatrix.downloaded");  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
}  