
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
package lunarion.node.object.server;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * object transfer protocol
 */
public class OTPServer {  
    
    /*
     * the thread sending file to clients.  
     */
    class Sender extends Thread{  
        // 网络输入流  
        private InputStream in;  
        // 网络输出流  
        private OutputStream out;  
        // 下载文件名  
        private String filename;  
  
        /*
         * file_on_svr: file on the server that will be sending to the client
         */
        public Sender(String file_on_svr, Socket socket){  
            try {  
                this.out = socket.getOutputStream();  
                this.in = socket.getInputStream();  
                this.filename = filename;  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
        }  
          
        @Override  
        public void run() {  
            try {  
                System.out.println("[INFO]: start to download file!");  
                int temp = 0;  
                StringWriter sw = new StringWriter();  
                while((temp = in.read()) != 0){  
                    sw.write(temp);  
                    //sw.flush();  
                }  
                // 获取命令  
                String cmds = sw.toString();  
                System.out.println("cmd : " + cmds);  
                if("get".equals(cmds)){  
                    // 初始化文件  
                    File file = new File(this.filename);  
                    RandomAccessFile access = new RandomAccessFile(file,"r");  
                    //  
                    StringWriter sw1 = new StringWriter();  
                    while((temp = in.read()) != 0){  
                        sw1.write(temp);  
                        sw1.flush();  
                    }  
                    System.out.println(sw1.toString());  
                    // 获取断点位置  
                    int startIndex = 0;  
                    if(!sw1.toString().isEmpty()){  
                        startIndex = Integer.parseInt(sw1.toString());  
                    }  
                    long length = file.length();  
                    byte[] filelength = String.valueOf(length).getBytes();  
                    out.write(filelength);  
                    out.write(0);  
                    out.flush();  
                    // 计划要读的文件长度  
                    //int length = (int) file.length();//Integer.parseInt(sw2.toString());  
                    System.out.println("file length : " + length);  
                    // 缓冲区1M  
                    byte[] buffer = new byte[1024*1024];  
                    // 剩余要读取的长度  
                    int tatol = (int) length;  
                    System.out.println("startIndex : " + startIndex);  
                    access.skipBytes(startIndex);  
                    while (true) {  
                        // 如果剩余长度为0则结束  
                        if(tatol == 0){  
                            break;  
                        }  
                        // 本次要读取的长度假设为剩余长度  
                        int len = tatol - startIndex;  
                        // 如果本次要读取的长度大于缓冲区的容量  
                        if(len > buffer.length){  
                            // 修改本次要读取的长度为缓冲区的容量  
                            len = buffer.length;  
                        }  
                        // 读取文件，返回真正读取的长度  
                        int rlength = access.read(buffer,0,len);  
                        // 将剩余要读取的长度减去本次已经读取的  
                        tatol -= rlength;  
                        // 如果本次读取个数不为0则写入输出流，否则结束  
                        if(rlength > 0){  
                            // 将本次读取的写入输出流中  
                            out.write(buffer,0,rlength);  
                            out.flush();  
                        } else {  
                            break;  
                        }  
                        // 输出读取进度  
                        //System.out.println("finish : " + ((float)(length -tatol) / length) *100 + " %");  
                    }  
                    //System.out.println("receive file finished!");  
                    // 关闭流  
                    out.close();  
                    in.close();  
                    access.close();  
                }  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
            super.run();  
        }  
    }  
      
    public void run(String file_on_svr, Socket socket){  
        
        new Sender(file_on_svr,socket).start();  
    }  
      
    public static void main(String[] args) throws Exception {  
        
        ServerSocket server = new ServerSocket(8888);  
        /*
         * file path
         */
        String file_on_svr = "/home/feiben/DBTest/TheMatrix.rmvb";  
        for(;;){  
            Socket socket = server.accept();  
            new OTPServer().run(file_on_svr, socket);  
        }  
    }  
  
}  