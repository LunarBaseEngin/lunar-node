package LCG.DB.Local.NLP.WordSeg;

 

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.DB.Local.NLP.Word2Vex.VectorModel;
import LCG.DB.Local.NLP.Word2Vex.Word2Vec;
import LCG.RecordTable.StoreUtile.Record32KBytes;
 
public class TestWordNeoralNetworkQuery {

  
    public static void testModel(String model_file){

        VectorModel vm = VectorModel.loadModel(model_file);
        Set<VectorModel.WordScore> result1 = Collections.emptySet();

        String word = "社会";
        System.out.println("==========" + word + "===============");
        result1 = vm.similar(word);
        for (VectorModel.WordScore ws : result1){
            System.out.println(ws.name + " :\t" + ws.score);
        }
       
        word = "坚持";
        System.out.println("==========" + word + "===============");
        Set<VectorModel.WordScore> result2 = Collections.emptySet();

        result2 = vm.similar(word);
        for (VectorModel.WordScore ws : result2){
            System.out.println(ws.name + " :\t" + ws.score);
        }
        
        word = "关系";
        System.out.println("==========" + word + "===============");
        Set<VectorModel.WordScore> result3 = Collections.emptySet();

        result3 = vm.similar(word);
        for (VectorModel.WordScore ws : result3){
            System.out.println(ws.name + " :\t" + ws.score);
        }
    }

    public static void main(String[] args) throws IOException{
 
    	String model_file = "/home/feiben/EclipseWorkspace/lunarbase-node/corpus/corpus1.model";
    	 
        testModel(model_file);
        
        
    }

}
