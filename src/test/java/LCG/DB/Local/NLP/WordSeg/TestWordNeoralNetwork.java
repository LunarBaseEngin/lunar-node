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
 
public class TestWordNeoralNetwork {

    public static void trainCorpus( 
    							String model_file,
    							LunarDB corpus_db,
    							String corpus_table, 
    							String corpus_column) throws IOException{

        Word2Vec wv = new Word2Vec.Factory()
                .setMethod(Word2Vec.Method.skip_gram)
                .setNumOfThread(1)
                .setDBConnection(corpus_db, corpus_table, corpus_column)
                .build(); 
        
       
        wv.training();
        wv.saveModel(new File(model_file));
    }

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
 
    	String model_file = "/home/feiben/DBTest/LunarNode/corpus1.model";
    	/*
		 * this corpus_db is where the corpus stored.
		 */
		String corpus_db = "/home/feiben/DBTest/LunarNode/CorpusDB";
		LunarDB l_db = new LunarDB();	 
		
		l_db.openDB(corpus_db);
		String table = "text";
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
		TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
		tt.registerTokenizer(column, t_e); 
		
        trainCorpus( model_file, l_db, table, column);
       // testModel(model_file);
        
        
        l_db.closeDB();
    }

}
