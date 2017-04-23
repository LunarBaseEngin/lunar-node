package LCG.DB.Local.NLP.Word2Vex;

 

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.DB.Local.NLP.Word2Vex.Util.Counter;
import LCG.DB.Local.NLP.Word2Vex.Util.HuffmanNode;
import LCG.DB.Local.NLP.Word2Vex.Util.HuffmanTree;
import LCG.DB.Local.NLP.Word2Vex.Util.LineIterator;
import LCG.RecordTable.StoreUtile.Record32KBytes;

 
public class Word2Vec {

    private Logger logger = Logger.getLogger("Word2Vec");

    /*
     * window size for words
     */
    private int window_size; 
    /*
     * word vector dimension
     */
    private int word_dimension;  

    /*
     * At present, it supports cbow and skip_gram only
     */
    public static enum Method{
        cbow, skip_gram
    }

    /*
     * which neural network training method 
     */
    private Method training_method;  

    private double sample;
	private int negative_sample;
    private double alpha;        
    private double alpha_thresold;
    private double alpha_initial;   
    private int frequent_thresold = 5;
    private final byte[] alphaLock = new byte[0];  
    
    private double[] expTable;
    private static final int EXP_TABLE_SIZE = 1000;
    private static final int MAX_EXP = 6;

    private Map<String, WordNeuron> neuronMap;
 
    private int totalWordCount;     // 语料中的总词数
    private int current_word_count;   // 当前已阅的词数，并行时由线程更新
    private int numOfThread;        // 线程个数

    /*
     * counter for words and phrase
     */
    private Counter<String> word_counter = new Counter<String>();

    //private File tempCorpus = null;
    //private BufferedWriter tempCorpusWriter;
    private LunarDB corpus_db ;
    private String corpus_table ;
    private String corpus_column;
    
    public static class Factory {

        private int vectorSize = 200;
        private int windowSize = 5;
        private int freqThresold = 5;
        
        Method trainMethod = Method.skip_gram;

        private LunarDB corpus_db;
        private String corpus_table;
        private String corpus_column;
        private double sample = 1e-3;
//        private int negativeSample = 0;

        private double alpha = 0.025, alphaThreshold = 0.0001;
        private int numOfThread = 1;

        public Factory setDBConnection(LunarDB db_instance, String _table, String _col) 
        {
        	this.corpus_db = db_instance;
        	this.corpus_table = _table;
        	this.corpus_column = _col;
        	return this;
        }

        public Factory setVectorSize(int size){
            vectorSize = size;
            return this;
        }

        public Factory setWindow(int size){
            windowSize = size;
            return this;
        }

        public Factory setFreqThresold(int thresold){
            freqThresold = thresold;
            return this;
        }

        public Factory setMethod(Method method){
            trainMethod = method;
            return this;
        }

        public Factory setSample(double rate){
            sample = rate;
            return this;
        }

//        public Factory setNegativeSample(int sample){
//            negativeSample = sample;
//            return this;
//        }

        public Factory setAlpha(double alpha){
            this.alpha = alpha;
            return this;
        }

        public Factory setAlphaThresold(double alpha){
            this.alphaThreshold = alpha;
            return this;
        }

        public Factory setNumOfThread(int numOfThread) {
            this.numOfThread = numOfThread;
            return this;
        }

        public Word2Vec build(){
            return new Word2Vec(this);
        }

    }

    private Word2Vec(Factory factory){
        word_dimension = factory.vectorSize;
        window_size = factory.windowSize;
        frequent_thresold = factory.freqThresold;
        training_method = factory.trainMethod;
        sample = factory.sample;
//        negativeSample = factory.negativeSample;
        alpha = factory.alpha;
        alpha_initial = alpha;
        alpha_thresold = factory.alphaThreshold;
        numOfThread = factory.numOfThread;

        totalWordCount = 0;
        expTable = new double[EXP_TABLE_SIZE];
        
        corpus_db = factory.corpus_db;
        corpus_table = factory.corpus_table;
        corpus_column = factory.corpus_column;
        computeExp();
    }

    /**
     * 预先计算并保存sigmoid函数结果，加快后续计算速度
     * f(x) = x / (x + 1)
     */
    private void computeExp() {
        for (int i = 0; i < EXP_TABLE_SIZE; i++) {
            expTable[i] = Math.exp(((i / (double) EXP_TABLE_SIZE * 2 - 1) * MAX_EXP));
            expTable[i] = expTable[i] / (expTable[i] + 1);
        }
    }
  
    public void prepareData(TokenizerForSearchEngine tokenizer, String[] text ){

        if (tokenizer == null ){
            return;
        }
        for(int i=0;i<text.length;i++)
        {
        	tokenizer.tokenize(text[1]);
        	if(tokenizer.size() >= 1)
        	{
        		current_word_count += tokenizer.size();
                /*
                 * count the word frequency
                 */
            	while (tokenizer.hasNext()){
            		word_counter.add(tokenizer.nextToken());
            	} 
        	}
        	
        } 
		 
        
        	
            if (this.corpus_db != null){ 
            	 
            	 for(int j=0;j<text.length;j++)
            	 {
            		 text[j] = "{"+ corpus_column + "=[\"" + text[j] + "\"]}";  
            	 }
            	 
            	 this.corpus_db.insertRecord(this.corpus_table,text);
            }  
        
    }

    public void commitData()
    {
    	this.corpus_db.save();
    }
    private void buildVocabulary() {

        neuronMap = new HashMap<String, WordNeuron>();
        for (String wordText : word_counter.keySet()){
            int freq = word_counter.get(wordText);
            if (freq < frequent_thresold){
                continue;
            }
            neuronMap.put(wordText,
                    new WordNeuron(wordText, word_counter.get(wordText), word_dimension));
        }
        logger.info("[INFO]: Totally read " + neuronMap.size() + " words.");
 
    }

    public void training() throws IOException{

        if (this.corpus_db == null){
            throw new NullPointerException("Corpus dataset is not available, please check your program again.");
        }

        int total_recs = this.corpus_db.recordsCount(this.corpus_table);
        int from = 0; 
        int count_each = 1000;
        TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
	
		while((from )< total_recs)
		{
			ArrayList<Record32KBytes>  result = this.corpus_db.fetchRecords(this.corpus_table, from, count_each);
			  
			for(int i=0;i< result.size();i++)
			{ 
				String text = result.get(i).valueOf(this.corpus_column) ;
				t_e.tokenize(text); 
				current_word_count += t_e.size();
				/*
				 * count the word frequency
				 */
				while (t_e.hasNext()){
			            word_counter.add(t_e.nextToken());
				} 
			} 
			from+=count_each;
		} 
        
        
        buildVocabulary();
        HuffmanTree.make(neuronMap.values());
        
        totalWordCount = current_word_count;
        current_word_count = 0;
        
        ExecutorService threadPool = Executors.newFixedThreadPool(numOfThread);

        
        try {
            BlockingQueue<LinkedList<String>> corpusQueue = new ArrayBlockingQueue<LinkedList<String>>(numOfThread);
            LinkedList<Future> futures = new LinkedList<Future>();  

            for (int i = 0; i < numOfThread; i++){ 
                futures.add(threadPool.submit(new Trainer(corpusQueue)));
            }
            
           
            from = 0; 
           	 
			while((from )< total_recs)
			{
				ArrayList<Record32KBytes>  result = this.corpus_db.fetchRecords(this.corpus_table, from, count_each);
				LinkedList<String> corpus = new LinkedList<String>();   
	            
				for(int i=0;i< result.size();i++)
				{ 
					String text = result.get(i).valueOf(this.corpus_column) ;
					corpus.add(text);  
				}
				
				corpusQueue.put(corpus);
				 
				
				from+=count_each;
			} 
  
            logger.info("the task queue has been allocated completely, " +
                    "please wait the thread pool (" + numOfThread + ") to process...");
 
            for (Future future : futures){
                
					future.get();
				
            }
            threadPool.shutdown();   
        } catch (InterruptedException|ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }

    private void skipGram(int index, List<WordNeuron> sentence, int b, double alpha) {

        WordNeuron word = sentence.get(index);
        int a, c = 0;
        for (a = b; a < window_size * 2 + 1 - b; a++) {
            if (a == window_size) {
                continue;
            }
            c = index - window_size + a;
            if (c < 0 || c >= sentence.size()) {
                continue;
            }

            double[] neu1e = new double[word_dimension];//误差项
            //Hierarchical Softmax
            List<HuffmanNode> pathNeurons = word.getPathNeurons();
            WordNeuron we = sentence.get(c);
            for (int neuronIndex = 0; neuronIndex < pathNeurons.size() - 1; neuronIndex++){
                HuffmanNeuron out = (HuffmanNeuron) pathNeurons.get(neuronIndex);
                double f = 0;
                // Propagate hidden -> output
                for (int j = 0; j < word_dimension; j++) {
                    f += we.vector[j] * out.vector[j];
                }
                if (f <= -MAX_EXP || f >= MAX_EXP) {
//                    System.out.println("F: " + f);
                    continue;
                } else {
                    f = (f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2);
                    f = expTable[(int) f];
                }
                // 'g' is the gradient multiplied by the learning rate
                HuffmanNeuron outNext = (HuffmanNeuron) pathNeurons.get(neuronIndex+1);
                double g = (1 - outNext.code - f) * alpha;
                for (c = 0; c < word_dimension; c++) {
                    neu1e[c] += g * out.vector[c];
                }
                // Learn weights hidden -> output
                for (c = 0; c < word_dimension; c++) {
                    out.vector[c] += g * we.vector[c];
                }
            }
            // Learn weights input -> hidden
            for (int j = 0; j < word_dimension; j++) {
                we.vector[j] += neu1e[j];
            }
        }

//        if (word.getName().equals("手")){
//            for (Double value : word.vector){
//                System.out.print(value + "\t");
//            }
//            System.out.println();
//        }
    }

    private void cbowGram(int index, List<WordNeuron> sentence, int b, double alpha) {

        WordNeuron word = sentence.get(index);
        int a, c = 0;

        double[] neu1e = new double[word_dimension];//误差项
        double[] neu1 = new double[word_dimension];//误差项
        WordNeuron last_word;

        for (a = b; a < window_size * 2 + 1 - b; a++)
            if (a != window_size) {
                c = index - window_size + a;
                if (c < 0)
                    continue;
                if (c >= sentence.size())
                    continue;
                last_word = sentence.get(c);
                if (last_word == null)
                    continue;
                for (c = 0; c < word_dimension; c++)
                    neu1[c] += last_word.vector[c];
            }
        //Hierarchical Softmax
        List<HuffmanNode> pathNeurons = word.getPathNeurons();
        for (int neuronIndex = 0; neuronIndex < pathNeurons.size() - 1; neuronIndex++){
            HuffmanNeuron out = (HuffmanNeuron) pathNeurons.get(neuronIndex);

            double f = 0;
            // Propagate hidden -> output
            for (c = 0; c < word_dimension; c++)
                f += neu1[c] * out.vector[c];
            if (f <= -MAX_EXP)
                continue;
            else if (f >= MAX_EXP)
                continue;
            else
                f = expTable[(int) ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2))];
            // 'g' is the gradient multiplied by the learning rate
            HuffmanNeuron outNext = (HuffmanNeuron) pathNeurons.get(neuronIndex+1);
            double g = (1 - outNext.code - f) * alpha;
            //
            for (c = 0; c < word_dimension; c++) {
                neu1e[c] += g * out.vector[c];
            }
            // Learn weights hidden -> output
            for (c = 0; c < word_dimension; c++) {
                out.vector[c] += g * neu1[c];
            }
        }
        for (a = b; a < window_size * 2 + 1 - b; a++) {
            if (a != window_size) {
                c = index - window_size + a;
                if (c < 0)
                    continue;
                if (c >= sentence.size())
                    continue;
                last_word = sentence.get(c);
                if (last_word == null)
                    continue;
                for (c = 0; c < word_dimension; c++)
                    last_word.vector[c] += neu1e[c];
            }

        }
    }

    private long nextRandom = 5;

    public class Trainer implements Runnable{

        private BlockingQueue<LinkedList<String>> corpusQueue;

        private LinkedList<String> corpusToBeTrained;
        int trainingWordCount;
        double tempAlpha;
      
        public Trainer(LinkedList<String> corpus){
            corpusToBeTrained = corpus;
            trainingWordCount = 0;
          
        }

        public Trainer(BlockingQueue<LinkedList<String>> corpusQueue){
            this.corpusQueue = corpusQueue;
        }

        private void computeAlpha(){
            synchronized (alphaLock){
                current_word_count += trainingWordCount;
                alpha = alpha_initial * (1 - current_word_count / (double) (totalWordCount + 1));
                if (alpha < alpha_initial * 0.0001) {
                    alpha = alpha_initial * 0.0001;
                }
//                logger.info("alpha:" + tempAlpha + "\tProgress: "
//                        + (int) (currentWordCount / (double) (totalWordCount + 1) * 100) + "%");
                System.out.println("alpha:" + tempAlpha + "\tProgress: "
                        + (int) (current_word_count / (double) (totalWordCount + 1) * 100)
                        + "%\t");
            }
        }

        private void training(){
 
            for( String line : corpusToBeTrained){
                List<WordNeuron> sentence = new ArrayList<WordNeuron>();
                TokenizerForSearchEngine tokenizer = new TokenizerForSearchEngine( );
                tokenizer.tokenize(line);
                trainingWordCount += tokenizer.size();
                while (tokenizer.hasNext()){
                    String token = tokenizer.nextToken();
                    WordNeuron entry = neuronMap.get(token);
                    if (entry == null) {
                        continue;
                    }
                    // The subsampling randomly discards frequent words while keeping the ranking same
                    if (sample > 0) {
                        double ran = (Math.sqrt(entry.getFrequency() / (sample * totalWordCount)) + 1)
                                * (sample * totalWordCount) / entry.getFrequency();
                        nextRandom = nextRandom * 25214903917L + 11;
                        if (ran < (nextRandom & 0xFFFF) / (double) 65536) {
                            continue;
                        }
                        sentence.add(entry);
                    }
                }
                for (int index = 0; index < sentence.size(); index++) {
                    nextRandom = nextRandom * 25214903917L + 11;
                    switch (training_method){
                        case cbow:
                            cbowGram(index, sentence, (int) nextRandom % window_size, tempAlpha);
                            break;
                        case skip_gram:
                            skipGram(index, sentence, (int) nextRandom % window_size, tempAlpha);
                            break;
                    }
                }

            }


        }

        @Override
        public void run() {
            boolean hasCorpusToBeTrained = true;

            try {
                while (hasCorpusToBeTrained){
//                    System.out.println("get a corpus");
                    corpusToBeTrained = corpusQueue.poll(2, TimeUnit.SECONDS);
//                    System.out.println("queue size: " + corpusQueue.size());
                    if (null != corpusToBeTrained) {
                        tempAlpha = alpha;
                        trainingWordCount = 0;
                        training();
                        computeAlpha(); //更新alpha
                    } else {
                        // 超过2s还没获得数据，认为主线程已经停止投放语料，即将停止训练。
                        hasCorpusToBeTrained = false;
                    }
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

        }
    }

    /**
     * 保存训练得到的模型
     * @param file 模型存放路径
     */
    public void saveModel(File file) {

        DataOutputStream dataOutputStream = null;
        try {
            dataOutputStream = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file)));
            dataOutputStream.writeInt(neuronMap.size());
            dataOutputStream.writeInt(word_dimension);
            for (Map.Entry<String, WordNeuron> element : neuronMap.entrySet()) {
                dataOutputStream.writeUTF(element.getKey());
                for (double d : element.getValue().vector) {
                    dataOutputStream.writeFloat(((Double) d).floatValue());
                }
            }
            logger.info("saving model successfully in " + file.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (dataOutputStream != null){
                    dataOutputStream.close();
                }
            }catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    public VectorModel outputVector(){

        Map<String, float[]> wordMapConverted = new HashMap<>();
        String wordKey;
        float[] vector;
        double vectorLength;
        double[] vectorNorm;

        for (Map.Entry<String, WordNeuron> element : neuronMap.entrySet()) {

            wordKey = element.getKey();

            vectorNorm = element.getValue().vector;
            vector = new float[word_dimension];
            vectorLength = 0;

            for (int vi = 0; vi < vectorNorm.length ; vi++){
                vectorLength += (float) vectorNorm[vi] * vectorNorm[vi];
                vector[vi] = (float) vectorNorm[vi];
            }

            vectorLength = Math.sqrt(vectorLength);

            for (int vi = 0; vi < vector.length; vi++) {
                vector[vi] /= vectorLength;
            }
            wordMapConverted.put(wordKey, vector);
        }

        return new VectorModel(wordMapConverted, word_dimension);
    }

}
