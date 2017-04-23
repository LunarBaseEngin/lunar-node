package LCG.DB.Local.NLP.Word2Vex;

import java.io.*;
import java.util.*;

 
public class VectorModel {

    private Map<String, float[]> wordMap = new HashMap<String, float[]>();
    /*
     * word feature dimension
     */
    private int word_feature_dim = 200;  

    private int topNSize = 40;

    public Map<String, float[]> getWordMap() {
        return wordMap;
    }

    public void setWordMap(Map<String, float[]> wordMap){
        this.wordMap = wordMap;
    } 
  
    public int getTopNSize() {
        return topNSize;
    }
 
    public void setTopNSize(int topNSize) {
        this.topNSize = topNSize;
    }

    public int getFeatureDimension() {
        return word_feature_dim;
    }

    public void setFeatureDimension(int vectorSize) {
        this.word_feature_dim = vectorSize;
    }

    
    public VectorModel(Map<String, float[]> word_map, int vector_size){

        if (word_map == null || word_map.isEmpty()){
            throw new IllegalArgumentException("word map is null or empty");
        }
        if (vector_size <= 0){
            throw new IllegalArgumentException("layerSize has to be a positive number");
        }

        this.wordMap = word_map;
        this.word_feature_dim = vector_size;
    }

 
    public static VectorModel loadModel(String path){

        if (path == null || path.isEmpty()){
            throw new IllegalArgumentException("model file can not be empty");
        }

        DataInputStream dis = null;
        int wordCount, layerSizeLoaded = 0;
        Map<String, float[]> wordMapLoaded = new HashMap<String, float[]>();
        try {
            dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            wordCount = dis.readInt();
            layerSizeLoaded = dis.readInt();
            float vector;

            String key;
            float[] value;
            for (int i = 0; i < wordCount; i++) {
                key = dis.readUTF();
                value = new float[layerSizeLoaded];
                double len = 0;
                for (int j = 0; j < layerSizeLoaded; j++) {
                    vector = dis.readFloat();
                    len += vector * vector;
                    value[j] = vector;
                }

                len = Math.sqrt(len);

                for (int j = 0; j < layerSizeLoaded; j++) {
                    value[j] /= len;
                }
                wordMapLoaded.put(key, value);
            }

        } catch (IOException ioe){
            ioe.printStackTrace();
        }finally {
            try {
                if (dis != null){
                    dis.close();
                }
            } catch (IOException ioe){
                ioe.printStackTrace();
            }
        }

        return new VectorModel(wordMapLoaded, layerSizeLoaded);

    }
 
    public void saveModel(File file) {

        DataOutputStream dataOutputStream = null;
        try {
            dataOutputStream = new DataOutputStream(new BufferedOutputStream(
                    new FileOutputStream(file)));
            dataOutputStream.writeInt(wordMap.size());
            dataOutputStream.writeInt(word_feature_dim);
            for (Map.Entry<String, float[]> element : wordMap.entrySet()) {
                dataOutputStream.writeUTF(element.getKey());
                for (float d : element.getValue()) {
                    dataOutputStream.writeFloat(d);
                }
            }
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

    /* 
     * if the model does not contains this query_word, 
     * returns a empty set.
     */
    public Set<WordScore> similar(String query_word){

        float[] center = wordMap.get(query_word);
        if (center == null){
            return Collections.emptySet();
        }

        int resultSize = wordMap.size() < topNSize ? wordMap.size() : topNSize + 1;
        TreeSet<WordScore> result = new TreeSet<WordScore>();
        for (int i = 0; i < resultSize; i++){
            result.add(new WordScore("^_^", -Float.MAX_VALUE));
        }
        float minDist = -Float.MAX_VALUE;
        for (Map.Entry<String, float[]> entry : wordMap.entrySet()){
            float[] vector = entry.getValue();
            float dist = 0;
            for (int i = 0; i < vector.length; i++){
                dist += center[i] * vector[i];
            }
            if (dist > minDist){
                result.add(new WordScore(entry.getKey(), dist));
                minDist = result.pollLast().score;
            }
        }
        result.pollFirst();

        return result;
    }

    public Set<WordScore> similar(float[] center){
        if (center == null || center.length != word_feature_dim){
            return Collections.emptySet();
        }

        int resultSize = wordMap.size() < topNSize ? wordMap.size() : topNSize;
        TreeSet<WordScore> result = new TreeSet<WordScore>();
        for (int i = 0; i < resultSize; i++){
            result.add(new WordScore("^_^", -Float.MAX_VALUE));
        }
        float minDist = -Float.MAX_VALUE;
        for (Map.Entry<String, float[]> entry : wordMap.entrySet()){
            float[] vector = entry.getValue();
            float dist = 0;
            for (int i = 0; i < vector.length; i++){
                dist += center[i] * vector[i];
            }
            if (dist > minDist){
                result.add(new WordScore(entry.getKey(), dist));
                minDist = result.pollLast().score;
            }
        }
//        result.pollFirst();

        return result;
    }

    /**
     * 词迁移，即word1 - word0 + word2 的结果，若三个词中有一个不在模型中，
     * 也就是没有词向量，则返回空集
     * @param word0 词
     * @param word1 词
     * @param word2 词
     * @return 与结果最相近的前topNSize个词
     */
    public TreeSet<WordScore> analogy(String word0, String word1, String word2) {
        float[] wv0 = wordMap.get(word0);
        float[] wv1 = wordMap.get(word1);
        float[] wv2 = wordMap.get(word2);

        if (wv1 == null || wv2 == null || wv0 == null) {
            return null;
        }
        float[] center = new float[word_feature_dim];
        for (int i = 0; i < word_feature_dim; i++) {
            center[i] = wv1[i] - wv0[i] + wv2[i];
        }

        int resultSize = wordMap.size() < topNSize ? wordMap.size() : topNSize;
        TreeSet<WordScore> result = new TreeSet<WordScore>();
        for (int i = 0; i < resultSize; i++){
            result.add(new WordScore("FINE", -Float.MAX_VALUE));
        }
        String name;
        float minDist = -Float.MAX_VALUE;
        for (Map.Entry<String, float[]> entry : wordMap.entrySet()){
            name = entry.getKey();
            if (name.equals(word1) || name.equals((word2))){
                continue;
            }
            float[] vector = entry.getValue();
            float dist = 0;
            for (int i = 0; i < vector.length; i++){
                dist += center[i] * vector[i];
            }
            if (dist > minDist){
                result.add(new WordScore(entry.getKey(), dist));
                minDist = result.pollLast().score;
            }
        }
        return result;
    }

    public float[] getWordVector(String word) {
        return wordMap.get(word);
    }


    public class WordScore implements Comparable<WordScore> {

        public String name;
        public float score;

        public WordScore(String name, float score) {
            this.name = name;
            this.score = score;
        }

        @Override
        public String toString() {
            return this.name + "\t" + score;
        }

         
        public int compareTo(WordScore o) {
            if (this.score < o.score) {
                return 1;
            } else {
                return -1;
            }
        }
    }

}
