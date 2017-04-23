package LCG.DB.Local.NLP.Word2Vex;

import LCG.DB.Local.NLP.Word2Vex.Util.HuffmanNode;

public class HuffmanNeuron implements HuffmanNode {

    protected int frequency = 0;
    protected HuffmanNode parentNeuron;
    protected int code = 0;
    protected double[] vector;

    
    public void setCode(int c) {
        code = c;
    }

  
    public void setFrequency(int freq) {
        frequency = freq;
    }

    
    public int getFrequency() {
        return frequency;
    }

   
    public void setParent(HuffmanNode parent) {
        parentNeuron = parent;
    }

  
    public HuffmanNode getParent() {
        return parentNeuron;
    }

   
    public HuffmanNode merge(HuffmanNode right){
        HuffmanNode parent = new HuffmanNeuron(frequency+right.getFrequency(), vector.length);
        parentNeuron = parent;
        this.code = 0;
        right.setParent(parent);
        right.setCode(1);
        return parent;
    }

     
    public int compareTo(HuffmanNode hn) {

        if (frequency > hn.getFrequency()){
            return 1;
        } else {
            return -1;
        }
    }

    public HuffmanNeuron(int freq, int vector_size) {

        this.frequency = freq;
        this.vector = new double[vector_size];
        parentNeuron = null;

    }
}
