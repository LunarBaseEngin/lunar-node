package LCG.DB.Local.NLP.Word2Vex.Util;

/* 
 * Interface of HaffmanTree node
 */
public interface HuffmanNode extends Comparable<HuffmanNode> {

    public void setCode(int c);

    public void setFrequency(int freq);

    public int getFrequency();

    public void setParent(HuffmanNode parent);

    public HuffmanNode getParent();

    public HuffmanNode merge(HuffmanNode sibling);
}
