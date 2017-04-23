
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
package LCG.DB.Local.NLP.WordSeg;

import java.util.List;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class TestAnsjSeg {
	public static void main(String[] args) throws Exception {
		String str1 = "这次新增了 。摘要 ，基于query的摘要 ，文章标红，优化了关键词抽取。等功能。很想做一件事情，做一个开源的nlp处理工具包。包括摘要，关键词抽取，倾向性分析，主题发现等功能，不在这里做了另起一个项目。有兴趣的可以联系我。加油" ;
		String str2 = "说白了 ，光一个摘要都能 写一篇 博士论文 ，对于 开放场景 的 摘要其实未必需要高深的算法，这个可以在工程中用，能用，简单，但是无法做到行业顶级";
		 
		System.out.println(ToAnalysis.parse(str2));
		List<Term> list = ToAnalysis.parse(str1);
		
		for(int i=0;i<list.size();i++)
		{
			System.out.println(list.get(i).getName() 
								+" : " + list.get(i).getNatureStr()
								+" : " + list.get(i).getRealName());
		}
		 
	}
}
