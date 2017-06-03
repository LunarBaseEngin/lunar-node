package lunarion.cluster.coordinator.adaptor;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.Grammar.AST.Relational.RelConstants;
import lunarion.cluster.coordinator.adaptor.converter.TripleOperator; 

public class LogicUtile {
	
	LunarDB db_instance; 
	String table_name;
	
	String[] column_names;
	public LogicUtile(LunarDB _db_instance, String _table_name , String[] _cols)
	{
		db_instance = _db_instance;
		table_name = _table_name;
		column_names = _cols;
	}

	/*
	 * A simple binary is as such forms:
	 * col op val, where op can be any inequality.
	 * e.g.
	 * payment <= 100,
	 * payment > 1000, 
	 */
	public TripleOperator isSimpleBinary(RexNode filter_node ) 
	{  
		 SqlKind sk = filter_node.getKind();
		 
		 TripleOperator t_o = null;
		 switch (sk) {
	      	case EQUALS:
	      		t_o = translateBinary(TripleOperator.ops.eq, TripleOperator.ops.eq, (RexCall) filter_node);
	      		break;
	      	case LESS_THAN:
	      		t_o = translateBinary(TripleOperator.ops.lt, TripleOperator.ops.gt, (RexCall) filter_node);
	      		break;
	      	case LESS_THAN_OR_EQUAL:
	      		t_o = translateBinary(TripleOperator.ops.lte, TripleOperator.ops.gte, (RexCall) filter_node);
	      		break;
	      	case NOT_EQUALS:
	      		t_o = translateBinary(TripleOperator.ops.not, TripleOperator.ops.not, (RexCall) filter_node);
	      		break;
	      	case GREATER_THAN:
	      		t_o = translateBinary(TripleOperator.ops.gt, TripleOperator.ops.lt, (RexCall) filter_node);
	      		break;
	      	case GREATER_THAN_OR_EQUAL:
	      		t_o = translateBinary(TripleOperator.ops.gte, TripleOperator.ops.lte, (RexCall) filter_node);
	      		break; 
	      	case LIKE:
	      		t_o = translateBinary(TripleOperator.ops.like, TripleOperator.ops.like, (RexCall) filter_node);
	      		break;
	      	default:
	      		break; 
		 } 
		 
		 return t_o; 
	 }
	protected String makeQuery(TripleOperator t_o)
	{
		if(t_o.isKeywordSearch())
			return makeFTQuery(t_o);
		else
			return makeRGQuery(t_o);
		
	}
	protected String makeFTQuery(TripleOperator t_o)
	{
		return  ""
				+ RelConstants.start_ft_search 
				+ RelConstants.start_exp
				+ t_o.getColumn() + " against(\"" + t_o.getKeywords() + "\")"
				+ RelConstants.end_exp;
		
	}
	
	private String makeRGQuery(TripleOperator t_o)
	{
		char lower_inclusive = t_o.isLowerInclusive()?'1':'0';
		char upper_inclusive = t_o.isUpperInclusive()?'1':'0';
		
		return  ""
				+ RelConstants.start_range
				+ RelConstants.start_exp
				+ t_o.getColumn() + ", " 
				+ t_o.getLowerBound()+ ", " 
				+ t_o.getUpperBound()+ ", " 
				+ lower_inclusive + ", " 
				+ upper_inclusive
				+ RelConstants.end_exp;
		
	}
	public FTQueryResult query(TripleOperator t_o)
	{
		String statement = makeQuery(t_o);
		
		return query(statement);
	}
	
	public FTQueryResult query(String statement)
	{
		return db_instance.queryRelational(table_name, statement); 
	}
	
	/*
	 * returns the ExpressionNode that lunar-node accepts.
	 * @ExpressionNode
	 */
	public String isSimpleLogic(RexNode filter_node ) 
	{
		 SqlKind sk = filter_node.getKind();
		 RexCall call = (RexCall) filter_node;
		 
		 //FTQueryResult result_iter = null;
		 String result_iter = ""; 
		 final RexNode left_exp = call.operands.get(0);
		 final RexNode right_exp = call.operands.get(1);
		 TripleOperator t_o_left = isSimpleBinary(left_exp);
		 TripleOperator t_o_right = isSimpleBinary(right_exp);
		 if(t_o_left!= null && t_o_right != null)
		 {
			 switch (sk) 
			 {
			 case AND:
			 	{
			 		result_iter = andLeftRight( t_o_left, t_o_right) ;
			 	}
			 	break;
			 case OR:
			 	{
			 		result_iter = orLeftRight( t_o_left, t_o_right);
			 	}
			 	break;	    		      	 
			 } 
		 }
		 else if(t_o_left == null && t_o_right == null)
		 {
			 String left_result =  isSimpleLogic(left_exp);
			 String right_result = isSimpleLogic(right_exp);
			 result_iter = logicalResult( sk,left_result,right_result); 
		 }
		 else 
		 { 
			 if(t_o_left != null)
			 {
				 //FTQueryResult left_result = query(t_o_left); 
				 //FTQueryResult right_result = isSimpleLogic(right_exp);
				 
				 String left_result = this.makeQuery(t_o_left);
				 String right_result = isSimpleLogic(right_exp); 
	    				
				 result_iter = logicalResult( sk,left_result,right_result); 
			 }
			 else
			 {
				 //FTQueryResult right_result = query(t_o_right); 
				 //FTQueryResult left_result = isSimpleLogic(left_exp);
				 String right_result = this.makeQuery(t_o_right); 
				 String left_result = isSimpleLogic(left_exp);
				 
				 result_iter = logicalResult( sk,left_result,right_result); 
			 } 
		 }
	    		
		 /*
		  * do the rest, in the case that has more operands like:
		  * score <90 AND score >65 and payment > 1000
		  * 
		  * the three will in the same operands list
		  */
		 if(call.operands.size()>2)
		 {
			 for(int i=2;i<call.operands.size();i++)
			 {
				 final RexNode exp_i = call.operands.get(i);
				 TripleOperator t_o_i = isSimpleBinary(exp_i);
				 if(t_o_i!= null )
				 {
					// FTQueryResult result_i = query(t_o_i);  
					 String result_i = this.makeQuery(t_o_i);  
					 result_iter = logicalResult( sk,result_iter,result_i);  
				 }
				 else
				 {
					 //FTQueryResult result_i = isSimpleLogic(exp_i);
					 String result_i = isSimpleLogic(exp_i);
					 result_iter = logicalResult( sk,result_iter,result_i);  
				 }
			 }
		 }
	    		
		 return result_iter; 
	}
	
	 private TripleOperator makeTripleOperator(TripleOperator.ops op, RexNode left, RexNode right)
	 {
		 String col_name = null;
		 /*
		  * in the case score=95, the left is translated to:
		  * CAST($0):INTEGER NOT NULL
		  * 
		  * then it is a cast, and can not directly transform to RexInputRef
		  */
		 switch (left.getKind()) {
	      	case INPUT_REF:
	      		final RexInputRef left1 = (RexInputRef) left;
	      		col_name = column_names[left1.getIndex()]; 
	      		break;
	      	case CAST:
	      		RexNode node = ((RexCall) left).operands.get(0);
	      		final RexInputRef left2 = (RexInputRef) node;
	      		col_name = column_names[left2.getIndex()]; 
	      		break;		 
		 }
		 
		// final RexInputRef left1 = (RexInputRef) left;
  		 
		 TripleOperator bo = null;
		 //String col_name = column_names[left1.getIndex()];
   	  	
		 switch(op)
		 {
   	  		case eq:
   	  			{
   	  				long bound =  Long.parseLong(((RexLiteral)right).toString());
   	  				bo = new TripleOperator(col_name, bound, bound, true, true);
   	  			}
   	  			break;
   	  		case lt:
   	  			{ 
   	  				long upper_bound =  Long.parseLong(((RexLiteral)right).toString());
   	  				bo = new TripleOperator(col_name, Long.MIN_VALUE, upper_bound, true, false);
 				}
   	  			break;
   	  		case lte:
   	  			{ 
   	  				long upper_bound =  Long.parseLong(((RexLiteral)right).toString());
   	  				bo = new TripleOperator(col_name, Long.MIN_VALUE, upper_bound, true, true);
   	  			} 
   	  			break;
   	  		case gt:
	   	  		{
	 				long lower_bound =  Long.parseLong(((RexLiteral)right).toString());
	 				bo = new TripleOperator(col_name, lower_bound, Long.MAX_VALUE, false, true);
	 			} 
	 			break;  
   	  		case gte:
   	  			{
   	  				long lower_bound =  Long.parseLong(((RexLiteral)right).toString());
   	  				bo = new TripleOperator(col_name, lower_bound, Long.MAX_VALUE, true, true);
   	  			} 
   	  			break;  
   	  		case like: 
	 			{
	 				/*
	 				 * like 'keyword', then here ((RexLiteral)right).toString() 
	 				 * gets 'keyword', including the single quote
	 				 */
	 				String keywords =   ((RexLiteral)right).toString()  ;
	 				keywords = keywords.substring(1, keywords.length()-1);
	 				bo = new TripleOperator(col_name, keywords);
	 			} 
 				break;  
   	  		case not:
   	  			return null; 
		 }
   	  
		 return bo;
	 }
	 
	 /*
	  * left: column
	  * right: value
	  * 
	  * if is not this order and this format, return false;
	  * For example, these will fail:
	  * column1 = column2
	  * col1<col2
	  */
	 private boolean translateBinary2(TripleOperator.ops op, RexNode left, RexNode right) 
	 {
	      switch (right.getKind()) {
	      	case LITERAL:
	      		break;
	      	default:
	      		return false;
	      }
	      final RexLiteral rightLiteral = (RexLiteral) right;
	      switch (left.getKind()) {
	      	case INPUT_REF:
	      		final RexInputRef left1 = (RexInputRef) left;
	      		String name = column_names[left1.getIndex()];
	      		//translateOp2(op, name, rightLiteral);
	      		 
	      		return true;
	      	case CAST:
	      		return translateBinary2(op, ((RexCall) left).operands.get(0), right);
	      	case OTHER_FUNCTION:
	      		//String itemName = ElasticsearchRules.isItem((RexCall) left);
	      		String itemName = ((RexInputRef) left).getName();
	      		if (itemName != null) {
	      			//translateOp2(op, itemName, rightLiteral);
	      			return true;
	      		}
	      		// fall through
	      default:
	        return false;
	      }
	    }
	 
	 private TripleOperator translateBinary(TripleOperator.ops op, TripleOperator.ops reverse_op, RexCall call) 
		{
			final RexNode left = call.operands.get(0);
			final RexNode right = call.operands.get(1);
			boolean b = translateBinary2(op, left, right);
			TripleOperator bo = null;
			if (b) 
			{
				return makeTripleOperator( op, left, right); 
			}
			b = translateBinary2(reverse_op, right, left);
			if (b) 
			{
				return makeTripleOperator( reverse_op, right, left); 
			} 
			else
				return null;
		 }
		 
	 private String logicalResult(SqlKind sk, String left, String right)
		{
			String result_iter = null;
			switch (sk) 
			{
		      	case AND:
		      	{
		      		//result_iter =  left.intersectAnother(right);
		      		result_iter = ""+ RelConstants.start_and ; 
		      	}
		      	break;
		      	case OR:
		      	{
		      		//result_iter = left.unionAnother(right);
		      		result_iter = ""+ RelConstants.start_or;
		      	}
		      	break;	    		      	 
			} 
			
			result_iter = result_iter 
						+ RelConstants.start_exp
						+ left
						+ RelConstants.seperator
						+ right
						+ RelConstants.end_exp;
			
			return result_iter;
		}
		

	 private String orLeftRight(TripleOperator t_o_left, TripleOperator t_o_right)
	 {
		 String result_iter = null; 
			 
		 String left_result = this.makeQuery(t_o_left);  
		 String right_result = this.makeQuery(t_o_right);  
		 //result_iter =  left_result.unionAnother(right_result);
		 result_iter = RelConstants.start_or 
				 		+ RelConstants.start_exp
				 		+ this.makeQuery(t_o_left)
				 		+ RelConstants.seperator
				 		+ this.makeQuery(t_o_right) 
				 		+ RelConstants.end_exp;
			 
			
	
		 return result_iter;
	}
	 
	 /*
		 * col1 <100 and col2>= 1000, col1 and col2 may be the same column, 
		 * and if it is, this AND will be translated to a TripleOperator:
		 * val1 < col < val2
		 */ 
		private String andLeftRight(TripleOperator t_o_left, TripleOperator t_o_right)
		{
			String result_iter = "";
			TripleOperator new_t_o = t_o_left.and(t_o_right);
			/*
			 * the same column, then change to val1 < column < val2
			 */
			if(new_t_o != null)
			{	
				result_iter = this.makeRGQuery(new_t_o);
			}
			/*
			 * different column, then two query respectively.
			 */
			else
			{
				//FTQueryResult left_result = query(t_o_left);  
				//FTQueryResult right_result = query(t_o_right);  
				//result_iter =  left_result.intersectAnother(right_result);
				result_iter = RelConstants.start_and + RelConstants.start_exp
								+ this.makeQuery(t_o_left)
								+ RelConstants.seperator
								+ this.makeQuery(t_o_right) 
								+ RelConstants.end_exp;
				
			}
			
			return result_iter;
		}
		
	
}
