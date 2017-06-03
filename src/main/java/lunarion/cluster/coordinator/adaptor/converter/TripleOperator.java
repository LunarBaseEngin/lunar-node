package lunarion.cluster.coordinator.adaptor.converter;

public class TripleOperator {
	public enum ops {
	    eq, /* equal */
	    lt, /* less than */
	    lte, /* less than and equal */
	    not, /* not */
	    gt, /* great than */
	    gte, /* great than and equal */
	    like /* column like 'keyword1, keyword2 + keyword3', 
	    		which is lunarBase specialized for full text search
	    		*/
	    
	}
	
	/*
	 * String[0]: column name;
	 * String[1]: lower bound;
	 * String[2]: upper bound
	 */
	private String column;
	private long lower_bound = 0L;
	private long upper_bound = 0L;
	
	private boolean lower_inclusive = false;
	private boolean upper_inclusive = false;
	
	boolean is_keywords_search = false;
	private String keywords = "";
	public TripleOperator(String col, long l_b, long u_b, boolean _lower_inclusive, boolean _upper_inclusive)
	{
		column = col;
		lower_bound = l_b ;
		upper_bound = u_b ;
		
		lower_inclusive = _lower_inclusive;
		upper_inclusive = _upper_inclusive;
	}
	
	public TripleOperator(String col, String _keywords)
	{
		column = col; 
		is_keywords_search = true;
		keywords = _keywords;
	}
	
	public String getKeywords()
	{ 
		return keywords;
	}
	public boolean isKeywordSearch()
	{
		return is_keywords_search;
	}
	public String getColumn()
	{
		return column;
	}
	
	public long getLowerBound()
	{
		return this.lower_bound;
	}
	
	public long getUpperBound()
	{
		return this.upper_bound;
	}
	
	public boolean isLowerInclusive()
	{
		return this.lower_inclusive;
	}
	
	public boolean isUpperInclusive()
	{
		return this.upper_inclusive;
	}
	
	public TripleOperator and(TripleOperator another_t_o)
	{
		if(another_t_o.isKeywordSearch() || this.is_keywords_search)
			return this;
		
		if(this.column.equalsIgnoreCase(another_t_o.getColumn()))
		{
			long lower = Math.max(this.lower_bound, another_t_o.getLowerBound());
			boolean l_inclusive = this.lower_bound < another_t_o.getLowerBound()?
										another_t_o.isLowerInclusive():
										this.lower_inclusive;
					
			long upper = Math.min(this.upper_bound, another_t_o.getUpperBound());
			boolean u_inclusive = this.upper_bound > another_t_o.getUpperBound()?
										another_t_o.isUpperInclusive():
										this.upper_inclusive;
							
			return new TripleOperator(this.column, lower, upper, l_inclusive, u_inclusive);
		}
		return null;
	}
	
	 
}
