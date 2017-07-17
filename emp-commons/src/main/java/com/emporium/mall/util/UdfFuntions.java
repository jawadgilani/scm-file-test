/**
 * 
 */
package com.emporium.mall.util;

/**
 * @author sg186104
 *
 */
import org.apache.spark.sql.api.java.UDF3;

/**
 * Created by SG186104 on 28/06/2017.
 * <p>
 * Collection of utility UDFs
 */
public class UdfFuntions {

    /**
     * Derive location from DPI table data
     */
    public static UDF3<String, String, String, String> code = new UDF3<String, String, String, String>() {

        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		
        public String call(String atri1, String atri2, String atri3) throws Exception {
           
			if(atri1.substring(0, 2) == "TOY" )
			{
				String Toycode = atri1.substring(0,2) + "" + 
			    String.valueOf(Integer.valueOf(atri2,5)) + "" + 
			    atri3.substring(2,4);
				
				return Toycode;
			} else if(atri1.substring(0, 2) == "GAR" )
			{
				String Garmentscode = atri1.substring(0,5) + "" + 
			    String.valueOf(Integer.valueOf(atri2,2)) + "" + 
			    atri3.substring(0);
				return Garmentscode;
				
			}else if (atri1.substring(0, 2) == "GRO" )
			{
				String GrosCode = atri1.substring(0) + "" + 
			    String.valueOf(Integer.valueOf(atri2,5));
			   return GrosCode;
			} else 
				
				return null	;	
			
        }
    };
}