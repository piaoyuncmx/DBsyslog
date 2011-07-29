/**
 * 
 */
package cn.edu.sjtu.topip;
import java.net.UnknownHostException;

import com.mongodb.*;
/**
 * @author cmx
 */
public class TopIp {
//	public  Mongo mg ;      
  //  public  DB db; 
    //public  DBCollection collection;
	static long startTime=System.currentTimeMillis();
	public static void main(String[] args) throws UnknownHostException, MongoException {
		// TODO Auto-generated method stub 
		 Mongo mg=new Mongo("10.50.15.203");
         DB db = mg.getDB("DBsyslog");
	     DBCollection collection=db.getCollection("panabit_20110704"); 
	     
	     String mapfuction="function(){emit(this.app,{inbyte:this.inbyte,outbyte:this.outbyte,out:0});}";
	     String reducefunction="function(key,vals){var n={inbyte:0,outbyte:0,out:0};" +
	     		"for( var i in vals){" +
	     		"if((vals[i].inbyte+vals[i].outbyte)>(n.inbyte+n.outbyte))" +
	     		"  { n.inbyte=vals[i].inbyte;  n.outbyte=vals[i].outbyte;} }" +
	     		"n.out=n.inbyte+n.outbyte;" +
	     		"return n;}";
	     		
	     DBObject query = new BasicDBObject();   
	    // String TopIP = null;
	     MapReduceOutput mr=collection.mapReduce(mapfuction,reducefunction,"TopIP",query);
      
	    for ( DBObject obj : mr.results() ) {
	         System.out.println( obj );
	     }
	    long endTime=System.currentTimeMillis(); 
	    System.out.println("程序运行时间： "+(endTime-startTime)+"ms");   
 
	}

}
