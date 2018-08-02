package comp9313.ass4

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


//Implementation of Set Similarity Join using ppjoin algorithm proposed by Professor Wei Wang, 
//Scientia Professor Xuemin Lin at UNSW. 

object SetSimJoin {
  def main(args:Array[String]){
   val inputFile=args(0)
   val outputFolder=args(1)
   val threshold = args(2).toDouble
   val conf=new SparkConf().setAppName("SimSimilarity").setMaster("local")
   val sc=new SparkContext(conf)
   val textfile =sc.textFile(inputFile)
   val lines=textfile.map(x=>x.split(" "))
   def jaccard[A](a:Set[A],b:Set[A]):Any={
     var sim:Double= 0.0; 
     sim = (a.intersect(b).size.toDouble/a.union(b).size.toDouble); 
     if(sim>=threshold){
       return sim} else{
         None}
     }
   def overlap[A](a:List[A],b:List[A],t:Double):Int={
     Math.ceil(((t/(1.0+t))*(a.size+b.size))).toInt}
   
   //Frequency sorting- sorting each row according to the token frequency
   val row_tokens =lines.flatMap(x=>x.tail)
   val token_doc_count=row_tokens.map(x=>(x.toInt,1)).countByKey().toList
   val sorted_tokens=token_doc_count.sortBy(_._2).map({case(a,b)=>a})
   val tokens_map=sorted_tokens.zipWithIndex.toMap
   val row_id=lines.map(x=>(x(0).toInt,x.drop(1).map(_.toInt)
       .sortBy(a=>tokens_map(a))))
   val doc_id_map=row_id.map(x=>(x._1,x._2)).collectAsMap
   //Calculating prefix length for each row
   val prefix_length=lines.map(x=>(x(0).toInt,(x.length-1)-Math.ceil((x.length-1)*threshold).toInt+1))
   val combine=row_id.join(prefix_length).map({
     case(a,(b,c))=>(a,b.take(c))})
  //Calculating inverted index for tokens
   val id_token=row_id.flatMapValues(x=>x)
   val token_id=id_token.map({case(a,b)=>(b,a)})
    val all_indices=token_id.groupByKey().mapValues(_.toList)
    val token_prefix=combine.flatMapValues(x=>x)
    val prefix_token=token_prefix.map({case(a,b)=>(b,a)})
  val inverted_index=prefix_token.groupByKey().mapValues(_.toList)
  //Implementing prefix and positional filtering 
  val potential_candidates=inverted_index.map(pair=>(pair._1,pair._2.combinations(2).toList))
  val alphas=potential_candidates.mapValues(a=>a.map(b=>overlap(doc_id_map(b(0))
      .toList,doc_id_map(b(1))
      .toList,threshold)))
  //ubound = 1+min(|x|-1, |y|-i)    
  val ubound = potential_candidates.map(x=>(x._1,x._2.map(y=>(1 + Math.min(doc_id_map(y(0)).length - doc_id_map(y(0)).indexOf(x._1.toInt), doc_id_map(y(1)).length - doc_id_map(y(1)).indexOf(x._1.toInt))))) )

  val merge=ubound.join(alphas).map{case(a,(b,c))=>(a,b zip c)}
  val comparisons=merge.mapValues(a=>a.map(b=> if(b._1>=b._2){b._2} else{-1}))
  val positional_prefix=potential_candidates.join(comparisons)
  .map{case(a,(b,c))=>(a,b zip c)}
  val positional_prefix_a=positional_prefix.mapValues(x=>x.filter(y=>y._2> -1))
  val candidates = positional_prefix_a.mapValues(x=>x.map(y=>y._1))

  val final_pairs = candidates.values.map(x=>x.map(y=> y match {
    case List(a,b)=> (a,b)})).flatMap(j=>j).distinct()
 
  val similarity_jaccard=final_pairs.map({case(a,b)=>((a,b),jaccard(doc_id_map(a).toSet,doc_id_map(b).toSet))})
  val final_answer=similarity_jaccard.filter{case((a,b),c)=>c!=None}
  
  //Final output sorting and writing
  val orderd_keys = final_answer.map(x => (if (x._1._1<x._1._2)(x._1) 
      else (x._1._2, x._1._1), x._2))
	val sorted = orderd_keys.sortByKey()
	val output = sorted.map(x => x._1 + "\t" + x._2)
  val format=output.coalesce(1).saveAsTextFile(outputFolder)
 
  }
}