package hw3.elastic_map_red;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> negWords = new HashSet<String>();
    private Set<String> posWords = new HashSet<String>();

    /*
    * Read a text file of positive and negative words for sentiment analysis.
    */
    protected void setup(Context context) throws IOException, FileNotFoundException{

      //http://pastebin.com/48nHpCtR
      //http://pastebin.com/AZpSYTHW
      List<String> tokenList = splitter(DictionaryContants.negStr);

      for(String word : tokenList ){
      //System.out.print(word + ",");
        negWords.add(word);
      }

      tokenList = splitter(DictionaryContants.posStr);

      for(String word : tokenList ){
      //System.out.print(word + ",");
        posWords.add(word);
      }

    }

    protected List<String> splitter(String line){

      List<String> tokenList = new ArrayList<String>();

      //Intested in the reature vector.
      String [] tokens = line.split("\\W");

      for( String token : tokens ){

          if(token.length() <= 2)
            continue;

        tokenList.add(token);
      }

      return tokenList;

    }



    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String message = value.toString();
      
      List<String> hashTagList = getHashTags(message);

      if(hashTagList.isEmpty() )  return;

      //SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();

      //int sentiment = sentimentAnalyzer.findSentiment(message);

      int sentiment = getSentiment(message);

      //System.out.println(sentiment);

      IntWritable s = new IntWritable(sentiment);

      for( String hashTag : hashTagList ) {
        word.set( hashTag );
        context.write(word, s);
      }
    }

    public List<String> getHashTags(String message){
      List<String> hashTagList = new ArrayList<String>();

      Pattern pattern = Pattern.compile("#(\\w+)");
      Matcher matcher = pattern.matcher(message);

      while(matcher.find()){
        hashTagList.add(matcher.group(1));
      }
      return hashTagList;
    }

    public int getSentiment(String message){
        List<String> tokenList = splitter(message);
        int sentiment = 0;
        
        for(String word : tokenList){
          if( posWords.contains(word) )
            sentiment++;
          if( negWords.contains(word) )
            sentiment--;
        }
        return sentiment;
    }

    //Not proud of this.

  }