package hw3.elastic_map_red;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;


//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListClustersResult;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.GetSubscriptionAttributesResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class EMRLauncher{

	static AmazonElasticMapReduceClient emrClient = null;

	public static void init(){

      Region usEast1 = Region.getRegion(Regions.US_EAST_1);
    	AWSCredentials credentials = null;
    	
    	try{
    	    credentials = new ProfileCredentialsProvider("Peter").getCredentials();
    	}catch( Exception e ){
    	    System.out.println("Cannot load credentials from credentials profile file"+
    					    "Please make sure that your credentials file is at the correct"+
    					    "Location (/Users/wakahiu/.aws/credentials), and is in valid format"+e);
            System.exit(-1);
    	}
    	emrClient = new AmazonElasticMapReduceClient(credentials);
      emrClient.setRegion(usEast1);
    }

    public static void main( String [] args) throws IOException{
      init();

      String bucketName = "s3n://" + args[0];
      String logName = bucketName + "/log-"+UUID.randomUUID() + ".log";

      System.out.println("Logging files to:\t" + logName );
      List<String> inputArgs = new ArrayList<String>();

      inputArgs.add(bucketName+"/input");
      //inputArgs.add(bucketName+"/output");
      inputArgs.add(bucketName+"/output-" + UUID.randomUUID() );

      /*
      * Cluster set-up.
      */
      HadoopJarStepConfig hadoopJarStepConfig = new HadoopJarStepConfig()
        .withJar(bucketName+"/ccbd-1.0-SNAPSHOT.jar")
        .withMainClass("hw3.elastic_map_red.WordCount")
        .withArgs( inputArgs );

      StepConfig stepConfigTweeterMapRed = new StepConfig()
        .withName("stepConfigHW3TweeterMapRed")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(hadoopJarStepConfig);

      RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest()
        .withName("ccbdHW3_map_red")
        .withLogUri(logName)
        .withSteps(stepConfigTweeterMapRed)
        .withInstances(  new JobFlowInstancesConfig()
          //.withEc2KeyName("/Users/wakahiu/.aws/PeterAmazon.pem")
          .withHadoopVersion("1.0.3")
          .withInstanceCount(3)
          .withKeepJobFlowAliveWhenNoSteps(false)
          .withPlacement( new PlacementType()
            .withAvailabilityZone("us-east-1a"))
          .withMasterInstanceType("m1.small")
          .withSlaveInstanceType("m1.small"));
     
      /*
      * Describe cluster.
      */
      ListClustersResult listClustersResult = emrClient.listClusters();

      System.out.println( " Listing Clusters " );
      System.out.println( " \t Name \t\t\t ID \t\t\t state \t\t\t Message" );
      for( ClusterSummary clusterSummary : listClustersResult.getClusters() ){
        System.out.print( " - " + clusterSummary.getName() );
        System.out.print( " \t " + clusterSummary.getId() );
        System.out.print( " \t " + clusterSummary.getStatus().getState() );
        System.out.print( " \t " + clusterSummary.getStatus().getStateChangeReason().getMessage() );
        System.out.println();
      }

      /*
      * Run the job flow.
      */
      RunJobFlowResult runJobFlowResult = emrClient.runJobFlow(runJobFlowRequest);
      System.out.println("Job id: " + runJobFlowResult.getJobFlowId());

      /*
      * Check the status of the running job.
      */
      int i = 0;
      STATUS_LOOP: while(true){

        /*
        DescribeJobFlowRequest describeJobFlowRequest = new DescribeJobFlowRequest()
          .withJobFlowIds(runJobFlowResult.getJobFlowId())
          
        DescribeJobFlowResult describeJobFlowResult = emrClient.describeJobFlows(describeJobFlowRequest);

        ListStepsResult listStepsResult = emrClient.ListSteps( listStepsRequest );

        StepExecutionStatusDetail stepExecutionStatusDetail = stepDetail.getExecutionStatusDetail();

        String state = stepExecutionStatusDetail.getState();

        System.out.println(state);

        Thread.sleep(500);

        */

        if( i > 10) break;
        i++;
      }

    }
}