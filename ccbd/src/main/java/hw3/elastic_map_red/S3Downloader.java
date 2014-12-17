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
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

public class S3Downloader{

	static AmazonS3 s3Client = null;

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
    	s3Client = new AmazonS3Client(credentials);
      s3Client.setRegion(usEast1);
    }

    public static void main( String [] args) throws IOException{
      init();

      String bucketName = args[0];

      try{

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucketName);

        ObjectListing objectListing;

        do{
          objectListing = s3Client.listObjects(listObjectsRequest);

          for( S3ObjectSummary objectSummary : objectListing.getObjectSummaries() ){
            System.out.println(" - " + objectSummary.getKey() + " Size = " + objectSummary.getSize() );
          }

          listObjectsRequest.setMarker(objectListing.getNextMarker());

        }while(objectListing.isTruncated() );

      } catch (AmazonServiceException ase) {
          System.out.println("Caught an AmazonServiceException, which means your request made it "
                  + "to Amazon S3, but was rejected with an error response for some reason.");
          System.out.println("Error Message:    " + ase.getMessage());
          System.out.println("HTTP Status Code: " + ase.getStatusCode());
          System.out.println("AWS Error Code:   " + ase.getErrorCode());
          System.out.println("Error Type:       " + ase.getErrorType());
          System.out.println("Request ID:       " + ase.getRequestId());
      } catch (AmazonClientException ace) {
          System.out.println("Caught an AmazonClientException, which " +
              "means the client encountered " +
                  "an internal error while trying to " +
                  "communicate with S3, " +
                  "such as not being able to access the network.");
          System.out.println("Error Message: " + ace.getMessage());
      }


    }
}