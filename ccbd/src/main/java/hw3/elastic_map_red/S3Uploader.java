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

public class S3Uploader{

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
        String key = "MyObjectKey";
        boolean fileExists = false;

        try{

	    	System.out.println("Starting");

	    	/*
	    	* List the buckets
	    	*/
	    	System.out.println("Listing buckets");
	    	for( Bucket bucket : s3Client.listBuckets() ){
	    		System.out.println(" - " + bucket.getName());
	    		if( bucketName.equals(bucket.getName() )){
	    			fileExists = true;
	    		}
	    		if( bucket.getName().length() > 40 && !bucketName.equals(bucket.getName() ) ){
	    			//System.out.println(" * " + bucket.getName());
	    			//s3Client.deleteBucket(bucket.get""Name());
	    		}

	    	}

	    	/*
	         * Create a new S3 bucket if it does not exist - Amazon S3 bucket names are globally unique,
	         * so once a bucket name has been taken by any user, you can't create
	         * another bucket with that same name.
	         *
	         * You can optionally specify a location for your bucket if you want to
	         * keep your data closer to your applications or users.
	         */
	    	if( !fileExists ){
	    		bucketName = "ccbd-hw3-tweet-bucket-" + UUID.randomUUID();

	        	System.out.println("Creating bucket " + bucketName + "\n");
	        	s3Client.createBucket(bucketName);	    		
	    	}


	    	List<File> fileList = listFiles(new File("/Users/wakahiu/Documents/school/CCBD/HW3/ccbd/input"));

	    	System.out.println("Uploading input files ");
	    	for(File file : fileList){
	    		String keyName = "input/"+file.getName();
	    		System.out.println(" - " + keyName );

	    		s3Client.putObject( new PutObjectRequest(bucketName, keyName, file ) );
	    	}


            fileList = listFiles(new File("/Users/wakahiu/Documents/school/CCBD/HW3/ccbd/constants"));

            System.out.println("Uploading constant files ");
            for(File file : fileList){
                String keyName = "constants/"+file.getName();
                System.out.println(" - " + keyName );

                s3Client.putObject( new PutObjectRequest(bucketName, keyName, file ) );
            }

	    	/*
	    	* Upload the jar.
	    	*/
	    	File hadoopJar = new File("/Users/wakahiu/Documents/school/CCBD/HW3/ccbd/target/ccbd-1.0-SNAPSHOT.jar");
	    	System.out.println("Uploading the jar file " + hadoopJar.getName());
	    	s3Client.putObject( new PutObjectRequest(bucketName, hadoopJar.getName(), hadoopJar ) );

        
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


     /**
     * Iteratively searches through a directory and lists all files.
     * @args	Directory to be search iteratively.
     *
     * @return A list of files in a directory.
     *
     * @throws IOException
     */
    private static List<File> listFiles(File dir){
    	List<File> fileList = new ArrayList<File>();

    	for(File file : dir.listFiles()){
    		//Directory
    		if( file.isDirectory()){
    			listFiles(file);
    		}
    		//Regular file.
    		else{
    			fileList.add(file);
    		}
    	}
    	return fileList;
    }
}