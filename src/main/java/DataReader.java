package fanout;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.s3.AmazonS3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.io.File;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import java.io.BufferedReader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DataReader implements RequestStreamHandler {
   String result = "First Line";

	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
		AmazonS3 s3 = new AmazonS3Client();
  		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject)jsonParser.parse(new InputStreamReader(inputStream, "UTF-8"));
			
			System.out.println("Call readRecords: " + jsonObject);

			String bucketName = (String) jsonObject.get("bucketName");
			String fileName = (String) jsonObject.get("fileName");
			String searchKey = (String) jsonObject.get("searchKey");
			
			//System.out.println("DownloadObject: " + fileName + ", search for key: " + searchKey);

			S3Object object = s3.getObject(new GetObjectRequest(bucketName, fileName));
			BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			Writer writer = new OutputStreamWriter(outputStream);
		
			String line = reader.readLine();
			String[] record;
		
			while (line != null) {
				record = line.split(",");
				if (record[0].compareTo(searchKey) == 0) {
					writer.write(line + "\n");
				}
				line = reader.readLine();
			}
			writer.close();
  		  
		} catch (AmazonServiceException ase) {
			System.out.println("AmazonServiceException:" +ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("AmazonClientException" + ace.getMessage());
		} catch (IOException e) {
			System.out.println("Error while flushing/closing fileWriter");
			e.printStackTrace();
		} catch (ParseException e) {
			System.out.println("Error while parsing json");
			e.printStackTrace();
		} 
	}	
}
