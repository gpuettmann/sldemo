package fanout;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.PutObjectRequest;

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

public class DataGenerator {
	Writer writer;
	int numCallBacks = 0;

	private class AsyncLambdaHandler implements AsyncHandler<InvokeRequest, InvokeResult> {
   	public void onSuccess(InvokeRequest req, InvokeResult res) {
			try {
         	ByteBuffer response_payload = res.getPayload();
         	System.out.println(new String(response_payload.array()));
   			writer.write(new String(response_payload.array()));  		
				numCallBacks = numCallBacks + 1;
			} catch (IOException e) {
				  e.printStackTrace();
			}
      }
        public void onError(Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
	 }

	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {

	  try {		  
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject)jsonParser.parse(new InputStreamReader(inputStream, "UTF-8"));

			String bucketName = (String) jsonObject.get("bucketName");
			String fileName = (String) jsonObject.get("fileName");
			long fileCount = (Long) jsonObject.get("fileCount");
			long fileLength = (Long) jsonObject.get("fileLength");
			
			JSONObject jsonSub = new JSONObject();
			jsonSub.put("bucketName", bucketName);
			jsonSub.put("fileLength", fileLength);
	 		jsonSub.put("fileName", "dummy");

			System.out.println("WriteObject: " + jsonObject);
			
			String function_name = "gpfanoutwriter";

			AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
			InvokeRequest[] req = new InvokeRequest[(int)(long)fileCount];
			Future[] future_res = new Future[(int)(long)fileCount];
	
			writer = new OutputStreamWriter(outputStream);
			numCallBacks = 0;	
			for(int i = 0; i < fileCount; i++) {
				jsonSub.remove("fileName");
				jsonSub.put("fileName", fileName + "-" + String.valueOf(i));
				String function_input = jsonSub.toJSONString();
				System.out.println("\nExecute: " + function_input);
				req[i] = new InvokeRequest()
					.withFunctionName(function_name)
					.withPayload(ByteBuffer.wrap(function_input.getBytes()));
				future_res[i] = lambda.invokeAsync(req[i], new AsyncLambdaHandler());
			}
			
			while (numCallBacks < fileCount) {
				Thread.sleep(100);
			}
			writer.close();
	   } catch (ParseException e) {
			System.out.println("Error while parsing json");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error while flushing/closing fileWriter");
			e.printStackTrace();
		} catch (InterruptedException e) {
				System.err.println("\nThread.sleep() was interrupted!");
				System.exit(0);
		}	
	}
}
