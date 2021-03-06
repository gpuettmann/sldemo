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

public class DataComposer {
   String result = "First Line";

	private class AsyncLambdaHandler implements AsyncHandler<InvokeRequest, InvokeResult> {
   	public void onSuccess(InvokeRequest req, InvokeResult res) {
			System.out.println("\nLambda function returned:");
         ByteBuffer response_payload = res.getPayload();
         System.out.println(new String(response_payload.array()));
     		result = result + "\n" + new String(response_payload.array());
			//    System.exit(0);
      }
        public void onError(Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
	 }

	public String myHandler(int myCount, Context context) {
		String function_name = "gpfanouttest";
		String function_input =  "{\"bucketName\": \"gpfanout\", \"fileName\": \"data\"}";

		System.out.println("\nHere we are");
	
		AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
		InvokeRequest[] req = new InvokeRequest[100];
		Future[] future_res = new Future[100];
	
		for(int i = 0; i < 100; i++) {
			function_input = "{\"bucketName\": \"gpfanout\", \"fileName\": \"data-" + String.valueOf(i) + "\"}";
			System.out.println("\nExecute: " + function_input);
			req[i] = new InvokeRequest()
				.withFunctionName(function_name)
				.withPayload(ByteBuffer.wrap(function_input.getBytes()));
			future_res[i] = lambda.invokeAsync(req[i], new AsyncLambdaHandler());
		}
		
		System.out.println("\nAfter Calling");

		while (!future_res[1].isDone() && !future_res[1].isCancelled()) {
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				System.err.println("\nThread.sleep() was interrupted!");
				System.exit(0);
			}
		}
			try {
				Thread.sleep(10000);
			}
			catch (InterruptedException e) {
				System.err.println("\nThread.sleep() was interrupted!");
				System.exit(0);
			}
		return result;
	}
}
