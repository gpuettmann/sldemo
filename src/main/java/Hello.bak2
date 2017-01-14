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

public class Hello implements RequestHandler<Request, Response>{
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
		String function_name = "gpfanoutbottom";
		String function_input = "13";
		
		System.out.println("\nHere we are");
	
		AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
		InvokeRequest[] req = new InvokeRequest[1000];
		Future[] future_res = new Future[1000];
	
		for(int i = 0; i < 1000; i++) {
			function_input = String.valueOf(i);
			System.out.println("\nExecute: " + function_input);
			req[i] = new InvokeRequest()
				.withFunctionName(function_name)
				.withPayload(ByteBuffer.wrap(function_input.getBytes()));
			future_res[i] = lambda.invokeAsync(req[i], new AsyncLambdaHandler());
		}
		//Future<InvokeResult> future_res = lambda.invokeAsync(req[1], new AsyncLambdaHandler());
		/*	
		AWSLambdaAsync lambda = AWSLambdaAsyncClientBuilder.defaultClient();
		InvokeRequest req = new InvokeRequest()
			.withFunctionName(function_name)
			.withPayload(ByteBuffer.wrap(function_input.getBytes()));
		
		Future<InvokeResult> future_res = lambda.invokeAsync(req, new AsyncLambdaHandler());
*/
		
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
	
	public String myBottom(int myCount, Context context) {
		return String.valueOf(myCount);
	}

	public String test(int myCount, Context context) {
		FileWriter fileWriter = null;
		try {
			for (int i = 0; i < 1000; i++){
				fileWriter = new FileWriter("/tmp/test.csv");
				fileWriter.append("4711,4712,abcdefg,100\n");
			}
		} catch (Exception e) {
			System.out.println("Error in csvFileWriter");
	  		e.printStackTrace();
		} finally {	
			try {
				fileWriter.flush();
				fileWriter.close();	
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter");
				e.printStackTrace();
			}
		}
		return "success";
	}

	public Response handleRequest(Request request, Context context) {
		AmazonS3 s3 = new AmazonS3Client();
	 	try {
			System.out.println("Uploading a new object to S3 " + request.bucketName + " - " + request.fileName);
			s3.putObject(new PutObjectRequest(request.bucketName, request.fileName, createSampleFile()));
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
									                    + "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}  catch (IOException e) {
			System.out.println("Error while flushing/closing fileWriter");
			e.printStackTrace();
		}
		return new Response("Success");
	}		

	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("data", ".csv");
		file.deleteOnExit();
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		for (int i = 0; i < 100000; i++) {
			writer.write("4711,4712,abcdefg,100\n");
		}
		return file;
	}
}
