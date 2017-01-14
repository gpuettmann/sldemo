package fanout;

public class Request {
	String bucketName;
	String fileName;

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public Request(String bucketName, String fileName) {
		this.bucketName = bucketName;
		this.fileName = fileName;
	}

	public Request() {
	}
}	
