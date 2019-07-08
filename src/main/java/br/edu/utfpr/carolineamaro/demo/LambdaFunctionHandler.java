package br.edu.utfpr.carolineamaro.demo;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectModerationLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectModerationLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	private DynamoDB dynamoDB = new DynamoDB(client);

	private static String tableName = "Fotos";

	public LambdaFunctionHandler() {
	}

	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();

		AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

		DetectModerationLabelsRequest detect = new DetectModerationLabelsRequest()
				.withImage(new Image().withS3Object(new S3Object().withBucket(bucket).withName(key)))
				.withMinConfidence(50F);

		try {
			DetectModerationLabelsResult result = rekognitionClient.detectModerationLabels(detect);
			result.getModerationLabels().forEach(item -> {
				this.inserirNoDynamo(bucket, key, item.getConfidence());
			});

		} catch (AmazonRekognitionException e) {
			e.printStackTrace();
		}
		
		return event.toString();

	}

	private void inserirNoDynamo(String bucket, String name, double isAdult) {

		Table table = dynamoDB.getTable(tableName);
		try {
			Item item = new Item().withPrimaryKey("FotoID", bucket + name).withString("bucket", bucket)
					.withString("name", name).withDouble("isAdult", isAdult);
			table.putItem(item);

		} catch (Exception e) {
			System.err.println("Create item failed.");
			System.err.println(e.getMessage());
		}
	}
}