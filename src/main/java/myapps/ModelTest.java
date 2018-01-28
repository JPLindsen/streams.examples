package myapps;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

//import com.thoughtmechanix.simpleservice.GLM_model_R_1511970560428_1;

import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;

public class ModelTest {

	// Name of the generated H2O model
	//private static String modelClassName = "com.github.megachucky.kafka.streams.machinelearning.models.gbm_pojo_test";
	private static String modelClassName =  "myapps.models.GLM_model_R_1511970560428_1";

	// Prediction Value
	private static String modelPrediction = "unknown";
	
	public static void main(final String[] args) throws Exception {
		
		// Create H2O object (see gbm_pojo_test.java)
		hex.genmodel.GenModel rawModel;
		rawModel = (hex.genmodel.GenModel) Class.forName(modelClassName).newInstance();
		EasyPredictModelWrapper model = new EasyPredictModelWrapper(rawModel);
		//GLM_model_R_1511970560428_1 glmModel = new GLM_model_R_1511970560428_1(); // POJO model
        //EasyPredictModelWrapper model = new EasyPredictModelWrapper(glmModel);
		
		// Configure Kafka Streams Application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-modeltest");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		// In the subsequent lines we define the processing topology of the
		// Streams application.
		final KStreamBuilder builder = new KStreamBuilder();

		// Construct a `KStream` from the input topic "ModelInputTopic", where
		// message values
		// represent lines of text (for the sake of this example, we ignore
		// whatever may be stored
		// in the message keys).		
		final KStream<String, String> modelInputLines = builder.stream("ModelInputTopic");

		// Stream Processor (in this case 'foreach' to add custom logic, i.e. apply the analytic model)
		modelInputLines.foreach((key, value) -> {
	
				if (value != null && !value.equals("")) {
					System.out.println("#####################");
					System.out.println("Input Values:" + value);

					String[] valuesArray = value.split(",");

					RowData row = new RowData();
			        row.put("AGE", valuesArray[0]);
			        row.put("RACE", valuesArray[1]);
			        row.put("PSA", valuesArray[2]);
			        row.put("GLEASON", valuesArray[3]);
			        
					BinomialModelPrediction p = null;
					try {
						p = model.predictBinomial(row);
					} catch (PredictException e) {
						e.printStackTrace();
					}

					modelPrediction = p.label;
					
					System.out.println("Label (aka prediction) is: " + p.label);
					System.out.print("Class probabilities: ");
					for (int i = 0; i < p.classProbabilities.length; i++) {
						if (i > 0) {
							System.out.print(",");
						}
						System.out.print(p.classProbabilities[i]);
					}
					System.out.println("");
					System.out.println("#####################");
					
				}

			}
		);

		// Transform message: Add prediction information
		KStream<String, Object> transformedMessage = modelInputLines.mapValues(value -> "Prediction: " + modelPrediction);
		
		// Send prediction information to Output Topic
		transformedMessage.to("ModelOutputTopic");

		// Start Kafka Streams Application to process new incoming messages from Input Topic
		final KafkaStreams streams = new KafkaStreams(builder, props);
		streams.cleanUp();
		streams.start();
		System.out.println("Model Prediction Microservice is running...");
		System.out.println("Input to Kafka Topic 'ModelInputTopic'; Output to Kafka Topic 'ModelOutputTopic'");

		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka
		// Streams
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}