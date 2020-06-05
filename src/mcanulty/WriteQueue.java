package mcanulty;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class WriteQueue {

	static final String QUEUE_NAME = "mcanulty-test";
	protected static final String TERMINATOR = "__DONE";
	private static final String filename = "vgsales.csv";
	
	public static void main(String[] args) throws Exception{
		
		final File f = new File(WriteQueue.class.getResource(filename).toURI());
		final ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		int sent = 0;
		try (final FileReader reader = new FileReader(f);
			 final CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
			 final Connection connection = factory.newConnection();
			 final Channel channel = connection.createChannel()) {
				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
				channel.queuePurge(QUEUE_NAME);  //just because I was having issues during testing, possibly because I had commented out ack'ing from receiver
				final List<String> headers = parser.getHeaderNames();
				final StringBuilder builder = new StringBuilder(1024);
			 
				for (final CSVRecord line : parser) {
					buildJSON(builder,line, headers);
					channel.basicPublish("", QUEUE_NAME, null, builder.toString().getBytes());
					sent++;
				}
				
				channel.basicPublish("", QUEUE_NAME,null,TERMINATOR.getBytes()); //just to tell receiver we're done, can't close connex as that causes ack issues
		}
		
		System.out.println("Sent "+sent+" rows from file "+f.getAbsolutePath());
	}
	
	private static void buildJSON(final StringBuilder builder, final CSVRecord line, final List<String> headers) {
		//could use a library but the structure is very simple, so just build locally
		
		builder.setLength(0); //clear content, reuse to avoid reallocation of memory
		builder.append("{");
		for (final String header : headers) {
			builder.append("\"").append(header).append("\":\"");
			builder.append(line.get(header));
			builder.append("\",");
		}
		builder.deleteCharAt(builder.length()-1); //trim trailing ','
		builder.append("}");
	}
}
