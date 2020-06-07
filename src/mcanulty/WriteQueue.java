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
	static final String TERMINATOR = "__DONE";
	
	private static final String filename = "vgsales.csv";
	
	public static void main(final String[] args) throws Exception{
		//assume file is local
		final File f = new File(WriteQueue.class.getResource(filename).toURI());
		
		//connect to RabbitMQ, assume its local, default credentials
		
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
				
				// next bit is just to tell receiver we're done. 
				// Tried closing the connection which seemed to cause issues with the receiver sending the ack.
				// however as soon as this is sent, the connection will be closed anyway via autoclose, so maybe
				// it was something else
				// could hook into ACK being returned, if it is by RabbitMQ, and once we've had 'sent' x ACK we 
				// could close things off, but not sure how to politely signal to the subscribers that nothing
				// else is going to be coming their way
				
				channel.basicPublish("", QUEUE_NAME,null,TERMINATOR.getBytes()); 
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
