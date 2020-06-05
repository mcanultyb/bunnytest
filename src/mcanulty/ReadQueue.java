package mcanulty;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

public final class ReadQueue {
	
	private static final JsonParser parser = new JsonParser();
	private static final Map<Integer,Map<String,TitlesAndSales>> titlesAndSalesByYear = new HashMap<>();
	
	private static final class TitlesAndSales {
		private BigDecimal sales = new BigDecimal(0).setScale(2, RoundingMode.HALF_UP);
		private final Set<String> titles = new HashSet<>();
		
		private void addSales(final String title, final BigDecimal sales) {
			this.sales = this.sales.add(sales);
			titles.add(title);
		}
		
		private int getTitleCount() {
			return titles.size();
		}
		
		private BigDecimal getTotalSales() {
			return sales;
		}
	}
	
	public static void main(final String[] args) throws Exception {
		
		//connect to RabbitMQ
		
		ConnectionFactory factory = new ConnectionFactory();
		// "guest"/"guest" by default, limited to localhost connections
		factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);

		Connection conn = factory.newConnection();
		//and summarise them
		final BigDecimal[] totalSales = new BigDecimal[] { new BigDecimal(0).setScale(2, RoundingMode.HALF_UP) };
		
		Channel channel = conn.createChannel();
		final Consumer consumer = new DefaultConsumer(channel) {
//			
//			@Override
//			public void handleShutdownSignal(String s, ShutdownSignalException ex) {
//				//ex.printStackTrace();
//				//System.out.println(s);
//				
//				
//			}
			
			
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				
				long deliveryTag = envelope.getDeliveryTag();
				
				final String jsonString = new String(body);
				//System.out.println("Received: "+jsonString);
				
				if (!WriteQueue.TERMINATOR.equals(jsonString)) {  //TODO haven't found the secret way of saying we are closing down politely and not to expect any further data - can't close channel from client as that causes issues when ack'ing
					
					//keep pulling messages off while we can
					final JsonElement parsed = parser.parse(jsonString);
					
					
					
					if (parsed.isJsonObject()) {
						JsonObject jsonObj = parsed.getAsJsonObject();
						
						//not expecting anything complex so no real need for further checks but of course we can be defensive here
						final String yearText = jsonObj.get("Year").getAsString();
						
						Integer year = Integer.valueOf(0);
						try {
							year = Integer.valueOf(yearText);	
						}
						catch (final NumberFormatException nfe) {
							//System.err.println("Unexpected data for year : "+yearText);
						}
						
						Map<String,TitlesAndSales> annualCounts = titlesAndSalesByYear.get(year);
						
						if (annualCounts == null) {
							//first time we've seen data in this year
							annualCounts = new HashMap<>();
							titlesAndSalesByYear.put(year, annualCounts);
						}
						
						final String genre = jsonObj.get("Genre").getAsString();
						
						TitlesAndSales tracker = annualCounts.get(genre);
						if (tracker == null) {
							annualCounts.put(genre, tracker = new TitlesAndSales());
						}
						
						final String title = jsonObj.get("Name").getAsString().toUpperCase();
						
						final BigDecimal sales = jsonObj.get("Global_Sales").getAsBigDecimal();
						tracker.addSales(title, sales);
						totalSales[0] = totalSales[0].add(sales);
					}
					channel.basicAck(deliveryTag, false);
				}
				else {
					//all done
					//probably a nicer way of doing this but....
					
					final List<Integer> keys = new ArrayList<>(titlesAndSalesByYear.keySet());
					Collections.sort(keys);
					
					for (final Integer year : keys) {
						System.out.println("============================");
						System.out.println("           "+year);
						
						final Map<String,TitlesAndSales> summary = titlesAndSalesByYear.get(year);
						
						BigDecimal annualSales = new BigDecimal(0).setScale(0, RoundingMode.HALF_UP);
						
						for (final Map.Entry<String,TitlesAndSales> entry : summary.entrySet()) {
							System.out.println("----------------------------------");
							System.out.println("Genre : "+entry.getKey());
							System.out.println("Titles: "+entry.getValue().getTitleCount());
							System.out.println("Sales : "+entry.getValue().getTotalSales());
							annualSales = annualSales.add(entry.getValue().getTotalSales());
							
						}
						System.out.println("----------------------------------");
						System.out.println("Total sales for year : "+annualSales);
						System.out.println("----------------------------------");
					}
					System.out.println("\r\n============================");
					System.out.println("Total sales (all genres, all years) : "+totalSales[0]);

					//close off connection....somehow
					
				}
			}
		};
		
		channel.basicConsume(WriteQueue.QUEUE_NAME, false	, "myConsumerTag", consumer);
	}

}
