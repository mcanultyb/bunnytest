# bunnytest
Technical exercise.


I didn't have time to set up a hosting account and figure out how to deploy a container to one, which I think is what this is asking me to do.  I used the off-the-shelf image from dockerhub with rabbitMQ on a local installation of docker:

docker pull rabbitmq

Then

docker run -d -p5672:5672 --hostname my-rabbit --name some-rabbit2 rabbitmq:3

The source code assumes a local environment with default ports / account credentials for rabbitMQ.

The main thing that I haven't been able to do (other than deploy the container, but I think thats just a case of finding a suitable host and giving my card details then deploying the RabbitMQ image direct from DockerHub?) is gracefully signal from the publisher to the subscriber(s) that no more data is to follow, which then tells the subscriber to total eveything up and shut down. 

I had assumed that closing off the channel/connection from the publisher would trigger some sort of callback on the subscriber implementation but I didn't get that working.  Probably missing something really simple there, ie: which method I needed to override from DefaultConsumer.  My assumption was that if the publisher shut down, I'd be told about it, and use that to trigger the calculation of the totals.  I ran into issues with ACKs not being able to be returned to the publisher etc.

So for now I am just sending a shared constant value as the message content to tell the subscriber that the publisher is finished.

The question wording was a little ambiguous around the totals :

"outputs the total sales and number of titles per genre in each year"

could mean "total sales, and in addition, the number of titles per genre" or "total sales AND titles/genre" per year

So I've gone for outputting:

- Sales & titles per genre per year

- Total annual sales

- Total sales for all years, all genres.

Output is sorted in chronological order (even though there was no requirement to do so) and I probably should have sorted the genres to make the output more consistent.

I used a few libraries just to make life a bit easier.  Apache CSV as I noticed Monsters, Inc would cause issues if I did a simple split on the line text, Google's JSON parser, RabbitMQ client and some logging (requried by RabBitMQ client I think).

I wasn't sure (as there was mention of microservice in the email, but not the details in the PDF) whether the expectation was to have a service running somewhere that you could hit, that sent the messages into the queue, and another which you could hit to pull the messages off the queue and process them, so I've done them as standalone Java classes.

To send the messages, run WriteQueue, and to read them use ReadQueue.

