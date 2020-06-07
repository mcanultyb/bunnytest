# bunnytest
Technical exercise.


I didn't have time to set up a hosting account and figure out how to deploy a container to one, which I think is what this is asking me to do.  I used the off-the-shelf image from dockerhub with rabbitMQ on a local installation of docker:

docker pull rabbitmq
docker run -d -p5672:5672 --hostname my-rabbit --name some-rabbit2 rabbitmq:3

The source code assumes a local environment with default ports / account credentials for rabbitMQ.



