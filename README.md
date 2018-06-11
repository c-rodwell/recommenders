#Install dependencies
1. Clone the Last.fm API Bindings for Java project: https://github.com/jkovacs/lastfm-java
2. `cd` into the project directory and run `mvn install`

#Crawler

Build the jar with maven: `mvn clean package assembly:single`

Executing the jar: `java -jar crawler-jar-with-dependencies.jar`

Run with optional arg: `java -jar crawler-jar-with-dependencies.jar <number_of_users>`

This program will collect all the required data and preprocess them before storing into Elasticsearch. 
It needs to be run successfully before running the recommender program.

#Recommender

Build the jar with maven: `mvn clean package assembly:single`

Executing the jar: `java -jar recommender-jar-with-dependencies.jar`

Run with args: `java -jar crawler-jar-with-dependencies.jar [username] <number_of_recommendations>`

This program will get the list of recommendations for the username that was passed as program arg.