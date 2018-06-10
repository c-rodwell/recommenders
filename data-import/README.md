Build the data-import jar with maven: `mvn clean package assembly:single`

Executing the jar: `java -jar crawler-jar-with-dependencies.jar`

Run with optional arg: `java -jar crawler-jar-with-dependencies.jar <number_of_users>`

number_of_users (optional) : MAX = 500000, MIN = 100, DEFAULT = 100

This program will collect all the required data and preprocess them before storing into Elasticsearch. 
It needs to be run successfully before running the recommender program.