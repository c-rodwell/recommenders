#Recommender

Build the jar with maven: `mvn clean package assembly:single`

Executing the jar: `java -jar recommender-jar-with-dependencies.jar`

Run with args: `java -jar crawler-jar-with-dependencies.jar [username] <number_of_recommendations>`

This program will get the list of recommendations for the username that was passed as program arg.