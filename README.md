Required:

 1. Java
 2. Apache Maven
 3. Elasticsearch 6.2.4

Java Version:
`java version "1.8.0_172"`
`Java(TM) SE Runtime Environment (build 1.8.0_172-b11)`
`Java HotSpot(TM) 64-Bit Server VM (build 25.172-b11, mixed mode)`

How to download and install Apache Maven: https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html

How to download and install Elasticsearch: https://www.elastic.co/downloads/elasticsearch

**Install Dependencies**
1. Clone `Last.fm API Bindings for Java` - https://github.com/jkovacs/lastfm-java
2. From a terminal, run ``mvn install`` in the root directory of this project. It will install this dependency.

**Building data-import (crawler)**

1. Make sure the `Last.fm API Bindings for Java` dependency is installed already.
2. On the root directory of `data-import`, run ``mvn clean package assembly:single``. This will create a target directory with the ``crawler-jar-with-dependencies.jar`` inside.

**Building data-import (recommender)**

1. On the root directory of `hybrid-music-rec`, run ``mvn clean package assembly:single``. This will create a target directory with ``recommender-jar-with-dependencies.jar`` inside.

**Running crawler**
`java -jar crawler-jar-with-dependencies.jar `

**Running recommender**
`java -jar recommender-jar-with-dependencies.jar`