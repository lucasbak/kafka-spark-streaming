# Project setup

## Scala with maven

We will use Intellij 15.01 Community Edition
Scala 2.10.4
Spark 1.4.1
Maven 3.3
Sbt: 0.13.7

### Intellij Setup

* Download Intellij CE 15.01 Edition from website https://www.jetbrains.com/idea/download/
* Install Scala plugin

### Project Set up

#### Creation

* Menu -> New project  
* MVN Project
* Create from archetype net.alchim31.maven: scala-archetype-simple version:1.4
* Set groupId, artifactID, and that's it

#### Layout

First remove not necessary dependencies and test files
then add the plugin to create a jar which will contain  all the needed dependencies

* Remove all test files inside src/test/scala ( not needed for trying out )
* In pom.xml add inside
 - properties
    ```xml
		<properties>
			<maven.compiler.source>1.7</maven.compiler.source>
			<maven.compiler.target>1.7</maven.compiler.target>
			<encoding>UTF-8</encoding>
			<scala.version>2.10.4</scala.version>
		</properties>
		```
 - dependencies
    ```xml  
		<dependencies>
		  <dependency>
	          <groupId>org.apache.spark</groupId>
	          <artifactId>spark-streaming_2.10</artifactId>
	          <version>1.4.1</version>
	      </dependency>
	      <dependency>
	          <groupId>org.apache.spark</groupId>
	          <artifactId>spark-streaming-kafka_2.10</artifactId>
	          <version>1.4.1</version>
	      </dependency>
	  </dependencies>
    ```

  - plugin
  	```xml
  	<plugin>
      <artifactId>maven-assembly-plugin</artifactId>
      <configuration>
          <archive>
              <manifest>
                  <mainClass>fr.edf.dspit.Streamer</mainClass>
              </manifest>
          </archive>
          <descriptorRefs>
          <!-- this set the default main class to use when executed -->
              <descriptorRef>jar-with-dependencies</descriptorRef>
          </descriptorRefs>
      </configuration>
      <executions>
          <execution>
              <phase>package</phase>
              <goals>
                  <goal>single</goal>
              </goals>
          </execution>
      </executions>
    </plugin>
    ```