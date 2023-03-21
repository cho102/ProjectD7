mvn clean package
spark-submit --class edu.ucr.cs.cs167.cho102.App --master "local[*]" target/ProjectD7-1.0-SNAPSHOT.jar Tweets_10k.json.bz2
spark-submit --class edu.ucr.cs.cs167.cho102.Task2 --master "local[*]" target/ProjectD7-1.0-SNAPSHOT.jar tweets_clean.json tweets_topic.json
spark-submit --class edu.ucr.cs.cs167.cho102.Task3 --master "local[*]" target/ProjectD7-1.0-SNAPSHOT.jar tweets_topic.json
