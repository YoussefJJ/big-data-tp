hadoop fs -get validationOutput/part-r-00000 processed_data
java -cp "$KAFKA_HOME/libs/*":. DataSender Test-Kafka
