java -cp "$KAFKA_HOME/libs/*":. Consumer Test-Kafka
cp /root/out.txt /root/hbase-code/tn/insat/validation/
cd hbase-code
java tn.insat.validation.ValidationHBase
