python3 delete_column.py cab_rides.csv filtered_cab_rides.csv
hadoop fs -mkdir validation
hadoop fs -put filtered_cab_rides.csv validation
hadoop jar uberDistance-1.jar DistanceCount validation validationOutput
