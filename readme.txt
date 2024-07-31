docker compose up -d
List topics: kafka-topics --list --bootstrap-server broker:29092 
Consume data: kafka-console-consumer --topic market_data_topic --bootstrap-server broker:9092 --from-beginning
