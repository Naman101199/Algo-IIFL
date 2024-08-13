docker compose up -d
List topics: kafka-topics --list --bootstrap-server broker:29092 
Consume data: kafka-console-consumer --topic market_data_topic --bootstrap-server broker:9092 --from-beginning


Zookeeper: 2181
Kafka: 9092, 9101
Spark Master: 7077, 8080


Type	Protocol	Port Range	Source
Custom TCP Rule	TCP	2181	0.0.0.0/0
Custom TCP Rule	TCP	9092	0.0.0.0/0
Custom TCP Rule	TCP	9101	0.0.0.0/0
Custom TCP Rule	TCP	7077	0.0.0.0/0
Custom TCP Rule	TCP	8080	0.0.0.0/0



cd Downloads
scp -i algo-test.pem ~/.ssh/id_rsa* ec2-user@ec2-3-145-188-59.us-east-2.compute.amazonaws.com:~/.ssh/

ssh -i algo-test.pem ec2-user@ec2-3-145-188-59.us-east-2.compute.amazonaws.com

chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub

ssh -T git@github.com

git clone git@github.com:Naman101199/Algo-IIFL.git


nohup python jobs/API_Producer.py >/dev/null 2>&1
nohup python jobs/API_Consumer.py >/dev/null 2>&1