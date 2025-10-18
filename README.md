# Проверить коннекторы Debezium
curl http://localhost:8084/connectors

# Проверить топики
docker exec -it kafka-broker1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# Посмотреть сообщения в топике file_metadata
docker exec -it kafka-broker1 kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic pgserver.public.file_metadata \
  --from-beginning