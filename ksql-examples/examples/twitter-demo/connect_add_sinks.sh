curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_TWITTER_LANG_PER_MIN_STREAM",
  "config": {
    "schema.ignore": "true",
    "topics": "TWITTER_LANG_PER_MIN_STREAM",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "TWITTER_LANG_PER_MIN_STREAM:twitter_lang_per_min",
    "connection.url": "http://localhost:9200"
  }
}'

curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_tweet_count_per_user_per_hour_stream",
  "config": {
    "schema.ignore": "true",
    "topics": "TWEET_COUNT_PER_USER_PER_HOUR_STREAM",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "TWEET_COUNT_PER_USER_PER_HOUR_STREAM:tweet_count_per_user_per_hour_stream",
    "connection.url": "http://localhost:9200"
  }
}'

curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_tweets_by_time",
  "config": {
    "schema.ignore": "true",
    "topics": "TWEETS_BY_TIME",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "TWEETS_BY_TIME:tweets_by_time",
    "connection.url": "http://localhost:9200"
  }
}'

curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_tweets",
  "config": {
    "schema.ignore": "true",
    "topics": "TWEETS_BY_TIME",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter.schemas.enable": false,
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "key.ignore": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "type.name": "type.name=kafkaconnect",
    "topic.index.map": "TWEETS_BY_TIME:tweets_by_time",
    "connection.url": "http://localhost:9200"
  }
}'


curl -X "POST" "http://localhost:8083/connectors/" \
     -H "Content-Type: application/json" \
     -d $'{
  "name": "es_sink_twitter",
  "config": {
    "topics": "twitter_json_01",
    "connection.url": "http://localhost:9200",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "type.name": "type.name=kafka-connect",
    "transforms": "ExtractId",
    "transforms.ExtractId.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractId.field": "Id"
  }
}'
