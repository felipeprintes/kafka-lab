from kafka import KafkaConsumer
import json
import ast

# Kafka connfig
#BROKER = ['localhost:9092']
TOPIC = 'meu-topico'
consumer = KafkaConsumer(TOPIC, group_id='grupo1', bootstrap_servers = 'localhost:9092')

#msg = ConsumerRecord(topic='meu-topico', partition=0, offset=8976641, timestamp=1604489070804, timestamp_type=0, key=None, value=b"{'frase': 'Eu tenho 24 anos'}", checksum=1810012771, serialized_key_size=-1, serialized_value_size=29)
#n_msg = json.loads(msg.values)
#print(msg)

## json.loads -> de string para um objeto json
## json.dumps -> de objeto json para string

for msg in consumer:
    print(msg)
    #txt = msg.value.decode('utf-8')
    #txt_json = json.loads(txt)
    #txt_json = ast.literal_eval(txt)
    #print(txt_json['frase'])
    #texto = json.loads(json.dumps(msg).value)
    #print("NOVO TEXTO: {}".format(texto))