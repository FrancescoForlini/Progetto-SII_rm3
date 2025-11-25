
import time
import logging
import pandas as pd
import weaviate
from weaviate.classes.config import DataType, Configure, Property





def join_list(df: pd.DataFrame, columns: list):

    for column in columns:
        df[column] = df[column].apply(lambda x: ', '.join(x))      

    return df            





logging.basicConfig(
    level=logging.INFO, format="- %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

ad_p_100_p_extended_path = "..\\nba_players_datasets\\nba_players_from_2023_dataset.json"

df = pd.read_json(ad_p_100_p_extended_path) 
df = df[["Player", "Birthday", "School", "Height_cm", "Weight_kg", "Position", "From", "To", "Summary"]]

df = join_list(df, ['Position', 'School'])
# df = df.sample(frac=1, random_state=42).reset_index(drop=True)


logger.info("Starting Weaviate ...")

client = weaviate.connect_to_local()

print("Weaviate ready!") if client.is_ready() else print("Weaviate non ready")

time.sleep(5)

logger.info("So far so god")

logger.info("Defineing NBA_players_from_2023 collection")

properties = {
    "Player":       DataType.TEXT,
    "Birthday":     DataType.TEXT,
    "School":       DataType.TEXT,
    "Height_cm":    DataType.INT,
    "Weight_kg":    DataType.INT,
    "Position":     DataType.TEXT,
    "From":         DataType.INT,
    "To":           DataType.INT,
    "Summary":      DataType.TEXT
}


collection_name = "NBA_players_from_2023"

existing_collections = list(client.collections.list_all().keys())
if collection_name in existing_collections:
    client.collections.delete(collection_name)


client.collections.create(
    collection_name,
    vector_config=[
        Configure.Vectors.text2vec_transformers(
            name="summary_vector",
            source_properties=["Summary"],
            inference_url="http://t2v-transformers:8080" # "http://localhost:8080" === "http://t2v-transformers:8080" for multiple inference containers (client weaviate is in docker) 
        )
    ],
    properties=[
        Property(name="Player", data_type=DataType.TEXT),
        Property(name="Birthday", data_type=DataType.TEXT),
        Property(name="School", data_type=DataType.TEXT),
        Property(name="Height_cm", data_type=DataType.INT),
        Property(name="Weight_kg", data_type=DataType.INT),
        Property(name="Position", data_type=DataType.TEXT),
        Property(name="From", data_type=DataType.INT),
        Property(name="To", data_type=DataType.INT),
        Property(name="Summary", data_type=DataType.TEXT)
        ]
)


logger.info(f"Adding objects to {collection_name} collection")

docs = df.to_dict(orient="records")  # dict's list

collection = client.collections.use(collection_name)

with collection.batch.dynamic() as batch:
    for doc in docs:
        batch.add_object(
            properties=doc
    )
        
failed_objects = collection.batch.failed_objects

logger.info(f"Batching process finished with {len(failed_objects)} failed imports")

client.close()





# docker compose 
"""

version: '3.8'
name: search_engine
services:

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.2
    container_name: elasticsearch
    environment:
      - node.name=es01
      - cluster.name=docker-cluster
      - discovery.type=single-node
      #- ELASTIC_PASSWORD=********
      - xpack.security.enabled=false  # disabled security (no https)
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms1g -Xmx1g"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    #volumes:
     # - es_data:/usr/share/elasticsearch/data
    ports:
      - "9200:9200" # http
      - "9300:9300" #transport (interna custer)
  
  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.2
    container_name: kibana
    environment:
      ELASTICSEARCH_HOSTS: "http://elasticsearch:9200"
    ports:
      - "5601:5601"
    depends_on:
      - "elasticsearch"

  weaviate:
    image: semitechnologies/weaviate:latest
    container_name: weaviate
    environment:
      QUERY_DEFAULTS_LIMIT: 20
      #ENABLE_API: "true"
      PERSISTENCE_DATA_PATH: "/var/lib/weaviate"
      DEFAULT_VECTORIZER_MODULE: text2vec-transformers
      ENABLE_MODULES: text2vec-transformers
      TRANSFORMERS_INFERENCE_API: http://t2v-transformers:8080
      AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED: "true"
      ENABLE_GRPC: "true"
    depends_on:
    - text2vec-transformers
    ports:
      - "8080:8080"
      - "50051:50051"
    #volumes:
      #- weaviate_data:/var/lib/weaviate

  text2vec-transformers:
    container_name: t2v-transformers
    image: cr.weaviate.io/semitechnologies/transformers-inference:sentence-transformers-multi-qa-MiniLM-L6-cos-v1
    environment:
      ENABLE_CUDA: 0 # set to 1 to enable
      # NVIDIA_VISIBLE_DEVICES: all # enable if running with CUDA
    ports:
      - "8081:8080"


# volumes:
  # es_data:
    # driver: local
  # weaviate_data:
    # driver: local
    

"""  




