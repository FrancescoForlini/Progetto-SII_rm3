
import pandas as pd  # type: ignore
import ast
from elasticsearch import Elasticsearch
import logging





logging.basicConfig(
    level=logging.INFO, format="= %(levelname)s = %(message)s"
)

logger = logging.getLogger(__name__) 

ad_p_100_p_extended_path = "..\\nba_players_datasets\\nba_players_from_2023_dataset.json"
query_path = "..\\nba_players_datasets\\elasticsearch_queries.json"

outputs_path = "..\\outputs\\elasticsearch_queries_output.txt"

queries_df = pd.read_json(query_path, orient="records")

es = Elasticsearch("http://localhost:9200")  # Elasticsearch([{'host': 'localhost', 'port': 9200}])

logger.info("Starting Elasticsearch ...")

if not es.ping():
    logger.info("Shuckes! Something went wrong")
    raise Exception("Failed to launch Elasticsearch.")

logger.info("So far so good. Now it's time to level up, so let's try some queries")
 
index_name = "nba_players_until_2023"


with open(outputs_path, "w", encoding="utf=8") as f:
    f.write(f"\n")
    for i, row in queries_df.iterrows():

        f.write(f"================================================================================================================================== \n")
        f.write(f"\tQuery : {row['natural_language_query']} \n================================================================================================================================== \n")
 
        es_query = row['json_format_query']
        res = es.search(index=index_name, body=es_query)
        hits = res['hits'] 
        

        j = 1
        if hits['total']['value']> 0:  # there are some documents retrived 
            
            for docs_hit_dict in hits['hits']:
                hit = f"{str(j)}) {docs_hit_dict['_source']['Player']}. " if 'Player' in list(docs_hit_dict['_source'].keys()) else f"{str(j)}) "
                j = j + 1

                for key, val in docs_hit_dict['_source'].items():
                    if key != 'Player' :
                        hit += f"{key}: {', '.join(val)}| " if isinstance(val, list) else f"{key}: {str(val)}| " 

                f.write(f"\t\t{hit} \n\n")


        if "aggregations" in res.keys(): # for aggregations
            buckets = res["aggregations"]["top_school"]["buckets"]

            if len(buckets)> 0:  # there are some documents retrived for aggregations
                [ f.write(f"\t\t{b['key']}: {b['doc_count']}\n\n") for b in buckets[:2] ] 

                
logger.info("We are done!") 




