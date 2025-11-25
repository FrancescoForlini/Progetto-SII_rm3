
import logging
import pandas as pd # type: ignore
import weaviate
from weaviate.classes.query import MetadataQuery





logging.basicConfig(level=logging.INFO, format="- %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

i = 0


outputs_path = "..\\outputs\\weaviate_queries_ollama_output.txt"

ad_p_100_p_extended_path = "..\\nba_players_datasets\\nba_players_from_2023_dataset.json"
df = pd.read_json(ad_p_100_p_extended_path)

collection_name = "NBA_players_from_2023"

queries = [
    
    "Creative guard with elite IQ, advanced playmaking, mastered pick-and-roll and excellent vision and control. Someone who can direct the offense ",
    
    "High level scorer with deadly mid-range and consistent three-point accuracy. Someone with shot creation from all levels, also under pressure. Clutch player",
    
    "Tenacious forward with strong defensive instincts and impressive rebound skills. Someone who can who can switch on multiple positions ",
    
    "Explosive wing with exceptional speed, agility, and finishing at the rim through contact above the rim. Someone who can cut and run transitions",

    "Reliable defender with physical presence, strong rebounding instincts and rim protection. Someone who can dominate in the paint offensvely and defensevly ",

    "Perimeter shooter with fast release, consistent accuracy and excellent off-ball movement. Someone who can space the floor with his off-ball gravity",


    "An elite guard with exceptional IQ, excellent pick-and-roll and superb vision. Midrange mastery. Directs offense with precision and applies defensive pressure effectively.",

    "Guard with elite catch-and-shoot ability, spacing gravity, off-ball movement, and efficient midrange scoring. Can defend wings effectively.",

    "Elite offensive creator with advanced isolation scoring, step-back shooting, off-ball gravity to generate spacing, pick-and-roll mastery, and passing vision to create opportunities for multiple teammates.",

    "Guard with elite catch-and-shoot ability, spacing gravity, off-ball movement, and efficient midrange scoring. Can defend wings effectively.",

    "A generational playmaker with elite strength, vision, and versatility (clutch player). Dominates as a passer, scorer, and defender in multiple roles.",

]

logger.info("Connecting to Weaviate ...")

with weaviate.connect_to_local() as client:
    with open(outputs_path, "w", encoding="utf=8") as f:

        collection = client.collections.use(collection_name)
        logger.info("OK! Let's do some queries ...\n")

        #hybrid
        for i, query in enumerate(queries):
            f.write(f"\n\t{i+1} query: {queries[i]}\n")
            
            
            response = collection.query.hybrid(query, alpha=0.3, return_metadata=MetadataQuery(distance=True)) # Balance between vector and keyword search

            for j, res in enumerate(response.objects):
                properties_dict = res.properties
                f.write(f"\t{j+1}) {properties_dict['player']} born on {properties_dict['birthday']}. {properties_dict['summary']}\n")

        f.write(f"\n\n***************************************************************************************************************************************************************************************\n")
        f.write(f"***************************************************************************************************************************************************************************************\n\n")

        #bm25
        for i, query in enumerate(queries):
            f.write(f"\n\t{i+1} query: {queries[i]}\n")
            
            
            response = collection.query.bm25(query, return_metadata=MetadataQuery(distance=True)) # Balance between vector and keyword search

            for j, res in enumerate(response.objects):
                properties_dict = res.properties
                f.write(f"\t{j+1}) {properties_dict['player']} born on {properties_dict['birthday']}. {properties_dict['summary']}\n")


# client.close() not needed because the `with` handles it




 