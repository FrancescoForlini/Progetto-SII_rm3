
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import logging




def define_mapping():
    return {
        "mappings": {
            "properties": {
                "Player":        { "type": "text" },
                "Birthday":      { "type": "date", "format": "MMMM d, yyyy" },
                "School":        { "type": "keyword" },
                "Height":        { "type": "integer" },
                "Height_cm":     { "type": "integer" },
                "Weight":        { "type": "integer" },
                "Weight_kg":     { "type": "integer" },
                "Position":      { "type": "keyword" },
                "From":          { "type": "integer" },
                "To":            { "type": "integer" },
                "G":             { "type": "integer" },
                "GS":            { "type": "integer" },
                "MP":            { "type": "integer" },
                "FG":            { "type": "float" },
                "FGA":           { "type": "float" },
                "FG%":           { "type": "float" },
                "2PA":           { "type": "float" },
                "2P%":           { "type": "float" },
                "3PA":           { "type": "float" },
                "3P%":           { "type": "float" },
                "FTA":           { "type": "float" },
                "FT%":           { "type": "float" },
                "PTS":           { "type": "float" },
                "ORB":           { "type": "float" },
                "DRB":           { "type": "float" },
                "AST":           { "type": "float" },
                "STL":           { "type": "float" },
                "BLK":           { "type": "float" },
                "TOV":           { "type": "float" },
                "ORtg":          { "type": "float" },
                "DRtg":          { "type": "float" },
                "PER":           { "type": "float" },
                "TS%":           { "type": "float" },
                "ORB%":          { "type": "float" },
                "DRB%":          { "type": "float" },
                "AST%":          { "type": "float" },
                "STL%":          { "type": "float" },
                "BLK%":          { "type": "float" },
                "TOV%":          { "type": "float" },
                "BPM":           { "type": "float" },
                "WS":            { "type": "float" },
                "VORP":          { "type": "float" },
                "Summary":       { "type": "text" },
                "Combined_ad":   { "type": "float" },
                "Stats_text":    { "type": "text" }
            }
        }
    }


def build_stats_text(r):
    return str(
                f"{r['Player']} was born on {r['Birthday']}. "
                f"He is {r['Height_cm']} cm tall and weighs {r['Weight_kg']} kg. "
                f"He attended { ', '.join(r['School'])} before playing in the NBA as a { ', '.join(r['Position'])} from {r['From']} to {r['To']}. "
                f"During his career, he played {r['G']} games for a total of {r['MP']} minutes. "
                f"Per 100 possessions, he averaged {r['PTS']} points, shooting {r['FG%']*100:.1f}% from the field with a PER of {r['PER']} (league average is 15). "
                f"He attempted {r['3PA']} three-pointers, converting {r['3P%']*100:.1f}% of them, "
                f"and went to the free throw line {r['FTA']} times, making {r['FT%']*100:.1f}%. "
                f"His true shooting percentage (TS%) was {r['TS%']*100:.1f}%, accounting for 2P, 3P, and free throws. "
                f"He averaged {r['ORB']} offensive rebounds ({r['ORB%']}% of available) and {r['DRB']} defensive rebounds ({r['DRB%']}%). "
                f"In addition, he recorded {r['BLK']} blocks and {r['STL']} steals. "
                f"Per 100 possessions, he contributed {r['AST']} assists ({r['AST%']}% of potential assists) while committing {r['TOV']} turnovers. "
                f"This resulted in an offensive rating of {r['ORtg']} and a defensive rating of {r['DRtg']}. "
                f"Advanced stats include Box Plus Minus (BPM) of {r['BPM']}, Win Shares (WS) of {r['WS']}, "
                f"and Value Over Replacement Player (VORP) of {r['VORP']}."
            )


# for batching
def chuncked_iterable(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i: i+size]





logging.basicConfig(
    level=logging.INFO, format="- %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)



ad_p_100_p_extended_path = "..\\nba_players_datasets\\nba_players_from_2023_dataset.json"

df = pd.read_json(ad_p_100_p_extended_path)

df['Combined_ad'] = 0.25*df['ORtg'] + 0.25*(110 - df['DRtg']) + 0.2*df['WS'] + 0.15*df['BPM'] + 0.15*df['VORP']
df['Stats_text'] = df.apply(lambda x: build_stats_text(x), axis=1)
#print(df[['School', 'Position', 'Combined_ad', 'Stats_text']].head(10))

es = Elasticsearch("http://localhost:9200")

logger.info("Starting Elasticsearch ...")

if not es.ping():
    logger.info("Shuckes! Something went wrong")
    raise Exception("Failed to launch Elasticsearch.")

logger.info("So far so god")

index_name = "nba_players_until_2023"

mapping = define_mapping()

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    logger.info(f"index with name {index_name} created")

df_to_dict = df.to_dict(orient="records")  # list of dicts

# docs to be indexed
docs = [
    {
        "_index": index_name,
        "_id": idx,
        "_source": player
    }
    for idx, player in enumerate(df_to_dict)
        ]

batch_size = 100

logger.info("Let's start indexing ...")

for i, batch in enumerate(chuncked_iterable(docs, batch_size)): # iterating on batches
    
    success, failed = helpers.bulk(es, batch, stats_only=True)
    print(f"Successes: {success}, failures: {failed}")
    print(f"{i} batch done")

logger.info("Indexing done!")

# print(es.indices.get(index=index_name))

es.indices.refresh(index=index_name)
count = es.count(index=index_name)
print(count['count'])




