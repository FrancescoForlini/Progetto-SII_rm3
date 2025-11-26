
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
import matplotlib.pyplot as plt
import community as community_louvain

pd.set_option('display.max_colwidth', None)





query_path = ".\\nba_players_datasets\\nba_players_from_2023_dataset.json"


# loading dataset
df = pd.read_json(query_path)

players_tuple_list = [ (index, player)  for index, player in zip(df.index, df['Player']) ]

scaler = StandardScaler()

# scaled feature
to_scale = ['ORtg', 'DRtg']
to_scale = ['PTS', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'ORtg', 'DRtg', 'PER']

scaled_list = scaler.fit_transform(df[to_scale]).tolist()

similarity_matrix = cosine_similarity(scaled_list) 

G = nx.Graph()

# building player similarities  G
for i, t1 in enumerate(players_tuple_list):
    for t2 in players_tuple_list[i+1:]:
        if similarity_matrix[t1[0], t2[0]] > 0.80:   # treeshold
            G.add_edge(t1[1], t2[1], weight=similarity_matrix[t1[0], t2[0]])

not_sim_players = list(set(df.index) - set(G.nodes))
[ G.add_node(not_sim_player) for not_sim_player in df.loc[not_sim_players, 'Player'] ]



nx.write_graphml(G, "players_similarity.graphml")
#nx.write_gexf(G, "player_similarity1.gexf")
nx.write_gml(G, "players_similarity.gml")




