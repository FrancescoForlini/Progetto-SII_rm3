
import pandas as pd
from sklearn.preprocessing import StandardScaler
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx
import matplotlib.pyplot as plt
import community as community_louvain

pd.set_option('display.max_colwidth', None)





query_path = "..\\nba_players_datasets\\nba_players_from_2023_dataset.json"
sim_community_path = "..\\outputs\\similarity_communities_detection.txt"

# loading dataset
df = pd.read_json(query_path)

player_index = df.index.to_list()

players = df['Player'].to_list()

scaler = StandardScaler()

# scaled feature
to_scale = ['ORtg', 'DRtg']
to_scale = ['PTS', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'ORtg', 'DRtg', 'PER']

scaled_list = scaler.fit_transform(df[to_scale]).tolist()

similarity_matrix = cosine_similarity(scaled_list) 

G = nx.Graph()

# building player similarities  G
for i in range(len(player_index)):
    for j in range(i+1, len(player_index)):
        if similarity_matrix[i, j] > 0.80:   # treeshold
            G.add_edge(i, j, weight=similarity_matrix[i,j])

communities = nx.community.greedy_modularity_communities(G, weight="weight", cutoff=4, best_n=8)
#communities = community_louvain.best_partition(G, weight='weight', random_state=42)


# drawing the communities
supergraph = nx.cycle_graph(len(communities))
superpos = nx.spring_layout(supergraph, scale=2, seed=429)

# useing the "supernode" positions as the center of each node cluster
centers = list(superpos.values())
pos = {}
for center, comm in zip(centers, communities):
    pos.update(nx.spring_layout(nx.subgraph(G, comm),
                                 center=center, 
                                 seed=1430))
    
community_x_players_dict = {}

# nodes colored by cluster
for i, nodes in enumerate(communities):
    node_list = list(nodes)
    nx.draw_networkx_nodes(G, 
                           pos=pos, 
                           nodelist=node_list, 
                           node_size=50,
                           node_color=[i] * len(node_list), 
                           edgecolors="purple",
                           cmap=plt.cm.tab10, 
                           vmin=0,
                           vmax=len(communities) - 1)
    
    community_nodes_subgraph = G.subgraph(node_list).copy()
    sorted_community_nodes = sorted(community_nodes_subgraph, key=lambda x: G.degree[x] , reverse=True)  # 
    community_x_players_dict[i] = sorted_community_nodes


nx.draw_networkx_edges(G, pos=pos)

# central_nodes = sorted(G.degree, key=lambda x: x[1], reverse=True)[:20]
labels = {player_index_list[0]: df.loc[player_index_list[0], 'Player'] for _, player_index_list in community_x_players_dict.items()}

nx.draw_networkx_labels(G, pos=pos, labels=labels, font_size=10, font_color='#00BFFF')

# "unique" players (also drawing them)
not_sim_players = list(set(df.index) - set(G.nodes))
G_n = nx.cycle_graph(not_sim_players)
pos_G_n = nx.spring_layout(G_n, seed=42)

x_vals = [x for x, y in pos_G_n.values()]
y_vals = [y for x, y in pos_G_n.values()]
x_min, x_max = min(x_vals), max(x_vals)
y_min, y_max = min(y_vals), max(y_vals)

x_offset = -2.8 - min(x_vals)  # sposta a sinistra
y_offset = 2.8 - max(y_vals)   # sposta in alto

pos_shifted = {node: (x + x_offset, y + y_offset) for node, (x, y) in pos_G_n.items()}

nx.draw(G_n, pos=pos_shifted, node_color='violet', edgecolors="black", node_size=10, edge_color='cyan')

plt.tight_layout()
plt.show()


#df_not_sim_players = df.copy()
with open(sim_community_path, "w", encoding="utf=8") as f:
    for community, players in community_x_players_dict.items():

        f.write(f"{community+1} community. List of players:\n {list(df.loc[players, 'Player'])}")
        f.write(f"\n\n")

        #df_not_sim_players.drop(players, inplace=True)

    for player in not_sim_players:
        f.write(f"Player: {df.loc[player, 'Player']}\nSummary: {df.loc[player, 'Summary']}\n\n")


"""
# hits algorithm
auth = []
hub  = []

for i, nodes in enumerate(communities):
    node_list = list(nodes)
    sub_G = nx.subgraph(G, node_list)
    auth, hub = nx.hits(sub_G, max_iter=100, tol=1e-8)

    auth_v = [auth[n] for n in node_list]
    hub_v  = [hub[n] for n in node_list]

    auth.append(auth_v)
    hub.append(hub_v)

"""




