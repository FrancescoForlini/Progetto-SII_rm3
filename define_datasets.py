
import pandas as pd
import ast
import json





def modify_duplicates1(df : pd.DataFrame, col: str):

    suffix = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X'] 

    duplicates_player_series = df.groupby(col, group_keys=False).filter(lambda x : len(x) > 1)[col]  # series with doubles player and indeces

    for _, group in duplicates_player_series.groupby(col):
        df.loc[group.index, col] += " " + pd.Series(suffix[:len(group)])

    return df


# # ['Geroge Adams', ... , 'George Adams', ... , 'George Adams'] becomes ['Geroge Adams', ... , 'George Adams I', ... , 'George Adams II']
def modify_duplicates(df : pd.DataFrame, col : str):  

    suffix = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']  # df['Player'][df['Player'].isin(duplicated_player_indeces_map.keys())] += 'I'

    duplicates_player = df[col].value_counts()  # df['Player'].duplicated()

    duplicates_player_indeces_map = { 
        player : list(df[df[col] == player].index) 
        for player in duplicates_player[duplicates_player > 1].index }  # ... in df.loc[duplicates_player, 'Player'].index

    for _, indices in duplicates_player_indeces_map.items():
        indices.pop(0)  # remain equal (with the use of duplicated that would have been unnecessary)
        for i, idx in enumerate(indices):
            df.loc[idx, col] += f' {suffix[i]}'

    return df





NBA_players_file_path = ".\\nba_players_datasets\\NBA_players.json"  # updated like 2025
ad_players_file_path = ".\\nba_players_datasets\\advanced_players.json"  # updated like 2023
per_100_poss_players_file_path = ".\\nba_players_datasets\\per_100_poss_players.json"  # updated like 2023

ad_p100p_nba_players_path = ".\\nba_players_datasets\\nba_players_from_2023_dataset.json"

# read the datasets
NBA_players_df = pd.read_json(NBA_players_file_path)  #  shape (5313, 21)
per_100_poss_players_df = pd.read_json(per_100_poss_players_file_path)  # shape (3467, 31)
ad_players_df = pd.read_json(ad_players_file_path)  # shape (3467, 29) 

# change names for those that are equal:
# ['Geroge Adams', ... , 'George Adams', ... , 'George Adams'] becomes ['Geroge Adams', ... , 'George Adams I', ... , 'George Adams II'] 
per_100_poss_players_df = modify_duplicates(per_100_poss_players_df, 'Player')
ad_players_df = modify_duplicates(ad_players_df, 'Player')
NBA_players_df = modify_duplicates(NBA_players_df, 'Name')

# merge players_advanced_stats_df with players_per_100_poss_stats_df
ad_p100p_merged_df = pd.merge(
    ad_players_df[['Player', 'PER', 'TS%', 'ORB%', 'DRB%' , 'AST%', 'STL%', 'BLK%', 'TOV%', 'BPM', 'WS', 'VORP']], 
    per_100_poss_players_df[['Player', 'G', 'GS', 'MP', 'FG', 'FGA', 'FG%', '3PA', '3P%', '2PA', '2P%', 'FTA', 
                             'FT%', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'PTS', 'ORtg', 'DRtg', 'From', 'To', 'Hoa']], 
    on='Player', 
    how='right'
    )

# 100 poss stas are reliable when players played at least 500-600 min, equals to 25 entirely games (40 m)
ad_p100p_merged_df = ad_p100p_merged_df[(ad_p100p_merged_df['G'] > 30) & (ad_p100p_merged_df['To'] >= 2023)]  

# merge the df for 'Height', 'Position', 'Weight', 'Birthday', 'School' (missing)
cols_missing = ['Name', 'Height', 'Position', 'Weight', 'Birthday', 'School']
ad_p100p_merged_df = pd.merge(
    NBA_players_df[cols_missing], 
    ad_p100p_merged_df, 
    left_on=['Name'],
    right_on=['Player'], 
    how='right')
ad_p100p_merged_df.drop(columns=["Name"], inplace=True)

# height in cm and weight in kg
ad_p100p_merged_df['Height_cm'] = (ad_p100p_merged_df['Height']* 2.54).round().astype('Int64')
ad_p100p_merged_df['Weight_kg'] = (ad_p100p_merged_df['Weight'] * 0.45359237).round().astype(float)

# sorting
cols = ['Player', 'Birthday', 'School', 'Height', 'Height_cm', 'Weight', 'Weight_kg', 'Position', 
        'From', 'To', 'G', 'GS', 'MP', 'FG', 'FGA', 'FG%', '2PA', '2P%', '3PA', '3P%', 
        'FTA', 'FT%', 'PTS', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'ORtg', 'DRtg', 'PER', 
        'TS%', 'ORB%', 'DRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'BPM', 'WS', 'VORP', 'Hoa']
ad_p100p_merged_df = ad_p100p_merged_df[cols]

# "GS" : null -> "GS" : 0 and so go on
text_cols = ['School', 'Birthday']
num_cols = ['GS', 'FG', 'FGA', 'FG%', '2PA', '2P%', '3PA', '3P%', 'FTA', 'FT%',
            'PTS', 'ORB', 'DRB', 'AST', 'STL', 'BLK', 'TOV', 'ORtg', 'DRtg',
            'PER', 'TS%', 'ORB%', 'DRB%', 'AST%', 'STL%', 'BLK%', 'TOV%',
            'BPM', 'WS', 'VORP']
ad_p100p_merged_df[text_cols] = ad_p100p_merged_df[text_cols].fillna("undefined")
ad_p100p_merged_df[num_cols] = ad_p100p_merged_df[num_cols].fillna(0)
ad_p100p_merged_df['Position'] = ad_p100p_merged_df['Position'].apply(lambda x: ast.literal_eval(x))
ad_p100p_merged_df['School'] = ad_p100p_merged_df['School'].apply(lambda x: ast.literal_eval(x) if x != "undefined" else ["an unknown one"])

# adding summaries 
player_summaries_path = ".\\nba_players_datasets\\player_summaries.json" #summaries 1 o nothing
summaries_dict = json.load(open(player_summaries_path, "r", encoding="utf-8"))
ad_p100p_merged_df['Summary'] = list(summaries_dict.values())


#print(ad_p100p_merged_df.columns)
#print(ad_p100p_merged_df.shape)
#[ print(row['Player']) for _, row in ad_p100p_merged_df.iterrows() ]


ad_p100p_merged_df.to_json(ad_p100p_nba_players_path, orient="records")




