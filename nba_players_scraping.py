import pandas as pd
from io import StringIO

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import logging





# remove tuples without hrefs (with None instead in all rows)
def remove_tuples(df):  
    n_df = df.copy()
    
    for attr in n_df.columns:
        if all(isinstance(d1, tuple) for d1 in n_df[attr]) and all(d2[1] is None for d2 in n_df[attr]):
            n_df[attr] = [c[0] for c in n_df[attr]]

    return n_df # right


# "numbers" in effective numbers (float). N.B.: Wt can be empty
def int_numbers(df):  # only not tuple object (they don't have href) may be numbers
    n_df = df.copy()
    
    for k, series in enumerate([n_df[attr] for attr in n_df.columns]):
        
        if not any(isinstance(s, tuple) for s in series):
            series = series.astype(float)

    return n_df


# transformation table
def zoom_table(df, r):
    
    n_df = df.copy()

    n_df = n_df.loc[n_df['Season'] == 'Career']
    n_df = n_df.drop(columns=['Season', 'Tm', 'Lg'])
    n_df['From'] = r['From'].astype(int)
    n_df['To'] = r['To'].astype(int)
    n_df['Hof'] = [True if t[0][-1] == '*' else False for t in r['Player']]
    n_df['Player'] = r['Player'][0].replace('*', '')
    
    return n_df


def create_none_advanced_dataframe(row1):
    return pd.DataFrame({
        'Age': None, 'Pos': None, 'G': None, 'MP': None, 'PER': None, 'TS%': None, '3PAr': None, 'FTr': None,
        'ORB%': None, 'DRB%': None, 'TRB%': None, 'AST%': None, 'STL%': None, 'BLK%': None, 'TOV%': None, 'USG%': None,
        'Unnamed: 19': None, 'OWS': None, 'DWS': None, 'WS': None, 'WS/48': None, 'Boh2': None, 'OBPM': None,
        'DBPM': None, 'BPM': None, 'VORP': None, 'From': row1['From'].astype(int), 'To': row1['To'].astype(int),
        'Hof': False, 'Player': row1['Player'], 'Unnamed: 16': None, 'Unnamed: 21': None, 'Unnamed: 18': None,
        'Unnamed: 23': None
    })


# Taking statistics like per 100 possession statistics and avdanced statistics
def per_100_and_advanced_statistics(absolute_path, p_table, driv):
    df_per_100 = []
    df_ad = []
    
    for _, row in p_table.iterrows():  # for each plyer
        
        player, href = row['Player']
        logger.info(f"Let's go with player {player} from {row['From']}")
        driv.get(absolute_path + href)

        ################# Per_100_poss #################
        try: 
            dfp_t = driv.find_element(By.XPATH, '//table[@id="per_poss"]')
        except NoSuchElementException: 
            dfp_t = driv.find_element(By.XPATH, '//table[@id="per_minute"]')  #  or "per_game"

        dfp = pd.read_html(StringIO(dfp_t.get_attribute('outerHTML')))[0]
        dfp = zoom_table(dfp, row)

        df_per_100.append(dfp)
        
        ################# Advanced #################
        try:
            dfa_t = driv.find_element(By.XPATH, '//table[@id="advanced"]')
            dfa = pd.read_html(StringIO(dfa_t.get_attribute('outerHTML')))[0]
            dfa = zoom_table(dfa, row)

        except NoSuchElementException:
            dfa = create_none_advanced_dataframe(row)

        df_ad.append(dfa)
        
        
        time.sleep(10)  # otherwise I'll be kicked

    return df_per_100, df_ad



logging.basicConfig(
    level=logging.INFO, format="- %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# One letter at time
dir_path = ".\\nba_players_datasets"
alphabet = 'abcdefghijklmnopqrstuvwxyz'
bf_players_path = "https://www.basketball-reference.com/players/"
bf_absolute_path = "https://www.basketball-reference.com"

list_p_100_p = []
list_a = []

driver = webdriver.Firefox()

logger.info("Let's start scraping!")
for i, letter in enumerate(alphabet):

    if i % 5 == 0:
        logger.info("Mhhhh maybe it's time to relax a little ... otherwise I'll be kicked")
        time.sleep(50)
    
    logger.info(f"Time for letter {letter}!")

    letter_path = bf_players_path + letter
    driver.get(letter_path)

    #all players that starts with letter-name
    players_character_table = pd.read_html(letter_path, extract_links='body')[0]

    players_table = remove_tuples(players_character_table)
    players_table = int_numbers(players_table)

    players_table = players_table.loc[
        (players_table['From'] >= 1973) & (players_table['From'] < 2021)].reset_index(drop=True)

    df_p_100_p, df_a = per_100_and_advanced_statistics(bf_absolute_path, players_table, driver)
    [list_p_100_p.append(d) for d in df_p_100_p]  # list for each letter
    [list_a.append(d) for d in df_a]  # list for each letter

driver.close()

################# Per_100_poss #################
dataset_per_100_poss = pd.concat(list_p_100_p, ignore_index=True) # list of lists
dataset_per_100_poss.to_json(dir_path + "\\per_100_poss_players.json", orient='records', indent=3)

################# Advanced #################
dataset_advanced = pd.concat(list_a, ignore_index=True)
dataset_advanced.to_json(dir_path + "\\advanced_players.json", orient='records', indent=3)





