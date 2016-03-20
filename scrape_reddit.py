import pandas
import praw
from sqlalchemy import create_engine
import datetime

import json



def scrape_hot_ranking(subreddit_name, engine, limit=100, current_time=None, collection=""):
    user_agent_string = "position bias scraper; contact stoddardg@gmail.com"
    r = praw.Reddit(user_agent_string)
    subreddit = r.get_subreddit(subreddit_name)
    if current_time is None:
        current_time = datetime.datetime.now()
    position = 1
    post_data_list = []
    hot_submissions = subreddit.get_hot(limit=limit)
    for x in hot_submissions:
        post_data = {
            'id':x.id,
            'score':x.score,
            'num_comments':x.num_comments,
            'position':position,
            'subreddit':subreddit_name,
            'ranking':'hot',
            'time_scraped':current_time,
            'collection':collection,
            'url':x.url
        }
        post_data_list.append(post_data)
        position += 1

    post_data_df = pandas.DataFrame(post_data_list)
    post_data_df.to_sql('hot_ranking', engine, if_exists='append', index=False)

def scrape_new_ranking(subreddit_name, engine, limit=100, current_time=None, collection=""):
    user_agent_string = "position bias scraper; contact stoddardg@gmail.com"
    r = praw.Reddit(user_agent_string)
    subreddit = r.get_subreddit(subreddit_name)
    if current_time is None:
        current_time = datetime.datetime.now()
    position = 1
    post_data_list = []
    new_submissions = subreddit.get_new(limit=limit)
    for x in new_submissions:
        post_data = {
            'id':x.id,
            'score':x.score,
            'num_comments':x.num_comments,
            'position':position,
            'subreddit':subreddit_name,
            'ranking':'new',
            'time_scraped':current_time,
            'collection':collection,
            'url':x.url
        }
        post_data_list.append(post_data)
        position += 1

    post_data_df = pandas.DataFrame(post_data_list)
    post_data_df.to_sql('new_ranking', engine, if_exists='append', index=False)

def export_data(engine):
    hot_rankings = pandas.read_sql('hot_ranking',engine)
    new_rankings = pandas.read_sql('new_ranking',engine)

    hot_rankings.to_csv('hot_ranking.csv.gz', index=False, compression='gzip')
    new_rankings.to_csv('new_ranking.csv.gz', index=False, compression='gzip')

if __name__ == '__main__':
    
    engine = create_engine("sqlite:///reddit_time_series.db")
    if len(sys.argv) > 1:
        if sys.argv[1] == 'export':
            export_data(engine)
            return

    current_time = datetime.datetime.now()

    with open('subreddit_list.json') as data_file:    
        json_data = json.load(data_file)



    for collection in json_data:
        data = json_data[collection]
        for s in data['subreddits']:
            scrape_hot_ranking(s, engine, limit=50,current_time=current_time, collection=collection)
            scrape_new_ranking(s, engine, limit=50,current_time=current_time, collection=collection)











