# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################


def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY command for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00', '')


def get_id_urls(url, connection):
    '''
    Given a url, return the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''
    sql = sqlalchemy.sql.text('''
        INSERT INTO urls (url)
        VALUES (:url)
        ON CONFLICT (url) DO NOTHING
        RETURNING id_urls
    ''')
    res = connection.execute(sql, {'url': url}).first()

    if res is None:
        sql = sqlalchemy.sql.text('''
            SELECT id_urls
            FROM urls
            WHERE url = :url
        ''')
        res = connection.execute(sql, {'url': url}).first()

    id_urls = res[0]
    return id_urls


def insert_tweet(connection, tweet):
    '''
    Insert the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''

    # skip tweet if it's already inserted
    sql = sqlalchemy.sql.text('''
        SELECT id_tweets
        FROM tweets
        WHERE id_tweets = :id_tweets
    ''')
    res = connection.execute(sql, {'id_tweets': tweet['id']})
    if res.first() is not None:
        return

    ########################################
    # insert into the users table
    ########################################
    if tweet['user']['url'] is None:
        user_id_urls = None
    else:
        user_id_urls = get_id_urls(tweet['user']['url'], connection)

    sql = sqlalchemy.sql.text('''
        INSERT INTO users (id_users, screen_name)
        VALUES (:id_users, :screen_name)
        ON CONFLICT (id_users) DO NOTHING
    ''')
    connection.execute(sql, {
        'id_users': tweet['user']['id'],
        'screen_name': remove_nulls(tweet['user']['screen_name']),
    })

    ########################################
    # insert into the tweets table
    ########################################

    # extract geo
    try:
        geo_coords = str(tweet['geo']['coordinates'][0]) + ' ' + str(tweet['geo']['coordinates'][1])
        geo_str = 'POINT'
    except TypeError:
        try:
            geo_coords = '('
            for i, poly in enumerate(tweet['place']['bounding_box']['coordinates']):
                if i > 0:
                    geo_coords += ','
                geo_coords += '('
                for j, point in enumerate(poly):
                    geo_coords += str(point[0]) + ' ' + str(point[1]) + ','
                geo_coords += str(poly[0][0]) + ' ' + str(poly[0][1])
                geo_coords += ')'
            geo_coords += ')'
            geo_str = 'MULTIPOLYGON'
        except KeyError:
            if tweet['user']['geo_enabled']:
                geo_str = None
                geo_coords = None

    try:
        text = tweet['extended_tweet']['full_text']
    except:
        text = tweet['text']

    try:
        country_code = tweet['place']['country_code'].lower()
    except TypeError:
        country_code = None

    if country_code == 'us':
        state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
        if len(state_code) > 2:
            state_code = None
    else:
        state_code = None

    try:
        place_name = tweet['place']['full_name']
    except TypeError:
        place_name = None

    # insert unhydrated reply-to user if needed
    if tweet.get('in_reply_to_user_id', None) is not None:
        sql = sqlalchemy.sql.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, NULL)
            ON CONFLICT (id_users) DO NOTHING
        ''')
        connection.execute(sql, {
            'id_users': tweet['in_reply_to_user_id'],
        })

    # build geo value
    if geo_str is not None and geo_coords is not None:
        geo = geo_str + '(' + geo_coords + ')'
    else:
        geo = None

    sql = sqlalchemy.sql.text('''
        INSERT INTO tweets (
            id_tweets,
            id_users,
            created_at,
            in_reply_to_status_id,
            in_reply_to_user_id,
            quoted_status_id,
            retweet_count,
            favorite_count,
            quote_count,
            withheld_copyright,
            withheld_in_countries,
            source,
            text,
            country_code,
            state_code,
            lang,
            place_name,
            geo
        ) VALUES (
            :id_tweets,
            :id_users,
            :created_at,
            :in_reply_to_status_id,
            :in_reply_to_user_id,
            :quoted_status_id,
            :retweet_count,
            :favorite_count,
            :quote_count,
            :withheld_copyright,
            :withheld_in_countries,
            :source,
            :text,
            :country_code,
            :state_code,
            :lang,
            :place_name,
            ST_GeomFromText(:geo, 4326)
        ) ON CONFLICT (id_tweets) DO NOTHING
    ''')
    connection.execute(sql, {
        'id_tweets': tweet['id'],
        'id_users': tweet['user']['id'],
        'created_at': tweet.get('created_at'),
        'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
        'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
        'quoted_status_id': tweet.get('quoted_status_id'),
        'retweet_count': tweet.get('retweet_count'),
        'favorite_count': tweet.get('favorite_count'),
        'quote_count': tweet.get('quote_count'),
        'withheld_copyright': tweet.get('withheld_copyright'),
        'withheld_in_countries': tweet.get('withheld_in_countries'),
        'source': remove_nulls(tweet.get('source')),
        'text': remove_nulls(text),
        'country_code': country_code,
        'state_code': state_code,
        'lang': tweet.get('lang'),
        'place_name': remove_nulls(place_name),
        'geo': geo,
    })

    ########################################
    # insert into the tweet_urls table
    ########################################
    try:
        urls = tweet['extended_tweet']['entities']['urls']
    except KeyError:
        urls = tweet['entities']['urls']

    for url in urls:
        id_urls = get_id_urls(url['expanded_url'], connection)
        sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_urls (id_tweets, id_urls)
            VALUES (:id_tweets, :id_urls)
            ON CONFLICT DO NOTHING
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'id_urls': id_urls,
        })

    ########################################
    # insert into the tweet_mentions table
    ########################################
    try:
        mentions = tweet['extended_tweet']['entities']['user_mentions']
    except KeyError:
        mentions = tweet['entities']['user_mentions']

    for mention in mentions:
        # insert unhydrated user
        sql = sqlalchemy.sql.text('''
            INSERT INTO users (id_users, screen_name)
            VALUES (:id_users, :screen_name)
            ON CONFLICT (id_users) DO NOTHING
        ''')
        connection.execute(sql, {
            'id_users': mention['id'],
            'screen_name': remove_nulls(mention['screen_name']),
        })

        # insert into tweet_mentions
        sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_mentions (id_tweets, id_users)
            VALUES (:id_tweets, :id_users)
            ON CONFLICT DO NOTHING
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'id_users': mention['id'],
        })

    ########################################
    # insert into the tweet_tags table
    ########################################
    try:
        hashtags = tweet['extended_tweet']['entities']['hashtags']
        cashtags = tweet['extended_tweet']['entities']['symbols']
    except KeyError:
        hashtags = tweet['entities']['hashtags']
        cashtags = tweet['entities']['symbols']

    tags = ['#' + hashtag['text'] for hashtag in hashtags] + ['$' + cashtag['text'] for cashtag in cashtags]

    for tag in tags:
        sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_tags (id_tweets, tag)
            VALUES (:id_tweets, :tag)
            ON CONFLICT DO NOTHING
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'tag': remove_nulls(tag),
        })

    ########################################
    # insert into the tweet_media table
    ########################################
    try:
        media = tweet['extended_tweet']['extended_entities']['media']
    except KeyError:
        try:
            media = tweet['extended_entities']['media']
        except KeyError:
            media = []

    for medium in media:
        id_urls = get_id_urls(medium['media_url'], connection)
        sql = sqlalchemy.sql.text('''
            INSERT INTO tweet_media (id_tweets, id_urls, type)
            VALUES (:id_tweets, :id_urls, :type)
            ON CONFLICT DO NOTHING
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'id_urls': id_urls,
            'type': medium['type'],
        })


################################################################################
# main functions
################################################################################

if __name__ == '__main__':

    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--inputs', nargs='+', required=True)
    parser.add_argument('--print_every', type=int, default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py',
    })
    with engine.begin() as connection:
        for filename in sorted(args.inputs, reverse=True):
            with zipfile.ZipFile(filename, 'r') as archive:
                print(datetime.datetime.now(), filename)
                for subfilename in sorted(archive.namelist(), reverse=True):
                    with io.TextIOWrapper(archive.open(subfilename)) as f:
                        for i, line in enumerate(f):
                            tweet = json.loads(line)
                            insert_tweet(connection, tweet)
                            if i % args.print_every == 0:
                                print(datetime.datetime.now(), filename, subfilename, 'i=', i, 'id=', tweet['id'])
