from urllib.parse import urlencode, quote_plus
import time
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import pendulum
from datetime import datetime
import psycopg2
from psycopg2 import extras
import json
import configparser
import os
import boto3
import traceback


def find_tweets(html_source):
    tweets = list()
    begin = 0
    while begin != -1:
        begin = html_source.find('data-tweet-id="', begin)
        if begin != -1:
            begin = begin + len('data-tweet-id="')
            end = html_source.find('"', begin)
            tweet = html_source[begin:end]
            begin = html_source.find('data-time="', begin) + len('data-time="')
            end = html_source.find('"', begin)
            published_at = html_source[begin:end]
            tweets.append({'tweet_id': tweet, 'published_at': published_at})
    return tweets


def twitter(driver, query, since, until, language=None, seconds_of_tolerance=600):
    tweets = list()

    parameters = dict()
    if language is not None:
        parameters['l'] = language
    parameters['f'] = 'tweets'
    parameters['src'] = 'typd'
    parameters['vertical'] = 'default'
    parameters['q'] = "{0} since:{1} until:{2}".format(query, since.int_timestamp, until.add(seconds=1).int_timestamp)
    encoded_parameters = urlencode(parameters, quote_via=quote_plus)
    final_url = 'https://twitter.com/search?{0}'.format(encoded_parameters)

    driver.get(final_url)

    empty_timeline = driver.find_elements_by_css_selector('div.SearchEmptyTimeline')

    while len(empty_timeline) > 0 and until >= since:
        # if search returns no tweets then subtract one hour.
        until = until.subtract(seconds=seconds_of_tolerance)
        parameters['q'] = "{0} since:{1} until:{2}".format(query, since.int_timestamp,
                                                           until.add(seconds=1).int_timestamp)

        encoded_parameters = urlencode(parameters, quote_via=quote_plus)
        final_url = 'https://twitter.com/search?{0}'.format(encoded_parameters)

        driver.get(final_url)
        empty_timeline = driver.find_elements_by_css_selector('div.SearchEmptyTimeline')

    if until >= since:
        for i in range(10):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)

        current_time = datetime.utcnow()
        circumstantial_until = until

        html_page = driver.page_source

        tweets_current_page = find_tweets(html_page)
        for tweet in tweets_current_page:
            until = pendulum.utcfromtimestamp(int(tweet['published_at']))
            if until >= since:
                new_tweet = dict()
                new_tweet['published_at'] = datetime.utcfromtimestamp(until.int_timestamp)
                new_tweet['query'] = query
                new_tweet['since'] = datetime.utcfromtimestamp(since.int_timestamp)
                new_tweet['circumstantial_until'] = datetime.utcfromtimestamp(circumstantial_until.int_timestamp)
                new_tweet['language'] = language
                new_tweet['query_time'] = current_time
                new_tweet['twitter_url'] = final_url
                new_tweet['tweet_id'] = tweet['tweet_id']
                tweets.append(new_tweet)
    return until, tweets


if __name__ == '__main__':
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['dbname'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)

    try:
        # options = webdriver.ChromeOptions()
        # options.add_argument('headless')
        # driver = webdriver.Chrome(chrome_options=options)
        caps = DesiredCapabilities.PHANTOMJS
        caps["phantomjs.page.settings.userAgent"] = \
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0"
        driver = webdriver.PhantomJS(desired_capabilities=caps)
    except:
        c.execute("insert into error (current_record, error) VALUES (%s, %s)",
                  (None, traceback.format_exc()), )
        conn.commit()
        raise

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("""select * from aws_credentials;""")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=aws_credential['region_name']
    )
    sqs = aws_session.resource('sqs')
    subqueries_queue = sqs.get_queue_by_name(QueueName='subqueries')
    subqueries_queue_empty = False
    subquery = None
    while not subqueries_queue_empty:
        message = subqueries_queue.receive_messages()
        if len(message) == 0:
            subqueries_queue_empty = True
        else:
            subquery = json.loads(message[0].body)
            message[0].delete()
            try:
                # retrieve the oldest tweet for the query...
                c.execute(
                    "SELECT published_at FROM tweet_id WHERE query_alias = %s AND since = %s order by id desc limit 1",
                    (subquery['query_alias'], subquery['since'],))
                data = c.fetchone()
                # retrieve the since and until parameters for the query...
                original_until = pendulum.parse(subquery['until'])
                if data is not None:
                    until_pendulum = pendulum.instance(data['published_at']).add(seconds=1)
                else:
                    until_pendulum = original_until

                since_pendulum = pendulum.parse(subquery['since'])
                while since_pendulum <= until_pendulum:
                    until_pendulum, tweets = twitter(driver,
                                                     query=subquery['query'],
                                                     since=since_pendulum,
                                                     until=until_pendulum,
                                                     language=subquery['language'],
                                                     seconds_of_tolerance=subquery['seconds_of_tolerance'])
                    #print(len(tweets))
                    if len(tweets) > 0:
                        dataText = ','.join(c.mogrify('(%s,%s,%s,%s,%s,%s,%s)',
                                                    (subquery['query_alias'],
                                                     tweet['tweet_id'],
                                                     tweet['since'],
                                                     original_until.to_datetime_string(),
                                                     tweet['published_at'],
                                                     tweet['query_time'],
                                                     tweet['twitter_url'],)).decode('utf-8') for tweet in tweets)
                        c.execute("""insert into tweet_id
                                     (query_alias, tweet_id, since, until, published_at, query_time, twitter_url)
                                     values """ + dataText + " ON CONFLICT DO NOTHING")
                        if c.rowcount == 0:
                            until_pendulum = until_pendulum.subtract(seconds=1)

                c.execute("UPDATE subquery SET complete = true WHERE query_alias = %s AND since = %s",
                          (subquery['query_alias'], subquery['since'],))
                conn.commit()
            except:
                conn.rollback()
                c.execute("insert into error (current_record, error) VALUES (%s, %s)",
                          (json.dumps(subquery), traceback.format_exc()),)
                conn.commit()
                subqueries_queue.send_message(MessageBody=json.dumps(subquery))
                raise

    driver.close()

    c.execute("""update query
                 set status = 'PHASE3'
                 where status = 'PHASE2' and
                       not exists(
                           select *
                           from subquery
                           where query.query_alias = subquery.query_alias and
                                 not subquery.complete
                       )""")
    conn.commit()
    conn.close()