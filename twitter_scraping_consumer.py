import configparser
import json
import os
import time
import traceback
from urllib.parse import urlencode, quote_plus

import requests
import boto3
import psycopg2
from psycopg2 import extras
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


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
            tweets.append({'tweet_id': tweet, 'published_at': int(published_at)})
    return tweets


def main():
    # Connect to Postgres server.
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))
    conn = psycopg2.connect(host=config['database']['host'],
                            dbname=config['database']['db_name'],
                            user=config['database']['user'],
                            password=config['database']['password'])
    c = conn.cursor(cursor_factory=extras.RealDictCursor)

    # Retrieve AWS credentials and connect to Simple Queue Service (SQS).
    c.execute("""select * from aws_credentials;""")
    aws_credential = c.fetchone()
    aws_session = boto3.Session(
        aws_access_key_id=aws_credential['aws_access_key_id'],
        aws_secret_access_key=aws_credential['aws_secret_access_key'],
        region_name=config['aws']['region_queues']
    )
    sqs = aws_session.resource('sqs')
    subqueries_queue = sqs.get_queue_by_name(QueueName='twitter_scraping')
    # Retrieve AWS AMI.
    c.execute("select * from aws_ami "
              "where ami_name = 'twitter_scraping_consumer' "
              "and region_name = '{}'".format(aws_credential['region_name']))
    aws_ami = c.fetchone()

    subquery = None
    ip = None
    incomplete_transaction = False

    try:
        ip = requests.get('http://checkip.amazonaws.com').text.rstrip()

        options = Options()
        if 'DISPLAY' not in os.environ:
            options.headless = True
        # start the browser
        with webdriver.Firefox(firefox_options=options) as driver:
            subqueries_queue_empty = False
            while not subqueries_queue_empty:
                message = subqueries_queue.receive_messages()
                if len(message) == 0:
                    subqueries_queue_empty = True
                else:
                    subquery = json.loads(message[0].body)
                    message[0].delete()
                    incomplete_transaction = True

                    while subquery['since'] <= subquery['until']:
                        parameters = dict()
                        if subquery['language'] is not None:
                            parameters['l'] = subquery['language']
                        parameters['f'] = 'tweets'
                        parameters['src'] = 'typd'
                        parameters['vertical'] = 'default'
                        parameters['q'] = "{0} since:{1} until:{2}".format(subquery['search_terms'],
                                                                           subquery['since'],
                                                                           subquery['until'])
                        encoded_parameters = urlencode(parameters, quote_via=quote_plus)
                        final_url = 'https://twitter.com/search?{0}'.format(encoded_parameters)

                        driver.get(final_url)

                        empty_timeline = driver.find_elements_by_css_selector('div.SearchEmptyTimeline')

                        if len(empty_timeline) > 0:
                            c.execute("""
                                            insert into twitter_scraping_attempt
                                            (query_alias, since, until, twitter_url, ip, empty) VALUES 
                                            (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s);
                                        """,
                                      (subquery['query_alias'], subquery['since'], subquery['until'],
                                       final_url, ip, True))
                            subquery['until'] = subquery['until'] - subquery['tolerance_in_seconds']
                        else:
                            c.execute("""
                                            insert into twitter_scraping_attempt
                                            (query_alias, since, until, twitter_url, ip, empty) VALUES 
                                            (%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s);
                                        """,
                                      (subquery['query_alias'], subquery['since'], subquery['until'],
                                       final_url, ip, False))

                            for i in range(10):
                                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                time.sleep(0.5)

                            html_page = driver.page_source
                            tweets = find_tweets(html_page)

                            # todo: change default tolerance in seconds and subquery interval on create_database.sql
                            if len(tweets) == 0:
                                # todo: this is not normal... I believe it happens when twitter blocks
                                # the script... two alternatives: kill this server and start a new one OR
                                # log the HTML response from the server OR both. Maybe we could just raise an exception?
                                subquery['until'] = subquery['until'] - subquery['tolerance_in_seconds']
                            else:
                                data_text = ','.join(
                                    c.mogrify('(%s, to_timestamp(%s), to_timestamp(%s), %s, to_timestamp(%s))',
                                              (subquery['query_alias'],
                                               subquery['since'],
                                               subquery['until'],
                                               tweet['tweet_id'],
                                               tweet['published_at'],)
                                              ).decode('utf-8') for tweet in tweets)
                                c.execute("""
                                    insert into twitter_dry_tweet
                                    (query_alias, since, until, tweet_id, published_at) values 
                                    {}
                                    on conflict do nothing;
                                """.format(data_text))

                                i = len(tweets) - 1
                                found_new_until = False
                                while not found_new_until and i >= 0:
                                    if subquery['since'] <= tweets[i]['published_at'] <= subquery['until']:
                                        found_new_until = True
                                        subquery['until'] = tweets[i]['published_at'] + 1
                                    i = i - 1
                                if not found_new_until:
                                    subquery['until'] = subquery['until'] - subquery['tolerance_in_seconds']
                                else:
                                    if c.rowcount == 0:
                                        subquery['until'] = subquery['until'] - 1

                        conn.commit()

                    c.execute("""
                                UPDATE twitter_scraping_subquery
                                SET complete = true
                                WHERE query_alias = %s AND since = to_timestamp(%s);
                                """,
                              (subquery['query_alias'], subquery['since'],))
                    conn.commit()
                    incomplete_transaction = False
    except Exception:
        conn.rollback()
        c.execute("insert into error (current_record, error, module, ip) VALUES (%s, %s, %s, %s)",
                  (json.dumps(subquery), traceback.format_exc(), "twitter_scraping_consumer", ip))
        conn.commit()
        if incomplete_transaction:
            subqueries_queue.send_message(MessageBody=json.dumps(subquery))
        # start a new server
        ec2 = aws_session.resource('ec2')
        # https://boto3.readthedocs.io/en/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
        ec2.create_instances(ImageId=aws_ami['ami_id'],
                             InstanceType=aws_ami['instance_type'],
                             KeyName=aws_ami['key_pair_name'],
                             InstanceInitiatedShutdownBehavior='terminate',
                             MaxCount=1,
                             MinCount=1)
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
