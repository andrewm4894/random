#!/usr/bin/env python3

"""
description:
python script to take a url you can copy from a netdata dashboard, parse it and then rank all _km charts
and print out top_n of that ranked list of charts along with a link to each chart.

example usage:
python process_anomaly_scores.py --url="http://35.193.228.190:19999/#;after=1616599545000;before=1616601345000;theme=slate"
"""

import requests
import asyncio
from urllib.parse import urlparse
import argparse


def request_sync(job):
    chart = job[0]
    url = job[1]
    #print(f'getting data from {url}')
    r = requests.get(url)
    data = r.json()['data']
    score = 0
    if len(data) > 0:
        try:
            data = [sum(d[1:]) / (len(d) - 1) for d in data]
            score = round(sum(data)/len(data), 2)
        except:
            pass
    return chart, score


async def request_async(job):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, request_sync, job)


async def main(jobs):
    coroutines = [request_async(job) for job in jobs]
    results = await asyncio.gather(*coroutines)
    return results


if __name__ == "__main__":

    # handle args
    parser = argparse.ArgumentParser(description='Process anomaly scores.')
    parser.add_argument('--url', help='url from netdata dashboard',
                        default='http://127.0.0.1:19999/#menu_system_submenu_cpu;theme=slate')
    parser.add_argument('--n', help='top n charts by anomaly score', default=20)
    args = parser.parse_args()
    url = args.url
    top_n = int(args.n)

    # parse url
    print(f'url is : {url}')
    http = url.split('://')[0]
    url_parsed = urlparse(url)

    # get params from url
    host = url_parsed.netloc
    fragments = {x[0]: x[1] for x in [x.split('=') for x in url_parsed.fragment.split(';') if '=' in x]}
    after = int(int(fragments['after']) / 1000) if 'after' in fragments else -600
    before = int(int(fragments['before']) / 1000) if 'before' in fragments else 0

    # get charts available
    charts = requests.get(f'{http}://{host}/api/v1/charts').json()['charts']
    charts_href = {
        chart: f"#menu_{charts[chart]['type']}_submenu_{charts[chart]['family']}".replace('.', '_').replace(' ', '_')
        for chart in charts
    }
    charts = sorted([c for c in charts if c.endswith('_km')])

    # get scores via asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    jobs = [(c, f'http://{host}/api/v1/data?chart={c}&points=5&after={after}&before={before}') for c in charts]
    scores = loop.run_until_complete(main(jobs))

    # create sorted dict from scores
    scores = dict(sorted({r[0]: r[1] for r in scores}.items(), key=lambda item: item[1], reverse=True))

    # print top n
    print('--' * 20)
    print(f'top {top_n} scores')
    print('--' * 20)
    for i, chart in enumerate(list(scores.keys())[0:top_n]):
        print(
            str(i + 1).ljust(3), 
            chart.ljust(40), 
            format(scores[chart], '.2f'),
            f'link = {http}://{host}/{charts_href[chart]};after={after * 1000};before={before * 1000}')
