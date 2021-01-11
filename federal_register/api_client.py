"""
A simple client to pull rule and notice metadata from
the federal register.

author :: Jeremy Biggs
date :: 01-10-2021
"""

import argparse
import requests
from datetime import timedelta
from itertools import chain
from multiprocessing import cpu_count

import pandas as pd
import prefect
from pandas.tseries.offsets import MonthBegin
from prefect import Flow, task
from prefect.engine.signals import LOOP
from prefect.executors import DaskExecutor


# The API endpoint to get documents in JSON format
URL = 'https://www.federalregister.gov/api/v1/documents.json'

# TODO: Allow users to customize this list of fields?
FIELDS = ['abstract', 'agency_names', 'citation',
          'comment_url', 'dates', 'document_number',
          'effective_on', 'full_text_xml_url',
          'html_url', 'json_url', 'page_length',
          'pdf_url', 'publication_date',
          'regulation_id_number_info', 'significant',
          'title', 'topics', 'type']


def month_iterator(start='1995-01-01', end='2021-01-01', fformat='%Y-%m-%d'):
    """
    Given a start and end date, yield tuples of
    begin and end days for each  month.

    Yields
    ------
    d1, d2 : tuple of str
        A tuple of start and end dates for each month
        in ``fformat`` format
    """
    for d1, d2 in zip(pd.date_range(start, end, freq='MS'),
                      pd.date_range(start, end, freq='M')):
        yield d1.strftime(fformat), d2.strftime(fformat)

@task(max_retries=20, retry_delay=timedelta(seconds=2))
def get(args):
    """
    The main task, submitting a request to get
    rules for a given month. This will also iterate
    through the pages.

    Parameters
    ----------
    args : tuple of str
        A tuple of start and end dates
    """
    start, end = args
    # TODO: Allow users to customize this dict?
    current_params = {'page': 0,
                      'per_page': 1000,
                      'order': 'newest',
                      'fields[]': FIELDS,
                      'conditions[type][]': ['NOTICE', 'RULE'],
                      'conditions[publication_date][gte]': start,
                      'conditions[publication_date][lte]': end}

    # get the payload from the prefect context
    loop_payload = prefect.context.get('task_loop_result', {})

    # get all of the parameters from the payload
    n = loop_payload.get('n', 1)
    url = loop_payload.get('url', URL)
    params = loop_payload.get('params', current_params)
    results = loop_payload.get('res', [])

    # submit the GET request
    resp = requests.get(url, params=params).json()
    next_url = resp.get('next_page_url', None)
    results.extend(resp['results'])

    # if there is no `next_url`, return the result
    if next_url is None:
        return results

    # otherwise, continue looping through the pages
    raise LOOP(message=f"{start}, {n}", result=dict(n=n + 1,
                                                    params=None,
                                                    url=next_url,
                                                    res=results))

@task
def combine(results):
    """
    Combine the results into a single list
    """
    return list(chain.from_iterable(results))

def main(output_file, start_date, end_date, n_threads=3):
    """
    Run the process
    """

    # get the urls
    urls = list(month_iterator(start_date, end_date))

    # set up the flow
    with Flow('Map Reduce') as flow:
        rule = get.map(urls)
        rules = combine(rule)

    # run the flow with parallel processes in Dask
    # TODO: Change this to ``DaskExecutor```
    dask = LocalDaskExecutor(scheduler="processes",
                             num_workers=cpu_count())
    state = flow.run(executor=dask)

    # collect the results
    all_rules = state.result[rules].result
    df_all_rules = pd.DataFrame(all_rules)

    # TODO: Add other output options
    df_all_rules.to_csv(output_file)


if __name__ == '__main__':

    # TODO: Add more helpful argument parsing
    parser = argparse.ArgumentParser('Get Federal Register Rules...')
    parser.add_argument('output_file')
    parser.add_argument('-s', '--start_date', default='1995-01-01')
    parser.add_argument('-e', '--end_date', default='2021-01-01')
    parser.add_argument('-n', '--n_threads', default=3, type=int)

    args = parser.parse_args()

    main(args.output_file, args.start_date, args.end_date, args.n_threads)
