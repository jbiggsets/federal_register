"""
A simple client to pull rule and notice metadata from
the federal register.

author :: Jeremy Biggs
date :: 01-10-2021
"""

import argparse
from datetime import timedelta
from itertools import chain
from multiprocessing import cpu_count

import grequests
import pandas as pd
import prefect
from prefect import Flow, Parameter, task
from prefect.executors import LocalDaskExecutor


def exception_handler(request, exception):
    """
    An exception handler for `grequests`
    TODO: Add exception handling?
    """
    pass


def chunk(arr, size=250):
    """
    Group an array into chunks of ``size``
    """
    return [tuple(arr[i:i + size]) for i in range(0, len(arr), size)]


def collect_urls(filename):
    """
    Get the URLs from the file
    """
    # read in the data
    df = pd.read_csv(filename,
                     usecols=['document_number', 'publication_date'],
                     low_memory=False)\
           .drop_duplicates(subset='document_number', keep='first')\
           .reset_index(drop=True).copy()

    # collect the urls -- some of these won't work
    urls = []
    for _, row in df.iterrows():
        url = (f"https://www.govinfo.gov/content/pkg/"
               f"FR-{row['publication_date']}"
               f"/html/{row['document_number']}.htm")
        urls.append(url)
    return chunk(urls)


@task(max_retries=1, timeout=300, retry_delay=timedelta(seconds=1))
def get(urls):
    """
    The main task, submitting a request to get
    rules for a given month. This will also iterate
    through the pages.

    Parameters
    ----------
    urls : tuple of str
        A list of URLs
    """
    rs = (grequests.get(u, timeout=1) for u in urls)
    resps = grequests.imap(rs, exception_handler=exception_handler)
    resps = [(r.text, url) if r and r.ok else (None, url)
             for url, r in zip(urls, resps)]
    return resps

@task
def combine(resps):
    """
    Combine the results into a single list
    """
    return list(chain.from_iterable(resps))


def main(filename, outfile):
    """
    Run the process
    """

    # get all the URLs in chunks
    urls = collect_urls(filename)

    # set up the flow
    with Flow('Map Reduce') as flow:
        texts = get.map(urls)
        rules = combine(texts)

    # run the flow with parallel threads in Dask
    # TODO: Change this to ``DaskExecutor```
    dask = LocalDaskExecutor(scheduler="processes",
                             num_workers=cpu_count())
    state = flow.run(executor=dask)

    # collect the results
    all_rules = state.result[rules].result
    df_all_rules = pd.DataFrame(all_rules)
    df_all_rules.columns = ['text', 'url']

    # TODO: Add other output options
    df_all_rules.to_csv(outfile)


if __name__ == '__main__':

    # TODO: Add more helpful argument parsing
    parser = argparse.ArgumentParser('Get Federal Register text...')
    parser.add_argument('filename')
    parser.add_argument('outfile')

    args = parser.parse_args()

    main(args.filename, args.outfile)
