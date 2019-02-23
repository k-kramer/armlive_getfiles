""" ********** getFiles.py **********

Author: Michael Giansiracusa
Email: giansiracumt@ornl.gov

Web Tools Contact: Ranjeet Devarakonda zzr@ornl.gov

Purpose:
    This tool supports downloading files using the ARM Live Data Webservice
Requirements:
    This tool requires python3, requests, and loguru package
"""
import asyncio
import argparse
import json
import requests
import sys
import os
from loguru import logger

HELP_DESCRIPTION = """
*************************** ARM LIVE UTILITY TOOL ***************************************
This tool will help users utilize the ARM Live Data Webservice to download ARM data.
This programmatic interface allows users to query and automate machine-to-machine
downloads of ARM data. This tool uses a REST URL and specific parameters (saveData,
query), user ID and access token, a datastream name, a start date, and an end date,
and data files matching the criteria will be returned to the user and downloaded.

By using this web service, users can setup cron jobs and automatically download data from
/data/archive into their workspace. This will also eliminate the manual step of following
a link in an email to download data. All other data files, which are not on the spinning
disk (on HPSS), will have to go through the regular ordering process. More information
about this REST API and tools can be found at: https://adc.arm.gov/armlive/#scripts

To login/register for an access token visit: https://adc.arm.gov/armlive/livedata/home.
******************************************************************************************
"""
EXAMPLE = """Example:
python getFiles.py -u userName:XXXXXXXXXXXXXXXX -ds sgpmetE13.b1 -s 2017-01-14 -e 2017-01-20
getARMFiles -u userName:XXXXXXXXXXXXXXXX -ds sgpmetE13.b1 -s 2017-01-14 -e 2017-01-20
"""

def parse_arguments():
    """Parse command line arguments using argparse

    :return:
        Two Namespace object that have an attribute for each command line argument.
        The first return arg contains expected command line flags and arguments.
        The second return arg contains unexpected command line flags and arguments.
    """
    parser = argparse.ArgumentParser(description=HELP_DESCRIPTION, epilog=EXAMPLE,
                                     formatter_class=argparse.RawTextHelpFormatter)
    required_arguments = parser.add_argument_group("required arguments")

    required_arguments.add_argument("-u", "--user", type=str, dest="user", required=True,
                                    help="The user's ARM ID and access token, separated by a colon.\n"
                                         "Obtained from https://adc.arm.gov/armlive/livedata/home")
    required_arguments.add_argument("-ds", "--datastream", type=str, dest="datastream",
                                    help="Name of the datastream. The query service type allows the\n"
                                         "user to enter a DATASTREAM property that's less specific,\n"
                                         "and returns a collection of data files that match the\n"
                                         "DATASTREAM property. For example: sgp30ebbrE26.b1\n")

    parser.add_argument("-s", "--start", type=str, dest="start",
                        help="Optional; start date for the datastream. "
                             "Must be of the form YYYY-MM-DD")
    parser.add_argument("-e", "--end", type=str, dest="end",
                        help="Optional; end date for the datastream. "
                             "Must be of the form YYYY-MM-DD")
    parser.add_argument("-o", "--out", type=str, dest="output", default='',
                        help="Optional; full path to directory where you would like the output\n"
                             "files. Defaults to folder named after datastream in current working\n"
                             "directory.")
    parser.add_argument("-T", "--test", action="store_true", dest="test",
                        help="Optional; flag that enables test mode. When in test mode only the\n"
                             "query will be run.")
    parser.add_argument("-D", "--Debug", action="store_true", dest="debug",
                        help="Optional; flag that enables debug printing")

    cli_args, unknown_args = parser.parse_known_args()

    if len(sys.argv) <= 1 or not (cli_args.user and cli_args.datastream):
        parser.print_help()
        parser.print_usage()
        exit(1)

    return cli_args, unknown_args

def main():
    # cli_args, unknown_args = parse_arguments()
    cli_args = argparse.Namespace(user="devarakondar:5aebb6fb63e1032f", datastream="sgpaerich1B1.a1",
                                  start="2003-01-01", end="2003-02-01", output='', debug=True, test=False)
    """ main armlive automation script

    :param cli_args:
        A argparse.Namespace object with an attribute for each expected command line argument.
    :return:
        None
    """
    # set logging level
    logger.remove(0)
    logger.level('CRITICAL', color='<r>')
    logger.level('WARNING', color='<y>')
    logger.level('DEBUG', color='<lm>')
    logger.level('INFO', color='<le>')
    if cli_args.debug or cli_args.test:
        logger.add(sys.stdout, colorize=True, level='DEBUG')
    else:
        logger.add(sys.stdout, colorize=True, level='INFO',
               format='<e>{time:YYYY:MM:D:HH:mm:ss}</e> |<le>{level}</le>| <g>{message}</g>')
    # default start and end are empty
    start, end = '', ''
    # start and end strings for query_url are constructed if the arguments were provided
    if cli_args.start:
        start = "&start={}".format(cli_args.start)
    if cli_args.end:
        end = "&end={}".format(cli_args.end)
    # build the url to query the web service using the arguments provided
    query_url = 'https://adc.arm.gov/armlive/livedata/query?user={0}&ds={1}{2}{3}&wt=json'\
        .format(cli_args.user, cli_args.datastream, start, end)

    logger.debug("Getting file list using query url:\n\t{0}".format(query_url))
    # get url response, read the body of the message, and decode from bytes type to utf-8 string
    response_body = requests.get(query_url).text

    # if the response is an html doc, then there was an error with the user
    if response_body[1:14] == "!DOCTYPE html":
        logger.warning("Error with user. Check username or token.")
        exit(1)
    # parse into json object
    response_body_json = json.loads(response_body)
    logger.debug("response body:\n{0}\n".format(json.dumps(response_body_json, indent=True)))

    # construct output directory
    if cli_args.output:
        # output files to directory specified
        output_dir = os.path.join(cli_args.output)
    else:
        # if no folder given, add datastream folder to current working dir to prevent file mix-up
        output_dir = os.path.join(os.getcwd(), cli_args.datastream)
    # make directory if it doesn't exist
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    # not testing, response is successful and files were returned
    if not cli_args.test:
        num_files = len(response_body_json["files"])
        if response_body_json["status"] == "success" and num_files > 0:
            # create async event loop
            loop = asyncio.get_event_loop()
            # create queues and tasks for async execution
            links = asyncio.Queue()
            data = asyncio.Queue()
            tasks = asyncio.gather(
                build_links(response_body_json, cli_args.user, links),
                downloader(num_files, loop, links, data),
                write_files(output_dir, num_files, loop, data)
            )
            loop.run_until_complete(tasks)
        else:
            logger.warning("No files returned or url status error.\n"
                           "Check datastream name, start, and end date.\n"
                           "Data might not be in ADC Live Archive.")
    else:
        logger.debug("*** Files would have been downloaded to directory:\n----> {}".format(output_dir))

async def build_links(response_body_json: json, user: str, links: asyncio.Queue):
    # construct links to web service saveData function
    for file_name in response_body_json['files']:
        logger.debug("[CREATING LINK] {}".format(file_name))
        get_data_url = "https://adc.arm.gov/armlive/livedata/saveData?user={0}&file={1}".format(user, file_name)
        work = (file_name, get_data_url)
        # pass them to downloader queue
        await links.put(work)

async def downloader(num_donwloads: int, loop: asyncio.AbstractEventLoop, links: asyncio.Queue, data: asyncio.Queue):
    # get work from downloader queue and put into safe bytes to file queue
    downloaded = 0
    while downloaded < num_donwloads:
        file_name, get_data_url = await links.get()
        logger.info("[DOWNLOADING] {}".format(file_name))
        logger.debug("Using link: {1}".format(file_name, get_data_url))
        response = await loop.run_in_executor(None, requests.get, get_data_url)
        file_bytes = response.content
        work = (file_name, file_bytes)
        logger.debug("[DOWNLOADED] {}".format(file_name))
        await data.put(work)
        downloaded += 1

async def write_files(output_dir: str, num_files: int, loop: asyncio.AbstractEventLoop, data: asyncio.Queue):
    saved = 0
    while saved < num_files:
        file_name, file_bytes = await data.get()
        output_file = os.path.join(output_dir, file_name)
        # create file and write bytes to file
        with open(output_file, 'wb') as open_file:
            await loop.run_in_executor(None, open_file.write, file_bytes)
        logger.success("[SAVED] {}".format(output_file))
        saved += 1

if __name__ == "__main__":
    from time import time
    start = time()
    main()
    logger.debug('Execution time: {}'.format(time() - start))
