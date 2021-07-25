
import argparse
import sys

from typing import Sequence

from pydantic.error_wrappers import ValidationError
from requests.exceptions import ConnectionError

from clutch import Client
from clutch.schema.request.torrent.accessor import TorrentAccessorArgumentsRequest
from clutch.schema.request.torrent.add import TorrentAddArgumentsRequest
from clutch.schema.request.torrent.mutator import TorrentMutatorArgumentsRequest
from clutch.schema.user.method.shared import IdsArg
from clutch.schema.user.method.torrent.accessor import field_keys, TorrentAccessorField
from clutch.schema.user.method.torrent.action import TorrentActionMethod
from clutch.schema.user.method.torrent.add import TorrentAddArguments
from clutch.schema.user.method.torrent.mutator import TorrentMutatorArguments
from clutch.schema.user.response.torrent.accessor import TorrentAccessorResponse
from clutch.schema.user.response.torrent.accessor import TorrentAccessorObject
from clutch.schema.user.response.torrent.add import TorrentAdd
from clutch.schema.user.response.torrent.rename import TorrentRename


def print_torrent_info(torrent, full = False):
    print("Name: ", torrent.id, "-", torrent.name)
    print("Status: ", torrent.error, " ", torrent.error_string)

def print_torrent_message(message, torrent, include_torrent_error = True):
    if include_torrent_error:
        print(message,torrent.error_string, torrent.id, torrent.name)
    else:
        print(message,torrent.id, torrent.name)


def remove_torrent(client, torrent, delete_local_data=True, test=True):
    if test:
        print_torrent_message("Remove disabled. Skipping ",torrent, include_torrent_error = True)
    else:
        print_torrent_message("Removing ",torrent, include_torrent_error = True)
        remove_response = client.torrent.remove(torrent.id, delete_local_data=delete_local_data)
        print(remove_response)
        if remove_response.result == 'success':
            return(1)
    return(0)


def start_torrent(client, torrent, test=True, force_start = True):
    if test:
        print_torrent_message("Test mode. Start disabled. Skipping ", torrent, include_torrent_error = True)
    else:
        if force_start:
            action_method = TorrentActionMethod("torrent-start-now")
            print_torrent_message("Force Starting ",torrent, include_torrent_error = True)
        else:
            action_method = TorrentActionMethod("torrent-start")
            print_torrent_message("Starting ",torrent, include_torrent_error = True)

        start_response = client.torrent.action(action_method, ids = torrent.id)
        print(start_response)
        if start_response.result == 'success':
            return(1)
    return(0)

def re_add_torrent(client, torrent, test=True):
    if test:
        print_torrent_message("Test mode. Re-add running without remove", torrent, include_torrent_error = True)
        remove_response = None
    else:
        print_torrent_message("Removing ",torrent, include_torrent_error = True)
        remove_response = client.torrent.remove(ids = torrent.id, delete_local_data = False)
        print(remove_response)
    
    add_torrent_arguments: TorrentAddArguments = {
        "filename": torrent.magnet_link,
        "paused": True,
    }
    print_torrent_message("Adding ",torrent, include_torrent_error = False)
    add_response = client.torrent.add(add_torrent_arguments)
    print(add_response)
        
    if add_response.result == 'success' :
        return(1)
    return(0)

def get_torrents(client, fields = None, ids: IdsArg = None):
    if fields == None:
        all_fields=True
    else:
        all_fields=False
    
    torrents: Sequence[TorrentAccessorObject] = []
    try:
        print("Loading torrent list from transmission")
        response = client.torrent.accessor(fields = fields, all_fields=all_fields, ids = ids )
        if response.result != 'success':
            print("Transmission failed return a list of torrents. Transmission response: ", response)
        else:
            try:
                torrents: Sequence[TorrentAccessorObject] = response.arguments.torrents
                print("Successfully loaded torrent list from transmission. Torrents loaded:", len(torrents))
            except:
                print("Failed to parse transmission response to torrents list.")
    except ConnectionRefusedError as error:
        print("Transmission connection failure. Connection Refused.", error.json())
    except ValidationError as error:
        print("Transmission connection failure. Connection Response Invalid.", error.json())
    except ConnectionError as error:
        print("Transmission connection failure. Connection Failed.",  error)
    except:
        print("Transmission connection failure. Unexpected error:", sys.exc_info()[0])
    return torrents  

def filter_torrents(torrents, value, attribue = 'name', match_type = 'equals'):
    if match_type == 'equals':
        torrents_filter =  filter(lambda torrent: \
            getattr(torrent, attribue) == value
        , torrents)
        return list(torrents_filter)
    if match_type == 'startswith':
        torrents_filter =  filter(lambda torrent: \
            getattr(torrent, attribue).startswith(value)
        , torrents)
        return list(torrents_filter)
    if match_type == 'contains':
        torrents_filter =  filter(lambda torrent: \
            getattr(torrent, attribue).contains(value)
        , torrents)
        return list(torrents_filter)

def compare_torrent_list(torrents_a, torrents_b, attribue = 'name',match_type = 'equals'):
    matches = 0
    for torrent in torrents_a:
        if len(filter_torrents(torrents_b, value =  getattr(torrent, attribue), attribue = attribue, match_type = match_type)) > 0:
            matches = matches + 1
        else:
            print_torrent_message("No match in torrents_b found for", torrent, include_torrent_error = True)
    
    print('Found:',matches, '/', len(torrents_a), 'from torrents_a in torrents_b')


def clean_torrents(client,torrents: [TorrentAccessorObject], clean_function, test=True):
    cleaned = 0
    clean_attempts = 0
    ids: [IdsArg] = []

    for torrent in torrents:
        print("-------------------------------------------------------------- ")
        ids.append(torrent.id)
        clean_attempts = clean_attempts + 1
        cleaned = cleaned + clean_function(client, torrent, test = test)
    print('Cleaned:',cleaned, '/', clean_attempts )
    return(ids)

def clean_torrents_missing_data(client,torrents: [TorrentAccessorObject], test=True):
    torrents_for_cleaning = torrents_missing_data(torrents)
    if len(torrents_for_cleaning) == 0:
        print('No torrents missing data to clean')
        return 0
    else:
        print('Cleaning torrents with missing data. Clean action: remove and re-add')
        ids = clean_torrents(client = client, torrents = torrents_for_cleaning, clean_function = re_add_torrent, test = test)
        torrents_post_clean = get_torrents(client)
        compare_torrent_list(torrents_for_cleaning, torrents_post_clean, attribue = 'name',match_type = 'equals')
        return len(ids)
        

def clean_torrents_unregistered(client,torrents: [TorrentAccessorObject], test=True):
    torrents_for_cleaning = torrents_unregistered(torrents)
    if len(torrents_for_cleaning) == 0:
        print('No unregistered torrents to clean')
        return 0
    else:
        print('Cleaning torrents',len(torrents_for_cleaning),'that are unregistered. Clean action: remove')
        ids = clean_torrents(client = client, torrents = torrents_for_cleaning, clean_function = remove_torrent, test = test)
        cleaned = len(ids)
        return len(ids)
        

def clean_torrents_with_temp_errors(client,torrents: [TorrentAccessorObject], test=True, retries = 2, force = False):
    attempt = 0
    max_attempts = retries + 1
    torrents_for_cleaning = torrents_with_temp_errors(torrents)
    starting_torrents_for_cleaning = torrents_for_cleaning

    if len(torrents_for_cleaning) == 0:
        print('No torrents temp errors to force start')

    while len(torrents_for_cleaning) > 0 and attempt < max_attempts:
        torrents_for_cleaning_count = len(torrents_for_cleaning)
        attempt = attempt + 1
        
        print('Cleaning torrents with temp errors. Clean action: force start. Attempt',attempt, '/', max_attempts)
        ids = clean_torrents(client = client, torrents = torrents_with_temp_errors(torrents_for_cleaning), clean_function = start_torrent, test = test)
        torrents_for_cleaning = get_torrents(client, ids = ids)
        print("Unresolved:", len(torrents_with_temp_errors(torrents_for_cleaning)), "/", torrents_for_cleaning_count)
        print("-------------------------------------------------------------- ")
    if force and len(torrents_for_cleaning) > 0:
        print('Re-cleaning torrents with temp errors. Clean action: remove and re-add')
        ids = clean_torrents(client = client, torrents = torrents_with_temp_errors(torrents_for_cleaning), clean_function = re_add_torrent, test = test)
        return len(starting_torrents_for_cleaning)
    return len(starting_torrents_for_cleaning) - len(torrents_for_cleaning)


def torrents_missing_data(torrents: [TorrentAccessorObject]):
    torrents_filter =  filter(lambda torrent: \
        torrent.error_string == 'No data found! Ensure your drives are connected or use "Set Location". To re-download, remove the torrent and re-add it.' \
            , torrents)
    return list(torrents_filter)

def torrents_unregistered(torrents: [TorrentAccessorObject]):
    torrents_filter =  filter(lambda torrent: \
        torrent.error_string == "Unregistered torrent" \
            , torrents)
    return list(torrents_filter)

def torrents_with_errors(torrents: [TorrentAccessorObject]):
    torrents_filter =  filter(lambda torrent: torrent.error != 0, torrents)
    return list(torrents_filter)


def torrents_with_temp_errors(torrents: [TorrentAccessorObject]):
    torrents_filter =  filter(lambda torrent: \
        torrent.error_string.startswith("Input/output error") or \
                    torrent.error_string.startswith("Unable to save resume file") \
            , torrents)
    return list(torrents_filter)

def main(address="http://localhost:9091/transmission/rpc",
        scheme=None,
        host=None,
        port=None,
        path=None,
        query=None,
        username=None,
        password=None,
        debug=False,
        test=True):

    if test:
        print("running in test mode!")

    client = Client(address=address,
        scheme=scheme,
        host=host,
        port=port,
        path=path,
        query=query,
        username=username,
        password=password,
        debug=debug)

    torrents = get_torrents(client)
    if len(torrents) > 0:
        temp_errors_cleaned  = clean_torrents_with_temp_errors(client, torrents, test=test, force= False)
        missing_data_cleaned = clean_torrents_missing_data(client, torrents, test=test)
        unregistered_cleaned = clean_torrents_unregistered(client, torrents, test=test)
        print("Cleaned",temp_errors_cleaned, "temp errors", missing_data_cleaned, "missing data", unregistered_cleaned,"unregistered", \
            "from total of" ,len(torrents_with_errors(torrents)) , "errors across", len(torrents),"torrents")

   
parser = argparse.ArgumentParser(description='Transmission Cleaner - Automaticly remedy those torrent errors!')

parser.add_argument("--address", default="http://localhost:9091/transmission/rpc", type=str, help="Full URL path to transmission. eg. http://localhost:9091/transmission/rpc")
parser.add_argument("--scheme", default=None, type=str, help="Transmission connection schema. eg. http")
parser.add_argument("--host", default=None, type=str, help="Transmission connection host. eg. localhost")
parser.add_argument("--port", default=None, type=str, help="Transmission connection port. eg. 9091")
parser.add_argument("--path", default=None, type=str, help="Transmission connection host. eg. /transmission/rpc")
parser.add_argument("--query", default=None, type=str, help="Transmission connection query.")
parser.add_argument("--username", default=None, type=str, help="Transmission connection username.")
parser.add_argument("--password", default=None, type=str, help="Transmission connection password.")
parser.add_argument("--debug", default=False, help="Transmission connection debug flag.", action='store_true')
parser.add_argument("--test", default=False, help="Run cleaner in test mode. Print actions console instead of doing them.", action='store_true')

#group = parser.add_mutually_exclusive_group(required=True)
#group.add_argument('--address', action='store_true', help="This is the 'address' variable")
#group.add_argument('--scheme', action='store_true', help="This is the 'scheme' variable")

args = parser.parse_args()
print(args)
print(args.test)
main(address=args.address, scheme=args.scheme, host=args.host, port=args.port, path=args.path, \
    query=args.query, username=args.username, password=args.password, debug=args.debug, test = args.test)
