#!/usr/bin/env python

"""
Convert the USAXS scan log files from XML to mongodb via JSON.

requires python 3.6 (for the timestamp())
"""


import dateutil.parser
import json
import logging
import lxml.etree
import os
import sys
import uuid
# import databroker
from collections import OrderedDict
import spec2nexus


HOME = os.environ.get("HOME", "~")
MONGODB_YML = os.path.join(HOME, ".config/databroker/mongodb_config.yml")


def random_uuid():
    """return a random UUID"""
    try:
        s = uuid.uuid4().get_hex()
    except AttributeError:
        s = uuid.uuid4().hex
    return s


def time_float(datestring):
    dt = dateutil.parser.parse(datestring)
    return dt.timestamp()


def read_xml_file(xml_filename, db):
    """
    read the XML scanLog file, log scans into db
    
    Typical start of scanLog XML file::

        <?xml version="1.0" ?>
        <?xml-stylesheet type="text/xsl" href="scanlog.xsl" ?>
        <USAXS_SCAN_LOG version="1.0">
            <scan id="125:/share1/USAXS_data/2014-10/10_13_AgUrCh_josh.dat" number="125" state="complete" type="FlyScan">
                <title>Strip2_15_4min</title>
                <file>/share1/old_USAXS_data/2014-10/10_13_AgUrCh_josh.dat</file>
                <started date="2014-10-13" time="22:08:08"/>
                <ended date="2014-10-13" time="22:09:59"/>
            </scan>
            <scan id="126:/share1/USAXS_data/2014-10/10_13_AgUrCh_josh.dat" number="126" state="complete" type="FlyScan">
                <title>Strip2_15_6min</title>
                <file>/share1/old_USAXS_data/2014-10/10_13_AgUrCh_josh.dat</file>
                <started date="2014-10-13" time="22:10:26"/>
                <ended date="2014-10-13" time="22:12:18"/>
            </scan>

    """
    # print(xml_filename)
    tree = lxml.etree.parse(xml_filename)

    for _i_, node in enumerate(tree.getroot().xpath('scan')):
        scan_id = node.get("id")
        if scan_id is None:
            msg = "file {}, node {} has no @id attribute".format(xml_filename, _i_)
            raise ValueError(msg)
        scan = dict(
            xml_filename = xml_filename, 
            xml_id = scan_id,
            uuid = random_uuid(),
            )
        for key in "number state type".split():
            scan[key] = node.get(key)
        for subnode in node:
            if subnode.tag in ("started", "ended"):
                d = subnode.get("date")
                t = subnode.get("time")
                scan[subnode.tag] = "{} {}".format(d, t)
            else:
                scan[subnode.tag] = subnode.text
        # print(json.dumps(scan, indent=2))
        # if scan_id in db:
        #     print("known", json.dumps(db[scan_id], indent=2))
        #     print("new", json.dumps(scan, indent=2))
        #     print(scan_id + " already known ... updating with new information")
        db[scan_id] = scan      # replaces if already known


def make_start_event(scan):
    """
    return a `start` event dictionary from the scan information dictionary
    
    :see: https://nsls-ii.github.io/bluesky/documents.html?highlight=start#overview-of-a-run
    
    typical scan information dictionary::
    
        {
          "xml_id": "15:/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "uuid": "927e10f9fe27474785e9c41d0ffb6a4c", 
          "title": "GlassyCarbonM4_20100eV", 
          "started": "2016-10-05 22:08:08", 
          "number": "15", 
          "ended": "2016-10-05 22:09:59", 
          "state": "complete", 
          "file": "/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "xml_filename": "2017-04-04-scanlog.xml", 
          "type": "FlyScan"
        }
    
    typical Databroker start document, in mongodb.metadatastore "run_start" collection::

        {'beamline_id': 'developer',
         'detectors': ['scaler'],
         'hints': {'dimensions': [[['m1'], 'primary']]},
         'login_id': 'jemian@otz.aps.anl.gov',
         'md': {'demo': 'MONA motor scan', 'purpose': 'development'},
         'motors': ['m1'],
         'num_intervals': 10,
         'num_points': 11,
         'pid': 18543,
         'plan_args': {
             'detectors': ["EpicsScaler(prefix='gov:scaler1', name='scaler', read_attrs=['channels', 'time'], configuration_attrs=['preset_time', 'presets', 'gates', 'names', 'freq', 'auto_count_time', 'count_mode', 'delay', 'auto_count_delay', 'egu'])"],
          'motor': "EpicsMotor(prefix='gov:m1', name='m1', settle_time=0.0, timeout=None, read_attrs=['user_readback', 'user_setpoint'], configuration_attrs=['motor_egu', 'velocity', 'acceleration', 'user_offset', 'user_offset_dir'])",
          'num': 11,
          'per_step': 'None',
          'start': -2,
          'stop': 0},
         'plan_name': 'scan',
         'plan_pattern': 'linspace',
         'plan_pattern_args': {'num': 11, 'start': -2, 'stop': 0},
         'plan_pattern_module': 'numpy',
         'plan_type': 'generator',
         'proposal_id': None,
         'scan_id': 86,
         'time': 1513898975.0762107,
         'uid': '27a1daf5-d4a1-49b5-9415-64b6064d2064'}

    """
    event = OrderedDict()
    event["time"] = time_float(scan["started"])
    event["plan_name"] = scan["type"]
    event["uid"] = scan["uuid"]
    event["scan_id"] = scan["number"]
    # everything else in start document is optional
    event["time_text"] = scan["started"]
    event["SPEC"] = dict(
        filename = scan["file"],
        scan_number = scan["number"],
        scan_macro = scan["type"],
        title = scan["title"],
        )
    event["scanlog_id"] = scan["xml_id"]

    # print(json.dumps(event, indent=2))
    return event


def make_stop_event(scan):
    """
    return a `start` event dictionary from the scan information dictionary
    
    :see: https://nsls-ii.github.io/bluesky/documents.html?highlight=start#overview-of-a-run
    
    typical scan information dictionary::
    
        {
          "xml_id": "15:/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "uuid": "927e10f9fe27474785e9c41d0ffb6a4c", 
          "title": "GlassyCarbonM4_20100eV", 
          "started": "2016-10-05 22:08:08", 
          "number": "15", 
          "ended": "2016-10-05 22:09:59", 
          "state": "complete", 
          "file": "/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "xml_filename": "2017-04-04-scanlog.xml", 
          "type": "FlyScan"
        }
    
    typical Databroker stop document, in mongodb.metadatastore "run_stop" collection::

        {'exit_status': 'success',
         'num_events': {'primary': 11},
         'run_start': '27a1daf5-d4a1-49b5-9415-64b6064d2064',
         'time': 1513898982.243269,
         'uid': 'cbc6008f-a784-4ee4-825e-cdddf33db31d'}

    """
    if scan["state"] == "unknown":
        event = None        # do not report a 'stop' document
    else:
        event = OrderedDict()
        t = scan.get("ended", scan["started"])
        event["time"] = time_float(t)
        event["uid"] = random_uuid()
        event["run_start"] = scan["uuid"]
        event["exit_status"] = dict(
            complete="success", 
            scanning="aborted",
            # "failed" is another possible result, not used here
            )[scan["state"]]
        # everything else in start document is optional
        event["time_text"] = t
        event["scanlog_state"] = scan["state"]
        # event["num_events"] = 0     # no event documents known at this time
        
        # print(json.dumps(event, indent=2))
    return event


specfile = None

def parse_scan_data(scan):
    """
    try to read the SPEC data file to get the scan's data
    
    store that data back to the scan dictionary
    """
    if not os.path.exists(scan["file"]):
        return
    
    if specfile is not None:
        if specfile.get("fileName") != scan["file"]:
            specfile = None
    
    if specfile is None:
        specfile = spec2nexus.SpecDataFile(scan["file"])
    
    spec_scan = specfile.getScan(scan["number"])
    
    stream = []
    # TODO: now, make the descriptor and event documents
    scan["databroker_stream"] = stream


def make_document_stream(scans):
    """convert the scan data to document stream compatible with databroker"""
    document_stream = []

    for scan in scans.values():
        event = make_start_event(scan)
        document_stream.append(("start", event))
        
        data_streams = parse_scan_data(scan)
        if data_streams is not None:
            pass        # TODO:
        
        event = make_stop_event(scan)
        if event is not None:
            document_stream.append(("stop", event))
    
    return document_stream


def write_to_databroker(stream):
    with open("stream.json", "w") as fp:          # TODO: use databroker, this is interim
        json.dump(stream, fp, indent=2)


def main():
    scans = OrderedDict()
    for fname in sorted(os.listdir(".")):
        if fname.endswith(".xml"):
            print("file: " + fname)
            read_xml_file(fname, scans)
    print("{} scans".format(len(scans)))

    docs = make_document_stream(scans)
    print("{} events".format(len(docs)))

    write_to_databroker(docs)
    print("done")


if __name__ == "__main__":
    main()
