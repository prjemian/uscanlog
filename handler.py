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
    return float(dt.timestamp())


def make_start_event(scan):
    """
    return a `start` event dictionary from the scan information dictionary
    
    typical scan information dictionary::
    
        {
          "xml_id": "15:/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "uuid": "927e10f9fe27474785e9c41d0ffb6a4c", 
          "title": "GlassyCarbonM4_20100eV", 
          "started": null, 
          "number": "15", 
          "ended": null, 
          "state": "complete", 
          "file": "/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "xml_filename": "2017-04-04-scanlog.xml", 
          "type": "FlyScan"
        }
    
    typical Databroker start document, in mongodb.metadatastore "run_start" collection::

        {
            "OPHYD_VERSION": "1.0.0",
            "plan_type": "generator",
            "pid": 19927,
            "detectors": [
                "det1"
            ],
            "plan_pattern_args": {
                "stop": 0.29932260461360183,
                "num": 101,
                "start": -0.29932260461360183
            },
            "motors": [
                "motor4"
            ],
            "tune_num": 101,
            "num_intervals": 100,
            "tune": true,
            "tune_start": -0.29932260461360183,
            "uid": "ef7a616e-ca7f-423d-ae31-3beff4201ca2",
            "tune_name": "usaxs_motor_tune",
            "tune_axis": "motor4",
            "tune_finish": 0.29932260461360183,
            "tune_pretune_position": 0.42,
            "plan_pattern_module": "numpy",
            "login_id": "prjemian@ookhd",
            "tune_time_s": 1,
            "hints": {
                "dimensions": [
                    [
                        [
                            "motor4"
                        ],
                        "primary"
                    ]
                ]
            },
            "beamline_id": "developer",
            "num_points": 101,
            "plan_pattern": "linspace",
            "plan_name": "rel_scan",
            "BLUESKY_VERSION": "1.1.0",
            "plan_args": {
                "motor": "TunableSynAxis_v1(prefix='', name='motor4', read_attrs=['readback', 'setpoint'], configuration_attrs=[])",
                "stop": 0.29932260461360183,
                "num": 101,
                "start": -0.29932260461360183,
                "detectors": [
                    "SynGauss(name='det1', value=3.37341840745714e-09, timestamp=1519492987.8068526)"
                ],
                "per_step": "None"
            },
            "_id": "5a91a04d7a30a54dd7a40053",
            "time": 1519493197.3155074,
            "scan_id": 874,
            "tune_datetime": "2018-02-24 11:26:37.305737",
            "tune_det": "det1"
        }

    """
    event = dict(event_type="start", uid=scan["uuid"])
    event["time"] = time_float(scan["started"])
    event["SPEC"] = dict(
        filename = scan["file"],
        scan_number = scan["number"],
        title = scan["title"],
        )
    event["scan_id"] = scan["number"]
    event["plan_name"] = scan["type"]
    
    print(json.dumps(event, indent=2))
    return event


def make_stop_event(scan):
    """
    return a `start` event dictionary from the scan information dictionary
    
    typical scan information dictionary::
    
        {
          "xml_id": "15:/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "uuid": "927e10f9fe27474785e9c41d0ffb6a4c", 
          "title": "GlassyCarbonM4_20100eV", 
          "started": null, 
          "number": "15", 
          "ended": null, 
          "state": "complete", 
          "file": "/share1/USAXS_data/2016-10/10_05_Setup.dat", 
          "xml_filename": "2017-04-04-scanlog.xml", 
          "type": "FlyScan"
        }
    
    typical Databroker stop document, in mongodb.metadatastore "run_stop" collection::

        {
            "uid": "e56a8588-2d41-4bbc-97bd-3754675ceae2",
            "run_start": "ef7a616e-ca7f-423d-ae31-3beff4201ca2",
            "_id": "5a91a04e7a30a54dd7a400ba",
            "time": 1519493198.5081244,
            "exit_status": "success",
            "num_events": {
                "primary": 101
            }
        }

    """
    if scan["state"] == "unknown":
        event = None        # do not report a 'stop' document
    else:
        event = dict(event_type="stop", uid=random_uuid(), run_start=scan["uuid"])
        event["time"] = time_float(scan.get("ended", scan["started"]))
        event["exit_status"] = dict(
            complete="success", 
            scanning="aborted",
            # "failed" is another possible result, not used here
            )[scan["state"]]
        
        print(json.dumps(event, indent=2))
    return event


def read_xml_file(xml_filename, db):
    """
    read the XML scanLog file, log scans into db
    
    ::

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
            xml_id = id,
            uuid = scan_id(),
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


def make_events(scan, scans):
    db = dict(start=[], stop=[])
    handlers = dict(start=make_start_event, stop=make_stop_event)
    for key in "start stop".split():
        event = handlers[key](scan)
        if event is not None:
            db[key] = event


def main():
    scans = OrderedDict()
    for fname in os.listdir("."):
        if fname.endswith(".xml"):
            read_xml_file(fname, scans)


if __name__ == "__main__":
    main()
