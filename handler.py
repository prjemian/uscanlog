#!/usr/bin/env python

"""
Convert the USAXS scan log files from XML to mongodb via JSON.

requires python 3.6 (for the timestamp())
"""


import datetime
import dateutil.parser
import json
import logging
import lxml.etree
import os
import sys
import uuid
# import databroker
from collections import OrderedDict
import spec2nexus.spec as spec


HOME = os.environ.get("HOME", "~")
MONGODB_YML = os.path.join(HOME, ".config/databroker/mongodb_config.yml")
STREAM_KEYWORD = "_stream_"
specdatafile_obj = None


def random_uuid():
    """return a random UUID"""
    try:
        s = uuid.uuid4().get_hex()
    except AttributeError:
        s = uuid.uuid4().hex
    return s


def time_text(time_float):
    return str(datetime.datetime.utcfromtimestamp(time_float))


def time_float(datestring):
    """return float(time) since UNIX epoch"""
    dt = dateutil.parser.parse(datestring)
    return dt.timestamp()


def cleanup_name(k):
    """
    cleanup "k" (no periods, white space, ...)
    """
    for c in (",", ".", " "):
        k = k.replace(c, "_")
    return k


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


def make_start_document(scan):
    """
    return a `start` document from the scan information dictionary

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
    doc = OrderedDict()
    doc["time"] = time_float(scan["started"])
    doc["plan_name"] = scan["type"]
    doc["uid"] = scan["uuid"]
    doc["scan_id"] = scan["number"]
    # everything else in start document is optional
    doc["time_text"] = scan["started"]
    doc["SPEC"] = OrderedDict(
        filename = scan["file"],
        scan_number = scan["number"],
        scan_macro = scan["type"],
        title = scan["title"],
        )
    doc["scanlog_id"] = scan["xml_id"]

    add_event_metadata(scan, doc, "start")

    # print(json.dumps(doc, indent=2))
    return doc


def make_stop_document(scan):
    """
    return a `stop` document from the scan information dictionary

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
        doc = None        # do not report a 'stop' document
    else:
        doc = OrderedDict()
        t = scan.get("ended", scan["started"])
        doc["time"] = time_float(t)
        doc["uid"] = random_uuid()
        doc["run_start"] = scan["uuid"]
        doc["exit_status"] = dict(
            complete="success",
            scanning="aborted",
            # "failed" is another possible result, not used here
            )[scan["state"]]
        # everything else in start document is optional
        doc["time_text"] = t
        doc["scanlog_state"] = scan["state"]
        # doc["num_events"] = 0     # supplied by data parsing

        # print(json.dumps(doc, indent=2))
    return doc


def process_SPEC_scan_data(scan):
    """
    return stream of descriptor and event documents
    """
    sdf = openSpecDataFile(scan["file"])
    if sdf is None:
        return
    spec_scan = sdf.getScan(scan["number"])
    spec_scan.interpret()

    document_stream = []

    doc = OrderedDict()     # descriptor doc
    t_base = doc["time"] = time_float(spec_scan.date)
    descriptor_uuid = doc["uid"] = random_uuid()
    doc["run_start"] = scan["uuid"]
    dk = doc["data_keys"] = {}
    for k in spec_scan.data.keys():
        k_clean = cleanup_name(k)
        dk[k_clean] = dict(
            dtype = 'number',
            source = 'SPEC data file',
            shape = [],
            # units = "unknown",
            # precision = 3,
            SPEC_name = k
            )
    # doc["object_keys"] = {}        # TODO: required?
    # doc["configuration"] = {}      # TODO: required?
    # doc["hints"] = {}              # TODO: required?
    # everything else in document is optional
    doc["time_text"] = time_text(t_base)
    document_stream.append(("descriptor", doc))

    num_observations = len(spec_scan.data[spec_scan.column_first])
    # TODO: which of these two?
    # scan[STREAM_KEYWORD]["stop.num_events"] = {'primary': num_observations}
    scan[STREAM_KEYWORD]["stop.num_events"] = num_observations
    for i in range(num_observations):
        doc = OrderedDict()     # event doc
        t = spec_scan.data.get("Epoch", 0)[i] + t_base
        t_text = time_text(t)
        doc["time"] = t
        doc["uid"] = random_uuid()
        doc["seq_num"] = i+1
        doc["descriptor"] = descriptor_uuid
        doc["data"] = {}
        doc["timestamps"] = {}
        for k, v in spec_scan.data.items():
            k_clean = cleanup_name(k)
            doc["data"][k_clean] = v[i]
            doc["timestamps"][k_clean] = t
        # everything else in document is optional
        doc["time_text"] = t_text
        document_stream.append(("event", doc))

    return document_stream


def add_event_metadata(scan, event, doc_type):
    if scan.get(STREAM_KEYWORD) is not None:
        for key, value in scan[STREAM_KEYWORD].items(): # key:  "start.this.that.the.other"
            if key.startswith(doc_type+"."):            # "start"
                base = event
                parts = key.split(".")[1:]              # ['this', 'that', 'the', 'other']

                # build up the dictionary stack as needed
                for item in parts[:-1]:                 # ['this', 'that', 'the']
                    if item not in base:
                        base[item] = OrderedDict()
                    base = base[item]

                base[parts[-1]] = value                 # 'other'


def openSpecDataFile(filename):
    global specdatafile_obj
    if specdatafile_obj is not None:
        if hasattr(specdatafile_obj, "fileName"):
            if specdatafile_obj.fileName != filename:
                specdatafile_obj = None
        else:
            specdatafile_obj = None

    if specdatafile_obj is None:
        try:
            specdatafile_obj = spec.SpecDataFile(filename)
        except spec.NotASpecDataFile as _exc:
            return

    return specdatafile_obj


def parse_scan_data(scan):
    """
    try to read the SPEC data file to get the scan's data

    store that data back to the scan dictionary
    """
    if not os.path.exists(scan["file"]):
        return

    sdf = openSpecDataFile(scan["file"])
    if sdf is None:
        return

    spec_scan = sdf.getScan(scan["number"])
    spec_scan.interpret()

    stream = []

    scanmeta = scan[STREAM_KEYWORD] = OrderedDict()
    scanmeta["start.SPEC.command"] = spec_scan.scanCmd
    for k, v in spec_scan.metadata.items():
        k_clean = cleanup_name(k)
        scanmeta["start.metadata." + k_clean + ".value"] = v
        scanmeta["start.metadata." + k_clean + ".name"] = k

    # special commands that record data outside of SPEC:
    macro_name = spec_scan.scanCmd.split()[0]
    if macro_name in ('SAXS', 'WAXS'):
        # SAXS  ./01_30_Setup_saxs/AgBeLAB6_0001.hdf    20    20    1    5     1
        scanmeta["start.SPEC.hdf5_file"] = spec_scan.scanCmd.split()[1]
    elif macro_name in ('FlyScan'):
        # FlyScan  ar 8.76068 0 7.1442 2.5e-05
        match_text = "FlyScan file name = "
        for comm in spec_scan.comments: # pull it from the comments
            p = comm.find(match_text)
            if p >= 0:
                p += len(match_text)
                scanmeta["start.SPEC.hdf5_file"] = comm[p:-1]
                break
    else:
        stream += process_SPEC_scan_data(scan)

    return stream


def make_document_stream(scans):
    """convert the scan data to document stream compatible with databroker"""
    document_stream = []

    for scan in scans.values():
        data_stream = parse_scan_data(scan)

        doc = make_start_document(scan)
        document_stream.append(("start", doc))

        if data_stream is not None and len(data_stream) > 0:
            document_stream += data_stream

        doc = make_stop_document(scan)
        if doc is not None:
            document_stream.append(("stop", doc))

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
    print("{} docs".format(len(docs)))

    write_to_databroker(docs)
    print("done")


if __name__ == "__main__":
    main()
