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
JSON_FILE = "stream.json"
MIN_REPORT_INTERVAL_S = 5.0
MIN_REPORT_INTERVAL_I = 1000

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('uscanlog_handler')
logger.info("starting...")


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


def time_now():
    dt = datetime.datetime.now()
    return dt.timestamp()


def cleanup_name(k):
    """
    cleanup "k" (no periods, white space, ...)
    """
    for c in (",", ".", " ", "-"):
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
    tree = lxml.etree.parse(xml_filename)
    logger.info("reading from scanlog XML file: " + xml_filename)

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
        # logger.warning(json.dumps(scan, indent=2))
        if scan_id in db:
            logger.info(scan_id + " already known ... updating with new information")
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

    # logger.warning(json.dumps(doc, indent=2))
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
        
        add_event_metadata(scan, doc, "stop")

        # logger.warning(json.dumps(doc, indent=2))
    return doc


def determine_data_source(k, spec_scan):
    if k in spec_scan.positioner:
        data_source = 'SPEC positioner'
    elif k in spec_scan.header.positioner_xref.values():
        data_source = 'SPEC positioner name'
    elif k in spec_scan.header.positioner_xref:
        data_source = 'SPEC positioner mnemonic'
    # elif k in spec_scan.header.O[0]:    # TODO: flatten O[][] to O[]
    #     data_source = 'SPEC positioner (O)'
    # elif k in spec_scan.header.o[0]:
    #     data_source = 'SPEC positioner (o)'
    # elif k in spec_scan.header.J[0]:
    #     data_source = 'SPEC counter (J)'
    # elif k in spec_scan.header.j[0]:
    #     data_source = 'SPEC counter (j)'
    elif k in spec_scan.header.counter_xref.values():
        data_source = 'SPEC counter name'
    elif k in spec_scan.header.counter_xref:
        data_source = 'SPEC counter mnemonic'
    else:
        data_source = 'SPEC value'

    return data_source


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
            source = determine_data_source(k, spec_scan),
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

    dataset = spec_scan.data.get(spec_scan.column_first)
    if dataset is None:
        return          # no scan data!
    num_observations = len(dataset)
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
    # scanmeta["start.plan_args"] = spec_scan.scanCmd
    if hasattr(spec_scan, "T") and len(spec_scan.T) > 0:
        scanmeta["start.counter_basis"] = dict(
            description = "fixed time",
            value = float(spec_scan.T),
            units = "s",
            )
    elif hasattr(spec_scan, "M") and len(spec_scan.M) > 0:
        scanmeta["start.counter_basis"] = dict(
            description = "fixed monitor count",
            value = float(spec_scan.M),
            units = "counts",
            # TODO: identify the monitor
            )
    for k, v in sorted(spec_scan.metadata.items()):
        k_clean = cleanup_name(k)
        scanmeta["start.metadata." + k_clean + ".value"] = v
        scanmeta["start.metadata." + k_clean + ".name"] = k
    for k, v in sorted(spec_scan.positioner.items()):
        k_clean = cleanup_name(k)
        scanmeta["start.positioner." + k_clean + ".value"] = v
        scanmeta["start.positioner." + k_clean + ".name"] = k

    # special commands that record data outside of SPEC:
    macro_name = spec_scan.scanCmd.split()[0]
    if macro_name in ('SAXS', 'WAXS', 'pinSAXS',):
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
        data_stream = process_SPEC_scan_data(scan)
        if data_stream is not None:
            stream += data_stream

    return stream


def make_document_stream(scans):
    """convert the scan data to document stream compatible with databroker"""
    document_stream = []
    
    t_report = time_now() - 5
    i_report = -1

    with open(JSON_FILE, "w") as fp:
        logger.info("writing JSON to file: " + JSON_FILE)

        for _i, scan in enumerate(scans.values()):
            if time_now() >= t_report or _i >= i_report:
                logger.debug("{}: scan {} in make_document_stream()".format(
                    str(datetime.datetime.now()), _i+1))
                t_report = time_now() + MIN_REPORT_INTERVAL_S
                i_report = _i + MIN_REPORT_INTERVAL_I
            
            data_stream = parse_scan_data(scan)

            doc = make_start_document(scan)
            document_stream.append(("start", doc))

            if data_stream is not None and len(data_stream) > 0:
                document_stream += data_stream

            doc = make_stop_document(scan)
            if doc is not None:
                document_stream.append(("stop", doc))

            s = [json.dumps(doc, indent=2) for doc in document_stream]
            fp.write(",\n".join(s))
            document_stream = []

    return document_stream


def write_to_databroker(stream):
    pass
    with open(JSON_FILE, "w") as fp:          # TODO: use databroker, this is interim
        json.dump(stream, fp, indent=2)


def main():
    scans = OrderedDict()
    for fname in sorted(os.listdir(".")):
        if fname.endswith(".xml"):
            read_xml_file(fname, scans)
    logger.info("{} scans".format(len(scans)))

    docs = make_document_stream(scans)
    #logger.info("{} docs".format(len(docs)))

    #write_to_databroker(docs)
    #logger.info("done")


if __name__ == "__main__":
    main()
