# scanlog_translate

Convert the USAXS scan log files from XML to mongodb via JSON.

## General algorithm

1. ingest the XML into python list of event dictionaries.
1.a. loop over all XML files
1.b. eliminate redundancies
1.c. separate events for start and end events
1.d. tag each event with unique UUID (replaces existing @id attribute)
1.e. associate each end event with its start event by start's UUID
2. convert each event dictionary into JSON events
2.a json.dumps(event)
2.b (interim) write all events to text file as JSON
3. write each JSON event to mongodb as unique document
3.a get mongodb credentials from bluesky
3.b mongodb database name: scanlog


## Can we write into Databroker?

Can not exactly use the Databroker for this since the USAXS scanLog
has no descriptor or event information, nor does it have the additional
information expected by a "start" document.
