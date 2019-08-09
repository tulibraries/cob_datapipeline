from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch
from airflow import AirflowException


def oai_harvest(oai_endpoint_url, set_name, process_method, process_method_args, **kwargs):
    """ Harvest data from a given OAI endpoint
        oai_endpoint_url: http URL for the OAI endpoint
        set_name: OAI set to harvest from
        process_method: a function which takes in a record string and a dictionary of args, and
            returns that dictionary of args to be passed to the next record call.
            Responsible for writing the processed record for writing to file.
        **process_method_kwargs: user defined kwargs to be passed to process_method
        **kwargs: optional dictionary of harvest options.
                  if either from or until are present, they will be used to
                  limit the OAI results returned from the OAI feed
    """
    try:
        sickle = Sickle(oai_endpoint_url)
        harvest_args = {
            'metadataPrefix': 'marc21',
            'set': set_name
        }

        if 'harvest_from_date' in kwargs:
            harvest_args['from'] = kwargs['harvest_from_date']

        if 'harvest_until_date' in kwargs:
            harvest_args['until'] = kwargs['harvest_until_date']

        print("Harvesting {}".format(harvest_args))
        try:
            records = sickle.ListRecords(**harvest_args)
        except NoRecordsMatch as ex:
            print(str(ex))
            print("No records matched the date range given")
            records = []

        process_args = process_method_args
        for record in records:
            process_args = process_method(record, process_args)

    except Exception as ex:
        print(str(ex))
        if record is not None:
            print(record.raw)
        raise AirflowException('Harvest failed.')
