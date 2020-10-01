"""
This module is a placeholder for importing custom airflow operators.

Do not add methods/classes to this module.  Instead create them in separate
modules and import them below.
"""
from cob_datapipeline.operators.push_variable import PushVariable

from cob_datapipeline.operators.solr_api.base_operator\
        import SolrApiBaseOperator,\
               BatchMixin,\
               ListVariableMixin

from cob_datapipeline.operators.solr_api.delete_alias\
        import DeleteAlias,\
               DeleteAliasBatch,\
               DeleteAliasListVariable

from cob_datapipeline.operators.solr_api.delete_collection \
        import DeleteCollection,\
               DeleteCollectionBatch,\
               DeleteCollectionListVariable
