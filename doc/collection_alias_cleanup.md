How collection and alias cleanup work
====

The Catalog Search full re-indexing dag as well as the AZ and Web-Content
harvesting dags create artifacts in the Solr database such as aliases and
collections that need to eventually be cleaned up.

This process could of course be done manually, but it is an improvement if the
dags themselves could take care of this cleanup step.

To that end, we have added steps to these dags that together work to cleanup
such artifacts.


The collection and alias artifact cleanup work the same way so I will only
describe the collection cleanup here.


## Keeping track of what to delete (push_collection)
The first step is to have a way to keep track of what to delete. We can't just
delete what we create now because we need what we create now. To this end we
will use the airflow variable mechanism as a way to persist/remember
collections to be deleted.

The natural data structure for such a variable would be a list because we will
need to keep track of more than just one item (think for example if we fail to
delete something we would need to continue to keep track of whatever we fail to
delete so that we can try again the next time).

To that end we have created a `ListVariable` model which is a simple wrapper
around the airflow variable to treat that variable as a list.  We can push new
values into our variable using its `push` method.  For example
`ListVariable.push('foo', 'bar')` would push the value 'bar' into the list
variable saved as 'foo'.

Note that `ListVariable.push` creates the new list variable if it doesn't
already exist.

Note also that `ListVariable.push` will only add unique values by default.

We have created the `PushVariable` operator which is basically a wrapper to use
the `ListVariable` model in a task context (nothing new here just an operator).


## Deleting what we saved to our list variable (delete_collections)
Now that we have saved something to the list variable, we will want to process
it.

So we have created two airflow operators `DeleteCollectionListVariable` and
`DeleteAliasListVariable`. As their names would suggest these operators iterate
over the values in a ListVariable instance and try to delete either the
collections or aliases respectively.

### The safeguards
There are some safeguards that we would want in place.  We have already
mentioned one, which is to say we wouldn't want to delete what we just created.
So by default `DeleteCollectionsListVariable` will skip whatever value is last
in the list (it inherits this behavior from `DeleteCollectionBatch`). The
number of items that it skips can be configured using the `skip_from_last`
parameter.  So if you want to save at least the last three collections you
could set `skip_from_last=3`.

`DeleteCollectionBatch` and `DeleteCollectionListVariable` by extension) also
allows us to skip specific collections listed in the `skip_included` parameter.
Thus, if we wanted to always skip any collection named 'foo', or 'bar' we could
set `skip_included=['foo', 'bar']`.

Finally if neither of these safeguard suffices, we can also skip processing
a value by whatever matches a regular expression set in the `skip_matching`
parameter.

## Summary
To summarize, our workflow is that we push values into a `ListVariable` using
the `PushVariable` operator.  Then, we iterate and process over those values
using the `DeleteCollectionListVariable` or `DeleteAliasListVariable` operator.
