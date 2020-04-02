# A useful collection of Kafka Connect Transformations

This is a working progress repository for useful Kafka Connect Transformations.
Currently it have SMT's for:

* Convert a topic name from CamelCase to CAMEL_CASE
* Remove a field from a message 
* Flag a messages (in the headers) if it has errors
* Route a message where the error flag is ON, to a DLQ topic.

## Notes

The SelectRouter and the FlagMessagesWithErrors transformation can be very useful to implement a DLQ in the Sink connectors.


