# The events 

The library contains the java bean that represent event envelop containing all the metadatas of an event. 

The envelop contains the fields: 

 * `id` : the id of the event 
 * `sequenceNum` : the last event number 
 * `eventType` : the type of the event 
 * `emissionDate` : the date 
 * `transactionId` : an id for a transaction, used in case of multiple event in a row 
 * `metadata` : a free metadata object for this event 
 * `event` : the event himself 
 * `context` : a free context object for this event  
 * `version` : the version number of the event 
 * `totalMessageInTransaction` : the number of messages in the transaction 
 * `numMessageInTransaction` : the number of the current message in the transaction, eg 2/5 
 * `entityId` : the unique id of the entity concerned by the event  
 * `userId` : the id of the user that generate this event 
 * `systemId` : the of the system that generate this event
 
The `eventType` and `version` can be used to get the schema of the `event`. 