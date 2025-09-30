INSERT INTO db.customer_event (customer_aggregate_identifier,event_type,event_timestamp,event_parameters)
SELECT customer_aggregate_identifier,event_type,event_timestamp,event_parameters
FROM staging.customer_event s
ON DUPLICATE KEY UPDATE
customer_aggregate_identifier = s.customer_aggregate_identifier, event_type = s.event_type, event_timestamp = s.event_timestamp, event_parameters = s.event_parameters
