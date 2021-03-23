ALTER SYSTEM SET max_connections = 50;
ALTER SYSTEM RESET shared_buffers;
CREATE DATABASE eventsourcing;
CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
GRANT ALL PRIVILEGES ON DATABASE "eventsourcing" to eventsourcing;