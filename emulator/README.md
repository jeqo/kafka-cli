# Kafka Zip

A file format that represents set of records in kafka

- We could "drop" this file into a cluster and reproduce a set of events and reproduce scenarios.
- Say:
    - "this A events happen one after the other in these topics/partitions"
    - "then after (period) the following events B are added"
    - "an app X will consume these A and B events and will produce some results"
- We have these assumptions as unit-tests and documentation, but are hard to reproduce.
- We could take events from topics and package them as a file, edit them (e.g. change timestamps), and drop them again into Kafka.
- This will require a CLI tool to package/unpackage events, and reproduce scenarios.
- This will require a file format (most probably CSV-like) with specific format, and custom columns to tweak
- Would be interesting to see how this can help to recreate state on changelog topics.
- Instead of offsets/timestamp, it should contain the duration between events.
- We could also repeat the whole sequence with a certain frequency, in this way have a continous produce with a common pattern. Even 1 event should be enough to kick a execution.

Implementation:

- File format:
    - CSVs
        - One csv per topic-partition
        - package it as excel
    - Fields:
        - `topic`
        - `partition`
        - `after_ms`
        - `key_format` (default or sr-based)
        - `key` (string or binary or columnar version)
        - `value_format` (default or sr-based)
        - `value` (string or binary or columnar version)

- CLI
    - pack
        - args:
            - kafka cluster
            - topics
            - as excel
    - unpack
        - args:
            - kafka cluster
            - repeat: true/false
        - dry-run (sout)
