# Kafka Emulator - Rationale

## Requirements

### How to reproduce Kafka Streams join conditions

### Collaterals

#### Recreate Changelog topics

## Design Decisions

### Using SQLite as file-format

- CSV file-format doesn't have a friendly way to store bytes.
- CSV files can be created from SQLite, and vice-versa.

Examples:

```shell
sqlite-utils kfk-emulator.db "select * from records_v1" --csv > examples/records_v1.csv
```


