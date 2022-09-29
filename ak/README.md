# ak-cli

Current CLIs under Apache Kafka project:

- admin (24):
  - [ ] topics (`kafka-topics`)
    - replica
      - [ ] verification (`kafka-replica-verification`)
      - [ ] leader-election (`kafka-leader-election`)
    - partitions
      - [ ] reassign (`kafka-reassign-partitions`)
      - [ ] offsets (`kafka-get-offsets`)
  - records
    - [ ] delete (`kafka-delete-records`)
  - producer
    - [ ] console (`kafka-console-producer`)
    - [ ] verifiable (`kafka-verifiable-producer`)
    - [ ] perf (`kafka-producer-perf-test`)
  - consumer
    - [ ] console (`kafka-console-consumer`)
    - [ ] verifiable (`kafka-verifiable-consumer`)
    - [ ] perf (`kafka-consumer-perf-test`)
    - [ ] groups (`kafka-consumer-groups`)
  - [ ] acls (`kafka-acls`)
  - [ ] delegation-topics (`kafka-delegation-topics`)
    - should be under `authentication` including SCRAM users?
  - [ ] cluster (`kafka-cluster`)
    - [ ] storage ? (`kafka-storage`)
  - broker
    - [ ] api-versions (`kafka-broker-api-versions`)
  - [ ] configs (`kafka-configs`)
  - [ ] transactions (`kafka-transactions`)
  - streams
    - application
      - [ ] reset-offset (`kafka-streams-application-reset`)
  - log
    - [ ] dump (`kafka-dump-log`)
    - [ ] dirs (`kafka-log-dirs`)
  - [ ] features (`kafka-features`)

non-admin/execution:
  - connect
      - distributed (`connect-distributed`)
      - mm (`connect-mirror-maker`)
      - standalone (`connect-standalone`)
  - server
    - start (`kafka-server-start`)
    - stop (`kafka-server-stop`)
  - storage (`kafka-storage`)
  - mirror-maker (`kafka-mirror-maker`)
  - metadata
    - shell (`kafka-metadata-shell`)
    - quorum (`kafka-metadata-quorum`)
  - kafka-run-class
  - trogdor

to be deprecated:
  - zookeeper
    - security-migration (`zookeeper-security-migration`)
    - server
      - start (`zookeeper-server-start`)
      - stop (`zookeeper-server-stop`)
    - shell (`zookeeper-shell`)
