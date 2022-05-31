# CLI: Quotas

[![cli:quotas:build](https://github.com/jeqo/poc-apache-kafka/actions/workflows/cli-quotas-build.yml/badge.svg)](https://github.com/jeqo/poc-apache-kafka/actions/workflows/cli-quotas-build.yml)

CLI to manage Kafka Quotas. 

- [Documentation](./docs/kfk-quotas.adoc)

## Install

### Brew

Add tap:

```shell
brew tap jeqo/tap
```

Install `kfk-quotas`:

```shell
brew install kfk-quotas
```

### Manual

Find the latest release at: <https://github.com/jeqo/poc-apache-kafka/releases>

```shell
VERSION=0.1.0
wget https://github.com/jeqo/poc-apache-kafka/releases/download/cli-quotas-v$VERSION/kfk-quotas-$VERSION-linux-x86_64.tar.gz
tar xf kfk-quotas-$VERSION-linux-x86_64.tar.gz
mv kfk-quotas-$VERSION-linux-x86_64/bin/kfk-quotas /usr/local/bin/.
```
