# `kfk-ctx` - Kafka CLI: context

[![cli:context:build](https://github.com/jeqo/kafka-cli/actions/workflows/cli-context-build.yml/badge.svg)](https://github.com/jeqo/kafka-cli/actions/workflows/cli-context-build.yml)

- [Documentation](./docs/kfk-ctx.adoc)

## Install

### Brew

Add tap:

```shell
brew tap jeqo/tap
```

Install `kfk-ctx`:

```shell
brew install kfk-ctx
```

### Manual

Find the latest release at: <https://github.com/jeqo/kafka-cli/releases>

```shell
VERSION=0.2.10
wget https://github.com/jeqo/kafka-cli/releases/download/cli-context-v$VERSION/kfk-ctx-$VERSION-linux-x86_64.tar.gz
tar xf kfk-ctx-$VERSION-linux-x86_64.tar.gz
mv kfk-ctx-$VERSION-linux-x86_64/bin/kfk-ctx /usr/local/bin/.
```

## How to use

### Create and test contexts

[![asciicast](https://asciinema.org/a/uvnrAtccwqxVa4siJDD0TnWRx.svg)](https://asciinema.org/a/uvnrAtccwqxVa4siJDD0TnWRx)

### Connect to secure clusters (Confluent Cloud) with SASL PLAIN

[![asciicast](https://asciinema.org/a/MXDxnSlgXORPUBJRooGnOFh0A.svg)](https://asciinema.org/a/MXDxnSlgXORPUBJRooGnOFh0A)

### Manage Schema Registry contexts and kcat helper

[![asciicast](https://asciinema.org/a/59TulLyBo7ivMAZhofwrCdhvk.svg)](https://asciinema.org/a/59TulLyBo7ivMAZhofwrCdhvk)
