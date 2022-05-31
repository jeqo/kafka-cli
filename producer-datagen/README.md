# `kfk-producer-datagen` - Kafka CLI Producer Datagen

[![cli:producer-datagen:build](https://github.com/jeqo/kafka-cli/actions/workflows/cli-producer-datagen-build.yml/badge.svg)](https://github.com/jeqo/kafka-cli/actions/workflows/cli-producer-datagen-build.yml)

The `kafka-producer-datagen` extends the default `ProducerPerformance` by combining the data-generation with the library behind [Datagen Source Connector](https://github.com/confluentinc/kafka-connect-datagen).

- [Documentation](./docs/kfk-producer-datagen.adoc)

## Install

### Brew

Add tap:

```shell
brew tap jeqo/tap
```

Install `kfk-producer-datagen`:

```shell
brew install kfk-producer-datagen
```

### Manual

Find the latest release at: <https://github.com/jeqo/kafka-cli/releases>

```shell
VERSION=0.2.2
wget https://github.com/jeqo/kafka-cli/releases/download/cli-producer-datagen-v$VERSION/kfk-producer-datagen-$VERSION-linux-x86_64.tar.gz
tar xf kfk-producer-datagen-$VERSION-linux-x86_64.tar.gz
mv kfk-producer-datagen-$VERSION-linux-x86_64/bin/kfk-producer-datagen /usr/local/bin/.
```

## How to use

### Generate sample message and produce to a topic

[![asciicast](https://asciinema.org/a/483231.svg)](https://asciinema.org/a/483231)

### Check topics and schemas you have access to

[![asciicast](https://asciinema.org/a/483238.svg)](https://asciinema.org/a/483238)

### Generate messages with interval

[![asciicast](https://asciinema.org/a/483235.svg)](https://asciinema.org/a/483235)

### Run performance tests with generated messages

[![asciicast](https://asciinema.org/a/483244.svg)](https://asciinema.org/a/483244)
