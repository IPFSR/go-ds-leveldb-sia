# go-ds-leveldb-sia
An implementation of go-datastore using leveldb with each key value pair backed by Sia Renterd node.
This implementation is a modified version of https://github.com/ipfs/go-ds-leveldb

`go-ds-leveldb-sia` implements the [go-datastore](https://github.com/ipfs/go-datastore) interface using a LevelDB backend.

## Lead Maintainer

[Mayank Pandey](https://github.com/LexLuthr)

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Install

This module can be installed like a regular go module:

```
go get github.com/ipfs/go-ds-leveldb-sia
```

## Architecture

![Architecture](docs/architecture.png?raw=true "Title")

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT