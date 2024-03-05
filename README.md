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

## Usage

You can run the Sia backed IPFS node by either downloading the customised go-IPFS implementation from [here](https://github.com/IPFSR/kubo) or
you can use this plugin with a vanilla Kubo by replacing the `github.com/ipfs/go-ds-leveldb` with `github.com/IPFSR/go-ds-leveldb-sia` in file 
`plugin/plugins/levelds/levelds.go`

Please make sure to setup your renterd() first. Once it is running, export the following variables to a terminal and initiate a new IPFS node.

| Env Var                   | Default  | Description                                                              |
|---------------------------|----------|--------------------------------------------------------------------------|
| `IPFS_SIA_RENTERD_PASSWORD` |          | Renterd Password                                                         |
|`IPFS_SIA_RENTERD_WORKER_ADDRESS`|          | Renterd worker API address (ex: http://127.0.0.1:9980)                   |
|`IPFS_SIA_RENTERD_BUCKET`| IPFS     | A private bucket with this name will be created and used                 |
|`IPFS_SIA_SYNC_DELETE`| False(0) | If set to True(1), the DELETE operation will be synced to Renterd bucket |


### Instructions
* Set up a renterd node and note down the password and API address.
* Open a new terminal and clone the [IPFSR Kubo (go-IPFS)](https://github.com/IPFSR/kubo)
```shell
git clone https://github.com/IPFSR/kubo
```
* Enter the directory and build the binary
```shell
cd kubo
make build
```
* Export the `IPFS_SIA_RENTERD_PASSWORD` and `IPFS_SIA_RENTERD_WORKER_ADDRESS` environment variables.
* Initiate a new IPFS node
```shell
cmd/ipfs/ipfs init
```
* Verify that a new bucket named "IPFS" was created the Renterd and it contains a directory names "leveldb" with 5 files.
* Your IPFS node is now connected to the Renterd node for backing up levelDB metadata.

### Testing
1. Setup a new IPFS node with this library and backing `renterd` node by following the above instructions
2. Install a levelDB CLI tool like `go install github.com/cions/leveldb-cli/cmd/leveldb@latest`
3. Access the levelDB and list out all the key:value pairs
4. Delete all the key:value pairs using the leveldb CLI
5. Run `ipfs files ls /`.
6. Access the levelDB and list out all the key:value pairs. You should see all the key:values restore back to the DB.
7. Please note that `IPFS_SIA_SYNC_DELETE` only works for the DELETE operations carried out by IPFS process and not an external tool. Thus even if DELETE operations are being synced, any externally deleted key using CLI will be restored. 

## Contribute

PRs accepted.

Small note: If editing the README, please conform to the [standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT
