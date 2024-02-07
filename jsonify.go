package go_ds_leveldb_sia

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	ds "github.com/ipfs/go-datastore"
)

type KeyValue struct {
	Key   ds.Key `json:"k"`
	Value []byte `json:"v"`
}

type KV struct {
	K string
	V []byte
}

func (kv *KeyValue) Encode() ([]byte, error) {
	d := KV{K: kv.Key.String(), V: kv.Value}
	return json.Marshal(d)
}

func (kv *KeyValue) Decode(data []byte) error {
	var d KV
	err := json.Unmarshal(data, &d)
	if err != nil {
		return err
	}
	kv.Key = ds.NewKey(d.K)
	kv.Value = d.V
	return nil
}

func (kv *KeyValue) FileName() (string, error) {
	h := sha256.New()
	_, err := h.Write(kv.Key.Bytes())
	if err != nil {
		return "", fmt.Errorf("writing key to hasher: %w", err)
	}
	return "leveldb/" + hex.EncodeToString(h.Sum(nil)), nil
}
