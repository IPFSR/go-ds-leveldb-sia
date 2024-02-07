package go_ds_leveldb_sia

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/syndtr/goleveldb/leveldb"
	ldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"

	logging "github.com/ipfs/go-log/v2"

	rapi "go.sia.tech/renterd/api"
	rbclient "go.sia.tech/renterd/bus/client"
	rwclient "go.sia.tech/renterd/worker/client"
)

var log = logging.Logger("sia-leveldb")

const (
	SIA_PASS     = "IPFS_SIA_RENTERD_PASSWORD"
	SIA_ADDR     = "IPFS_SIA_RENTERD_WORKER_ADDRESS"
	SIA_BUCKET   = "IPFS_SIA_RENTERD_BUCKET"
	SIA_SYNC_DEL = "IPFS_SIA_SYNC_DELETE"
	defbucket    = "IPFS"
)

type Datastore struct {
	*accessor
	DB   *leveldb.DB
	path string
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)
var _ ds.Txn = (*transaction)(nil)
var _ ds.PersistentDatastore = (*Datastore)(nil)
var _ ds.Batching = (*Datastore)(nil)
var _ ds.Batch = (*leveldbBatch)(nil)

// Options is an alias of syndtr/goleveldb/opt.Options which might be extended
// in the future.
type Options opt.Options

// NewDatastore returns a new datastore backed by leveldb
//
// for path == "", an in memory backend will be chosen
func NewDatastore(path string, opts *Options) (*Datastore, error) {
	var nopts opt.Options
	if opts != nil {
		nopts = opt.Options(*opts)
	}

	var err error
	var db *leveldb.DB

	if path == "" {
		db, err = leveldb.Open(storage.NewMemStorage(), &nopts)
	} else {
		db, err = leveldb.OpenFile(path, &nopts)
		if ldberrors.IsCorrupted(err) && !nopts.GetReadOnly() {
			db, err = leveldb.RecoverFile(path, &nopts)
		}
	}

	if err != nil {
		return nil, err
	}
	bucket := defbucket
	syncDeletes := false
	rPass, ok := os.LookupEnv(SIA_PASS)
	if !ok {
		return nil, fmt.Errorf("enviroment varaible '%s' must be set", SIA_PASS)
	}
	rAddr, ok := os.LookupEnv(SIA_ADDR)
	if !ok {
		return nil, fmt.Errorf("enviroment varaible '%s' must be set", SIA_ADDR)
	}
	rBucket, ok := os.LookupEnv(SIA_BUCKET)
	if ok {
		bucket = rBucket
	}
	log.Infof("using the bucket %s for renterd", bucket)
	sd, ok := os.LookupEnv(SIA_SYNC_DEL)
	if ok {
		syncD, err := strconv.ParseBool(sd)
		if err != nil {
			return nil, fmt.Errorf("parsing '%s': %w", SIA_SYNC_DEL, err)
		}
		if syncD {
			syncDeletes = syncD
		}
		log.Infof("sync DELETE enabled: %t", syncDeletes)
	}

	ctx := context.Background()
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rbus := rbclient.New(rAddr+"/api/bus", rPass)
	cberr := rbus.CreateBucket(cctx, bucket, rapi.CreateBucketOptions{Policy: rapi.BucketPolicy{PublicReadAccess: false}})
	if cberr != nil {
		if !strings.Contains(cberr.Error(), "bucket already exists") {
			return nil, fmt.Errorf("creating bucket %s using renterd bus api: %w", bucket, cberr)
		}
	}
	renterd := rwclient.New(rAddr+"/api/worker", rPass)

	ds := Datastore{
		accessor: &accessor{
			ldb:         db,
			syncWrites:  true,
			closeLk:     new(sync.RWMutex),
			wClient:     renterd,
			bClient:     rbus,
			bucket:      bucket,
			syncDeletes: syncDeletes,
		},
		DB:   db,
		path: path,
	}
	err = ds.Restore(ctx)
	if err != nil {
		return nil, err
	}
	return &ds, nil
}

// An extraction of the common interface between LevelDB Transactions and the DB itself.
//
// It allows to plug in either inside the `accessor`.
type levelDbOps interface {
	Put(key, value []byte, wo *opt.WriteOptions) error
	Get(key []byte, ro *opt.ReadOptions) (value []byte, err error)
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
	Delete(key []byte, wo *opt.WriteOptions) error
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// Datastore operations using either the DB or a transaction as the backend.
type accessor struct {
	ldb        levelDbOps
	syncWrites bool
	closeLk    *sync.RWMutex

	// Sia renterd clients
	wClient     *rwclient.Client
	bClient     *rbclient.Client
	bucket      string
	syncDeletes bool
}

// Put adds the key:value to the levelDB and backs it up to the connected renterd node
// The operation is idempotent in nature allow retry in case of failure in levelDB op
func (a *accessor) Put(ctx context.Context, key ds.Key, value []byte) error {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	kv := KeyValue{Key: key, Value: value}
	path, err := kv.FileName()
	if err != nil {
		return fmt.Errorf("generating file path for renterd: %w", err)
	}
	data, err := kv.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode the data for PUT on renterd: %w", err)
	}
	_, err = a.wClient.UploadObject(ctx, bytes.NewReader(data), a.bucket, path, rapi.UploadObjectOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "object already exists") {
			return fmt.Errorf("performing PUT on renterd: %w", err)
		}
	}
	return a.ldb.Put(key.Bytes(), value, &opt.WriteOptions{Sync: a.syncWrites})
}

// Sync is a no ops implementation for consistency with go-datastore interface
func (a *accessor) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

// Get fetches the key:value from the levelDB and in case levelDB does not have the data, it
// fetches from the renterd and adds it to levelDB before returning the output to caller.
// We get ds.ErrNotFound when key does not exist in either location
func (a *accessor) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	val, err := a.ldb.Get(key.Bytes(), nil)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			kv := KeyValue{Key: key}
			path, err := kv.FileName()
			if err != nil {
				return nil, err
			}
			res, err := a.wClient.GetObject(ctx, a.bucket, path, rapi.DownloadObjectOptions{})
			if err != nil {
				if strings.Contains(err.Error(), "object not found") {
					return nil, ds.ErrNotFound
				}
				return nil, fmt.Errorf("performing GET on renterd: %w", err)
			}
			data, err := io.ReadAll(res.Content)
			if err != nil {
				return nil, fmt.Errorf("reading response for GET on renterd: %w", err)
			}
			if err := kv.Decode(data); err != nil {
				return nil, err
			}
			return kv.Value, nil
		}
		return nil, err
	}
	return val, nil
}

// Has checks if the key:value exists in the levelDB and in case levelDB does not have the data, it
// fetches from the renterd and adds it to levelDB before returning the output to caller.
func (a *accessor) Has(ctx context.Context, key ds.Key) (bool, error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	exists, err := a.ldb.Has(key.Bytes(), nil)
	if err != nil {
		return exists, err
	}
	if !exists {
		kv := KeyValue{Key: key}
		path, err := kv.FileName()
		if err != nil {
			return false, err
		}
		res, err := a.wClient.GetObject(ctx, a.bucket, path, rapi.DownloadObjectOptions{})
		if err != nil {
			if strings.Contains(err.Error(), "object not found") {
				return false, ds.ErrNotFound
			}
			return false, fmt.Errorf("performing HAS on renterd: %w", err)
		}
		data, err := io.ReadAll(res.Content)
		if err != nil {
			return false, fmt.Errorf("reading response for HAS on renterd: %w", err)
		}
		if err := kv.Decode(data); err != nil {
			return false, err
		}
		err = a.ldb.Put(key.Bytes(), kv.Value, &opt.WriteOptions{Sync: a.syncWrites})
		if err != nil {
			// Don't return false if leveldb put fails as next time HAS or GET are called, we can
			// always get the key:value from Renterd
			exists = true
		}
	}
	return exists, nil
}

// GetSize provides the size of the data for a key. It uses Put under the hood and does not
// require a separate renterd extension
func (a *accessor) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	return ds.GetBackedSize(ctx, a, key)
}

// Delete removes the key:value from renterd first and then tries to perform the same OP on levelDB.
// The Op is idempotent to allow retry in case of failure on levelDB
func (a *accessor) Delete(ctx context.Context, key ds.Key) (err error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	if a.syncDeletes {
		kv := KeyValue{Key: key}
		path, err := kv.FileName()
		if err != nil {
			return err
		}
		err = a.wClient.DeleteObject(ctx, a.bucket, path, rapi.DeleteObjectOptions{})
		if err != nil {
			if !strings.Contains(err.Error(), "object not found") {
				return fmt.Errorf("performing DELETE on renterd :%w", err)
			}
		}
	}
	return a.ldb.Delete(key.Bytes(), &opt.WriteOptions{Sync: a.syncWrites})
}

func (a *accessor) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()
	var rnge *util.Range

	// make a copy of the query for the fallback naive query implementation.
	// don't modify the original so res.Query() returns the correct results.
	qNaive := q
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		rnge = util.BytesPrefix([]byte(prefix + "/"))
		qNaive.Prefix = ""
	}
	i := a.ldb.NewIterator(rnge, nil)
	next := i.Next
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
			qNaive.Orders = nil
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			next = func() bool {
				next = i.Prev
				return i.Last()
			}
			qNaive.Orders = nil
		default:
		}
	}
	r := dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			if !next() {
				return dsq.Result{}, false
			}
			k := string(i.Key())
			e := dsq.Entry{Key: k, Size: len(i.Value())}

			if !q.KeysOnly {
				buf := make([]byte, len(i.Value()))
				copy(buf, i.Value())
				e.Value = buf
			}
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			a.closeLk.RLock()
			defer a.closeLk.RUnlock()
			i.Release()
			return nil
		},
	})
	return dsq.NaiveQueryApply(qNaive, r), nil
}

// DiskUsage returns the current disk size used by this levelDB.
// For in-mem datastores, it will return 0.
func (d *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.path == "" { // in-mem
		return 0, nil
	}

	var du uint64

	err := filepath.Walk(d.path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		du += uint64(info.Size())
		return nil
	})

	if err != nil {
		return 0, err
	}

	return du, nil
}

// LevelDB needs to be closed.
func (d *Datastore) Close() (err error) {
	d.closeLk.Lock()
	defer d.closeLk.Unlock()
	return d.DB.Close()
}

type leveldbBatch struct {
	b          *leveldb.Batch
	db         *leveldb.DB
	closeLk    *sync.RWMutex
	syncWrites bool
	// Sia renterd clients
	wClient     *rwclient.Client
	bClient     *rbclient.Client
	bucket      string
	syncDeletes bool
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return &leveldbBatch{
		b:           new(leveldb.Batch),
		db:          d.DB,
		closeLk:     d.closeLk,
		syncWrites:  d.syncWrites,
		wClient:     d.wClient,
		bClient:     d.bClient,
		bucket:      d.bucket,
		syncDeletes: d.syncDeletes,
	}, nil
}

func (b *leveldbBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	kv := KeyValue{Key: key, Value: value}
	path, err := kv.FileName()
	if err != nil {
		return fmt.Errorf("generating file path for renterd: %w", err)
	}
	data, err := kv.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode the data for batch PUT on renterd: %w", err)
	}
	_, err = b.wClient.UploadObject(ctx, bytes.NewReader(data), b.bucket, path, rapi.UploadObjectOptions{})
	if err != nil {
		if !strings.Contains(err.Error(), "object already exists") {
			return fmt.Errorf("performing batch PUT on renterd: %w", err)
		}
	}
	b.b.Put(key.Bytes(), value)
	return nil
}

func (b *leveldbBatch) Commit(ctx context.Context) error {
	b.closeLk.RLock()
	defer b.closeLk.RUnlock()
	return b.db.Write(b.b, &opt.WriteOptions{Sync: b.syncWrites})
}

func (b *leveldbBatch) Delete(ctx context.Context, key ds.Key) error {
	if b.syncDeletes {
		kv := KeyValue{Key: key}
		path, err := kv.FileName()
		if err != nil {
			return err
		}
		err = b.wClient.DeleteObject(ctx, b.bucket, path, rapi.DeleteObjectOptions{})
		if err != nil {
			if !strings.Contains(err.Error(), "object not found") {
				return fmt.Errorf("performing batch DELETE on renterd :%w", err)
			}
		}
	}
	b.b.Delete(key.Bytes())
	return nil
}

// A leveldb transaction embedding the accessor backed by the transaction.
type transaction struct {
	*accessor
	tx *leveldb.Transaction
}

func (t *transaction) Commit(ctx context.Context) error {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	return t.tx.Commit()
}

func (t *transaction) Discard(ctx context.Context) {
	t.closeLk.RLock()
	defer t.closeLk.RUnlock()
	t.tx.Discard()
}

func (d *Datastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	tx, err := d.DB.OpenTransaction()
	if err != nil {
		return nil, err
	}
	accessor := &accessor{ldb: tx, syncWrites: false, closeLk: d.closeLk}
	return &transaction{accessor, tx}, nil
}

// Restore is called when a new datastore is requested. It checks the available objects on Renterd
// and restore them to the local levelDB is any key pair is missing. Restore will not fix any DELETE ops
// if syncDelete is set as True.
func (a *accessor) Restore(ctx context.Context) error {
	a.closeLk.RLock()
	defer a.closeLk.RUnlock()

	marker := ""
	var dlist []string

	for {
		res, err := a.bClient.ListObjects(ctx, a.bucket, rapi.ListObjectOptions{Prefix: "/leveldb/", Limit: 50, Marker: marker})
		if err != nil {
			return fmt.Errorf("generating list of keys on renterd: %w", err)
		}
		for _, o := range res.Objects {
			dlist = append(dlist, o.Name)
		}
		if !res.HasMore {
			break
		}
		marker = res.NextMarker
	}

	for _, filename := range dlist {
		kv, err := a.downloadData(ctx, filename)
		if err != nil {
			return err
		}
		has, err := a.ldb.Has(kv.Key.Bytes(), nil)
		if err != nil {
			return err
		}
		if !has {
			err = a.ldb.Put(kv.Key.Bytes(), kv.Value, &opt.WriteOptions{Sync: a.syncWrites})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *accessor) downloadData(ctx context.Context, path string) (KeyValue, error) {
	var kv KeyValue
	res, err := a.wClient.GetObject(ctx, a.bucket, path, rapi.DownloadObjectOptions{})
	if err != nil {
		return kv, fmt.Errorf("fetching key:value data from renterd for %s: %w", path, err)
	}
	data, err := io.ReadAll(res.Content)
	if err != nil {
		return kv, fmt.Errorf("reading response for key:value data from renterd for %s: %w", path, err)
	}
	if err := kv.Decode(data); err != nil {
		return kv, err
	}
	return kv, nil
}

// TODO: Query, Transaction
