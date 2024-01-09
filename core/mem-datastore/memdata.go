package mem_datastore

import (
	"errors"
	"fmt"

	"github.com/alphadose/haxmap"
	"github.com/s8sg/goflow/core/sdk"
)

type MemDataStore struct {
	bucketName string
	store      *haxmap.Map[string, []byte]
}

func (mds *MemDataStore) CopyStore() (sdk.DataStore, error) {
	return &MemDataStore{bucketName: mds.bucketName, store: mds.store}, nil
}

func GetMemDataStore() (sdk.DataStore, error) {
	ds := &MemDataStore{store: haxmap.New[string, []byte]()}
	return ds, nil
}

func (mds *MemDataStore) Configure(flowName string, requestId string) {
	bucketName := fmt.Sprintf("core-%s-%s", flowName, requestId)

	mds.bucketName = bucketName
}

func (mds *MemDataStore) Init() error {
	return nil
}

func (mds *MemDataStore) Set(key string, value []byte) error {
	mds.store.Set(key, value)
	return nil
}

func (mds *MemDataStore) Get(key string) ([]byte, error) {
	if v, ok := mds.store.Get(key); ok {
		return v, nil
	}
	return nil, errors.New(fmt.Sprintf("error reading: %v, data is nil", key))
}

func (mds *MemDataStore) Del(key string) error {
	mds.store.Del(key)
	return nil
}

func (mds *MemDataStore) Cleanup() error {
	mds.store.ForEach(func(key string, bytes []byte) bool {
		mds.store.Del(key)
		return true
	})
	return nil
}
