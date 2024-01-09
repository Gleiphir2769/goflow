package mem_statestore

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/alphadose/haxmap"
	"github.com/s8sg/goflow/core/sdk"
)

type MemStateStore struct {
	keyPath string
	store   *haxmap.Map[string, string]
}

func (mds *MemStateStore) Set(key string, value string) error {
	mds.store.Set(key, value)
	return nil
}

func (mds *MemStateStore) Get(key string) (string, error) {
	if v, ok := mds.store.Get(key); ok {
		return v, nil
	}
	return "", errors.New(fmt.Sprintf("error reading: %v, data is nil", key))
}

func (mds *MemStateStore) Update(key string, oldValue string, newValue string) error {
	if ok := mds.store.CompareAndSwap(key, oldValue, newValue); !ok {
		err := fmt.Errorf("Old value doesn't match for key %s", key)
		return err
	}

	return nil
}

func (mds *MemStateStore) CopyStore() (sdk.StateStore, error) {
	return &MemStateStore{keyPath: mds.keyPath, store: mds.store}, nil
}

func GetMemDataStore() (sdk.StateStore, error) {
	ds := &MemStateStore{store: haxmap.New[string, string]()}
	return ds, nil
}

func (mds *MemStateStore) Configure(flowName string, requestId string) {
	keyPath := fmt.Sprintf("core-%s-%s", flowName, requestId)

	mds.keyPath = keyPath
}

func (mds *MemStateStore) Init() error {
	return nil
}

func (mds *MemStateStore) Del(key string) error {
	mds.store.Del(key)
	return nil
}

func (mds *MemStateStore) Cleanup() error {
	mds.store.ForEach(func(key string, _ string) bool {
		mds.store.Del(key)
		return true
	})
	return nil
}

func (mds *MemStateStore) Incr(key string, value int64) (int64, error) {
	key = mds.keyPath + "." + key
	oldValue, loaded := mds.store.GetOrSet(key, strconv.FormatInt(value, 10))
	if loaded {
		oldValueInt, _ := strconv.Atoi(oldValue)
		value = int64(oldValueInt) + value
		mds.store.Set(key, strconv.FormatInt(value, 10))
	}
	return value, nil
}
