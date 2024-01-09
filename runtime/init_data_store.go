package runtime

import (
	mem_datastore "github.com/s8sg/goflow/core/mem-datastore"
	"github.com/s8sg/goflow/core/sdk"
)

func initDataStore(redisURI string, password string) (dataStore sdk.DataStore, err error) {
	//dataStore, err = redisDataStore.GetRedisDataStore(redisURI, password)
	dataStore, err = mem_datastore.GetMemDataStore()
	return dataStore, err
}
