package runtime

import (
	mem_datastore "github.com/s8sg/goflow/core/mem-datastore"
	"github.com/s8sg/goflow/core/sdk"
)

func initDataStore() (dataStore sdk.DataStore, err error) {
	dataStore, err = mem_datastore.GetMemDataStore()
	return dataStore, err
}
