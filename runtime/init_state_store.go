package runtime

import (
	mem_statestore "github.com/s8sg/goflow/core/mem-statestore"
	"github.com/s8sg/goflow/core/sdk"
)

func initStateStore() (stateStore sdk.StateStore, err error) {
	stateStore, err = mem_statestore.GetMemDataStore()
	return stateStore, err
}
