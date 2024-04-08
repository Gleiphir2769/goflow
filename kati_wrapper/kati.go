package kati_wrapper

import (
	"time"

	"github.com/s8sg/goflow/core/sdk"
)

type Loader interface {
	Load(ctx *GlobalContext) (any, error)
}

type Updater interface {
	Update(interval time.Duration, outputCh chan any)
}

type Assembler interface {
	Assemble(ctx *GlobalContext) *ModelInfo
}

type Workflow interface {
	AddLoader(loader Loader) Workflow
	AddUpdater(updater Updater) Workflow
	AddAssembler(assembler Assembler)

	Union(others ...Workflow) Workflow

	GetDefinition() *sdk.Dag
}

type GlobalContext struct {
}

func (c GlobalContext) Bytes() []byte {
	return nil
}

func (c GlobalContext) AdContexts() []AdContext {
	return make([]AdContext, 0)
}

type AdContext struct {
}

type ModelInfo struct {
	Name string
	Data any
}

type WorkflowV1 struct {
}

func (w WorkflowV1) GetDefinition() *sdk.Dag {
	//TODO implement me
	panic("implement me")
}

func (w WorkflowV1) AddLoader(loader Loader) Workflow {
	//TODO implement me
	panic("implement me")
}

func (w WorkflowV1) AddUpdater(updater Updater) Workflow {
	//TODO implement me
	panic("implement me")
}

func (w WorkflowV1) AddAssembler(assembler Assembler) {
	//TODO implement me
	panic("implement me")
}

func (w WorkflowV1) Union(others ...Workflow) Workflow {
	//TODO implement me
	panic("implement me")
}
