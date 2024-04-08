package kati_wrapper

import (
	"errors"
	"fmt"

	"github.com/s8sg/goflow/core/sdk"
	flow "github.com/s8sg/goflow/flow/v1"
	"github.com/s8sg/goflow/runtime"
	v1 "github.com/s8sg/goflow/v1"
)

type FlowEngine struct {
	runtime *v1.FlowService

	subFlows map[string]*sdk.Dag
}

func NewFlowEngine() *FlowEngine {
	return &FlowEngine{runtime: &v1.FlowService{
		Port:              8080,
		WorkerConcurrency: 5,
		EnableMonitoring:  true,
		DebugEnabled:      true,
	}}
}

func (e *FlowEngine) Register(flowName string, workflow Workflow) error {
	dag := workflow.GetDefinition()
	if _, ok := e.subFlows[flowName]; ok {
		return fmt.Errorf("flow name %s has been registered", flowName)
	}
	e.subFlows[flowName] = dag
	return nil
}

func (e *FlowEngine) Validate() error {
	// do validate work...
	return nil
}

func (e *FlowEngine) Start() error {
	entireFlow := e.mergeDags()
	// merge all sub-dags
	errReg := e.runtime.Register("kati_builder", entireFlow)
	errVal := e.Validate()
	errStart := e.runtime.Start()
	return errors.Join(errReg, errVal, errStart)
}

func (e *FlowEngine) Execute(ctx GlobalContext) error {
	return e.runtime.Execute("kati_builder", &v1.Request{Body: ctx.Bytes()})
}

func (e *FlowEngine) mergeDags() runtime.FlowDefinitionHandler {
	// do merge...
	return func(workflow *flow.Workflow, context *flow.Context) error {
		return nil
	}
}
