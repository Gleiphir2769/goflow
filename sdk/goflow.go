package sdk

import (
	"fmt"
	runtimePkg "github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/runtime"
)

type FlowService struct {
	WorkerConcurrency int
	Flows             map[string]runtime.FlowDefinitionHandler
	OpenTraceUrl      string
	DataStore         sdk.DataStore
	Logger            sdk.Logger
	EnableMonitoring  bool
	DebugEnabled      bool

	runtime *runtime.FlowRuntime
}

type Request struct {
	Body      []byte
	RequestId string
	Query     map[string][]string
	Header    map[string][]string
}

const (
	DefaultTraceUrl          = "localhost:5775"
	DefaultWorkerConcurrency = 2
)

func (fs *FlowService) Execute(flowName string, req *Request) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided to execute flow")
	}

	fs.ConfigureDefault()

	request := &runtimePkg.Request{
		Header:    req.Header,
		RequestID: req.RequestId,
		Body:      req.Body,
		Query:     req.Query,
	}

	err := fs.runtime.Execute(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to execute request, %v", err)
	}

	return nil
}

func (fs *FlowService) Pause(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Pause(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to pause request, %v", err)
	}

	return nil
}

func (fs *FlowService) Resume(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Resume(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to resume request, %v", err)
	}

	return nil
}

func (fs *FlowService) Stop(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Stop(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to stop request, %v", err)
	}

	return nil
}

func (fs *FlowService) Register(flowName string, handler runtime.FlowDefinitionHandler) error {
	if flowName == "" {
		return fmt.Errorf("flow-name must not be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler must not be nil")
	}

	if fs.Flows == nil {
		fs.Flows = make(map[string]runtime.FlowDefinitionHandler)
	}

	if fs.Flows[flowName] != nil {
		return fmt.Errorf("flow-name must be unique for each flow")
	}

	fs.Flows[flowName] = handler

	errorChan := make(chan error)
	if err := fs.initRuntime(errorChan); err != nil {
		return err
	}
	go func() {
		err := <-errorChan
		close(errorChan)
		fs.Logger.Log("runtime has stopped, error: " + err.Error())
	}()

	err := fs.runtime.Register(map[string]runtime.FlowDefinitionHandler{flowName: handler})
	if err != nil {
		return err
	}

	return nil
}

func (fs *FlowService) Start() error {
	fs.ConfigureDefault()

	errorChan := make(chan error)
	defer close(errorChan)

	if err := fs.initRuntime(errorChan); err != nil {
		return err
	}

	return nil
}

func (fs *FlowService) ConfigureDefault() {
	if fs.OpenTraceUrl == "" {
		fs.OpenTraceUrl = DefaultTraceUrl
	}
	if fs.WorkerConcurrency == 0 {
		fs.WorkerConcurrency = DefaultWorkerConcurrency
	}
}

func (fs *FlowService) initRuntime(errorChan chan error) error {

	// runtime has already been initialized
	if fs.runtime != nil {
		return nil
	}

	fs.runtime = &runtime.FlowRuntime{
		Flows:            map[string]runtime.FlowDefinitionHandler{},
		OpenTracingUrl:   fs.OpenTraceUrl,
		DataStore:        fs.DataStore,
		Logger:           fs.Logger,
		Concurrency:      fs.WorkerConcurrency,
		EnableMonitoring: fs.EnableMonitoring,
		DebugEnabled:     fs.DebugEnabled,
	}

	if err := fs.runtime.Init(); err != nil {
		return err
	}
	go fs.runtimeWorker(errorChan)

	return nil
}

func (fs *FlowService) runtimeWorker(errorChan chan error) {
	err := fs.runtime.StartRuntime()
	errorChan <- fmt.Errorf("runtime has stopped, error: %v", err)
}
