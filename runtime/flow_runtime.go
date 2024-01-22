package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/jasonlvhit/gocron"
	"github.com/rs/xid"
	"github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/runtime/controller"
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/core/sdk/executor"
	"github.com/s8sg/goflow/core/sdk/exporter"
	"github.com/s8sg/goflow/eventhandler"
	log2 "github.com/s8sg/goflow/log"
	"log"
)

type FlowRuntime struct {
	Flows            map[string]FlowDefinitionHandler
	OpenTracingUrl   string
	stateStore       sdk.StateStore
	DataStore        sdk.DataStore
	Logger           sdk.Logger
	Concurrency      int
	EnableMonitoring bool
	DebugEnabled     bool

	eventHandler sdk.EventHandler

	taskQueues map[string]chan []byte
}

type Task struct {
	FlowName    string              `json:"flow_name"`
	RequestID   string              `json:"request_id"`
	Body        string              `json:"body"`
	Header      map[string][]string `json:"header"`
	RawQuery    string              `json:"raw_query"`
	Query       map[string][]string `json:"query"`
	RequestType string              `json:"request_type"`
}

const (
	InternalRequestQueueInitial = "goflow-internal-request"

	GoFlowRegisterInterval = 4

	PartialRequest = "PARTIAL"
	NewRequest     = "NEW"
	PauseRequest   = "PAUSE"
	ResumeRequest  = "RESUME"
	StopRequest    = "STOP"
)

func (fRuntime *FlowRuntime) Init() error {
	var err error

	fRuntime.stateStore, err = initStateStore()
	if err != nil {
		return fmt.Errorf("failed to initialize the StateStore, %v", err)
	}

	if fRuntime.DataStore == nil {
		fRuntime.DataStore, err = initDataStore()
		if err != nil {
			return fmt.Errorf("failed to initialize the StateStore, %v", err)
		}
	}

	if fRuntime.Logger == nil {
		fRuntime.Logger = &log2.StdErrLogger{}
	}

	fRuntime.eventHandler = &eventhandler.GoFlowEventHandler{
		TraceURI: fRuntime.OpenTracingUrl,
	}

	return nil
}

func (fRuntime *FlowRuntime) CreateExecutor(req *runtime.Request) (executor.Executor, error) {
	flowHandler, ok := fRuntime.Flows[req.FlowName]
	if !ok {
		return nil, fmt.Errorf("could not find handler for flow %s", req.FlowName)
	}
	ex := &FlowExecutor{
		StateStore:       fRuntime.stateStore,
		DataStore:        fRuntime.DataStore,
		EventHandler:     fRuntime.eventHandler,
		EnableMonitoring: fRuntime.EnableMonitoring,
		Handler:          flowHandler,
		Logger:           fRuntime.Logger,
		Runtime:          fRuntime,
		IsLoggingEnabled: fRuntime.DebugEnabled,
	}
	err := ex.Init(req)
	return ex, err
}

// Register flows to the runtime
// If the flow is already registered, it returns an error
func (fRuntime *FlowRuntime) Register(flows map[string]FlowDefinitionHandler) error {
	if len(flows) == 0 {
		return nil
	}

	var flowNames []string
	for flowName := range flows {
		if _, ok := fRuntime.Flows[flowName]; ok {
			return fmt.Errorf("flow %s already registered", flowName)
		}

		flowNames = append(flowNames, flowName)
	}

	// register flows to runtime
	for flowName, flowHandler := range flows {
		fRuntime.Flows[flowName] = flowHandler
	}

	// initialize task queues when in worker mode
	err := fRuntime.initializeTaskQueues(flows)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("failed to initialize task queues for flows %v, error %v", flowNames, err))
	}

	fRuntime.Logger.Log(fmt.Sprintf("[goflow] queue workers for flows %v started successfully", flowNames))

	return nil
}

func (fRuntime *FlowRuntime) Execute(flowName string, request *runtime.Request) error {
	data, _ := json.Marshal(&Task{
		FlowName:    flowName,
		RequestID:   request.RequestID,
		Body:        string(request.Body),
		Header:      request.Header,
		RawQuery:    request.RawQuery,
		Query:       request.Query,
		RequestType: NewRequest,
	})
	fRuntime.taskQueues[flowName] <- data
	return nil
}

func (fRuntime *FlowRuntime) Pause(flowName string, request *runtime.Request) error {
	data, _ := json.Marshal(&Task{
		FlowName:    flowName,
		RequestID:   request.RequestID,
		Body:        string(request.Body),
		Header:      request.Header,
		RawQuery:    request.RawQuery,
		Query:       request.Query,
		RequestType: PauseRequest,
	})
	fRuntime.taskQueues[flowName] <- data
	return nil
}

func (fRuntime *FlowRuntime) Stop(flowName string, request *runtime.Request) error {
	data, _ := json.Marshal(&Task{
		FlowName:    flowName,
		RequestID:   request.RequestID,
		Body:        string(request.Body),
		Header:      request.Header,
		RawQuery:    request.RawQuery,
		Query:       request.Query,
		RequestType: StopRequest,
	})
	fRuntime.taskQueues[flowName] <- data
	return nil
}

func (fRuntime *FlowRuntime) Resume(flowName string, request *runtime.Request) error {
	data, _ := json.Marshal(&Task{
		FlowName:    flowName,
		RequestID:   request.RequestID,
		Body:        string(request.Body),
		Header:      request.Header,
		RawQuery:    request.RawQuery,
		Query:       request.Query,
		RequestType: ResumeRequest,
	})
	fRuntime.taskQueues[flowName] <- data
	return nil
}

// StartRuntime starts the runtime
func (fRuntime *FlowRuntime) StartRuntime() error {
	registerDetails := func() error {
		// Get the flow details for each flow
		flowDetails := make(map[string]string)
		for flowID, defHandler := range fRuntime.Flows {
			dag, err := getFlowDefinition(defHandler)
			if err != nil {
				return fmt.Errorf("failed to start runtime, dag export failed, error %v", err)
			}
			flowDetails[flowID] = dag
		}

		return nil
	}

	err := registerDetails()
	if err != nil {
		log.Printf("failed to register details, %v", err)
		return err
	}

	err = gocron.Every(GoFlowRegisterInterval).Second().Do(func() {
		err := registerDetails()
		if err != nil {
			log.Printf("failed to register details, %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("failed to start runtime, %v", err)
	}

	<-gocron.Start()

	return fmt.Errorf("[goflow] runtime stopped")
}

func (fRuntime *FlowRuntime) EnqueuePartialRequest(pr *runtime.Request) error {
	data, _ := json.Marshal(&Task{
		FlowName:    pr.FlowName,
		RequestID:   pr.RequestID,
		Body:        string(pr.Body),
		Header:      pr.Header,
		RawQuery:    pr.RawQuery,
		Query:       pr.Query,
		RequestType: PartialRequest,
	})
	fRuntime.taskQueues[pr.FlowName] <- data
	return nil
}

// Consume messages from queue
func (fRuntime *FlowRuntime) Consume(payload []byte) {
	var task Task
	if err := json.Unmarshal(payload, &task); err != nil {
		fRuntime.Logger.Log("[goflow] rejecting task for parse failure, error " + err.Error())
		return
	}
	if err := fRuntime.handleRequest(makeRequestFromTask(task), task.RequestType); err != nil {
		fRuntime.Logger.Log("[goflow] rejecting task for failure, error " + err.Error())
	}
}

func (fRuntime *FlowRuntime) handleRequest(request *runtime.Request, requestType string) error {
	var err error
	switch requestType {
	case PartialRequest:
		err = fRuntime.handlePartialRequest(request)
	case NewRequest:
		err = fRuntime.handleNewRequest(request)
	case PauseRequest:
		err = fRuntime.handlePauseRequest(request)
	case ResumeRequest:
		err = fRuntime.handleResumeRequest(request)
	case StopRequest:
		err = fRuntime.handleStopRequest(request)
	default:
		return fmt.Errorf("invalid request %v received with type %s", request, requestType)
	}
	return err
}

func (fRuntime *FlowRuntime) handleNewRequest(request *runtime.Request) error {
	flowExecutor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		return fmt.Errorf("failed to execute request " + request.RequestID + ", error: " + err.Error())
	}

	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = controller.ExecuteFlowHandler(response, request, flowExecutor)
	if err != nil {
		return fmt.Errorf("request failed to be processed. error: " + err.Error())
	}

	return nil
}

func (fRuntime *FlowRuntime) handlePartialRequest(request *runtime.Request) error {
	flowExecutor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to execute request, error: %v", request.RequestID, err))
		return fmt.Errorf("[goflow] failed to execute request " + request.RequestID + ", error: " + err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = controller.PartialExecuteFlowHandler(response, request, flowExecutor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be processed. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("[goflow] request failed to be processed. error: " + err.Error())
	}
	return nil
}

func (fRuntime *FlowRuntime) handlePauseRequest(request *runtime.Request) error {
	flowExecutor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be paused. error: %v", request.RequestID, err))
		return fmt.Errorf("request %s failed to be paused. error: %v", request.RequestID, err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	err = controller.PauseFlowHandler(response, request, flowExecutor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be paused. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request %s failed to be paused. error: %v", request.RequestID, err.Error())
	}
	return nil
}

func (fRuntime *FlowRuntime) handleResumeRequest(request *runtime.Request) error {
	flowExecutor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be resumed. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request %s failed to be resumed. error: %v", request.RequestID, err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	err = controller.ResumeFlowHandler(response, request, flowExecutor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be resumed. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request %s failed to be resumed. error: %v", request.RequestID, err.Error())
	}
	return nil
}

func (fRuntime *FlowRuntime) handleStopRequest(request *runtime.Request) error {
	flowExecutor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be stopped. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request %s failed to be stopped. error: %v", request.RequestID, err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	err = controller.StopFlowHandler(response, request, flowExecutor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[request `%s`] failed to be stopped. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request %s failed to be stopped. error: %v", request.RequestID, err.Error())
	}
	return nil
}

func (fRuntime *FlowRuntime) initializeTaskQueues(flows map[string]FlowDefinitionHandler) error {

	if fRuntime.taskQueues == nil {
		fRuntime.taskQueues = make(map[string]chan []byte)
	}

	for flowName := range flows {
		fRuntime.taskQueues[flowName] = make(chan []byte, 1000)
	}

	for i := 0; i < fRuntime.Concurrency; i++ {
		for flowName, ch := range fRuntime.taskQueues {
			go func() {
				for data := range ch {
					fRuntime.Consume(data)
				}
			}()
			fRuntime.Logger.Log(fmt.Sprintf("added consumer for flow %s", flowName))
		}
	}

	return nil
}

func (fRuntime *FlowRuntime) cleanTaskQueues() error {
	fRuntime.taskQueues = map[string]chan []byte{}

	return nil
}

func (fRuntime *FlowRuntime) internalRequestQueueId(flowName string) string {
	return fmt.Sprintf("%s:%s", InternalRequestQueueInitial, flowName)
}

func (fRuntime *FlowRuntime) requestQueueId(flowName string) string {
	return flowName
}

func makeRequestFromTask(task Task) *runtime.Request {
	request := &runtime.Request{
		FlowName:  task.FlowName,
		RequestID: task.RequestID,
		Body:      []byte(task.Body),
		Header:    task.Header,
		RawQuery:  task.RawQuery,
		Query:     task.Query,
	}
	return request
}

func getFlowDefinition(handler FlowDefinitionHandler) (string, error) {
	ex := &FlowExecutor{
		Handler: handler,
	}
	flowExporter := exporter.CreateFlowExporter(ex)
	resp, err := flowExporter.Export()
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func getNewId() string {
	guid := xid.New()
	return guid.String()
}
