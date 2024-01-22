package main

import (
	"fmt"
	"github.com/s8sg/goflow/samples/single"
	"github.com/s8sg/goflow/sdk"
)

func main() {
	fs := &sdk.FlowService{
		OpenTraceUrl:      "localhost:5775",
		WorkerConcurrency: 5,
		EnableMonitoring:  true,
		DebugEnabled:      true,
	}
	fs.Register("single", single.DefineWorkflow)
	//fs.Register("serial", serial.DefineWorkflow)
	//fs.Register("parallel", parallel.DefineWorkflow)
	//fs.Register("condition", condition.DefineWorkflow)
	//fs.Register("loop", loop.DefineWorkflow)
	//fs.Register("myflow", myflow.DefineWorkflow)
	fmt.Println(fs.Start())

	err := fs.Execute("single", &sdk.Request{Body: []byte("Hello World")})
	if err != nil {
		panic(err)
	}
	exitChan := make(chan struct{})
	<-exitChan
}
