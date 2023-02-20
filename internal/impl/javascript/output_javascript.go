package javascript

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

func init() {
	//fmt.Println("init")
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
		p, err := newJavaScriptWriter(conf.JavaScript, mgr)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("javascript", 1, p, mgr)
		if err != nil {
			return nil, err
		}
		return w, nil
	}), docs.ComponentSpec{
		Name: "javascript",
		Categories: []string{
			"Mapping",
		},
		Summary:     "Executes the provided JavaScript code using the github.com/dop251/goja library. The `console` and `require` packages from https://github.com/dop251/goja_nodejs are also implementd.",
		Description: "",
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInterpolatedString("code", "The javascript code to use.").HasDefault(""),
			docs.FieldInterpolatedString("file", "The javascript file to use.").HasDefault(""),
			docs.FieldString("cache_res", "List of The [`cache` resource].").Array(),
			docs.FieldString("output_res", "List of The [`output` resource].").Array(),
			docs.FieldString("registry_global_folders", "List of global folders that will be used to load modules from if the requested JS module is not found elsewhere. If not defined, the the working path will be used.").Array(),
		).ChildDefaultAndTypesFromStruct(output.NewJavaScriptConfig()),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type javascriptOutput struct {
	program         *goja.Program
	requireRegistry *require.Registry
	//logger          *service.Logger
	mgr     bundle.NewManagement
	caches  []string
	outputs []string
	logger  log.Modular
	mut     sync.Mutex

	transactions  <-chan message.Transaction
	outputTSChans []chan message.Transaction
	outputStreams []output.Streamed
	shutSig       *shutdown.Signaller
}

func newJavaScriptWriter(conf output.JavaScriptConfig, mgr bundle.NewManagement) (*javascriptOutput, error) {
	if conf.Code != "" && conf.File != "" {
		return nil, errors.New("both 'code' and 'file' fields are specified but only one is allowed")
	}
	if conf.Code == "" && conf.File == "" {
		return nil, errors.New("neither 'code' nor 'file' fields are specified but one of them is required")
	}
	cacheRes := conf.CacheRes
	outputRes := conf.OutputRes
	outputConfs := make([]output.Config, len(outputRes))
	outputStreams := make([]output.Streamed, len(outputRes))

	for i := range outputConfs {
		conf := output.NewConfig()
		conf.Type = "resource"
		conf.Resource = outputRes[i]
		outputConfs[i] = conf
	}
	for i := range outputStreams {
		_outStream, err := mgr.NewOutput(outputConfs[i])
		if err != nil {
			return nil, err
		}
		outputStreams[i] = _outStream
	}
	outputTSChans := make([]chan message.Transaction, len(outputRes))
	for i := range outputTSChans {
		outputTSChans[i] = make(chan message.Transaction)
		if err := outputStreams[i].Consume(outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	filename := "main.js"
	var err error
	if conf.File != "" {
		// Open file and read code
		codeBytes, err := os.ReadFile(conf.File)
		if err != nil {
			return nil, fmt.Errorf("failed to open the file specifed in 'file' field: %v", err)
		}
		filename = conf.File
		conf.Code = string(codeBytes)
	}
	var program *goja.Program
	program, err = goja.Compile(filename, conf.Code, false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile javascript code: %v", err)
	}
	logger := mgr.Logger()
	requireRegistry := require.NewRegistry(require.WithGlobalFolders(conf.GlobalFolders...))
	requireRegistry.RegisterNativeModule("console", console.RequireWithPrinter(&VMLogger{logger}))
	return &javascriptOutput{program: program, requireRegistry: requireRegistry, logger: logger, mgr: mgr, caches: cacheRes, outputs: outputRes, outputTSChans: outputTSChans, outputStreams: outputStreams, transactions: nil, shutSig: shutdown.NewSignaller()}, nil
}

func (j *javascriptOutput) Consume(transactions <-chan message.Transaction) error {
	if j.transactions != nil {
		return component.ErrAlreadyStarted
	}
	j.transactions = transactions
	go j.loop()
	return nil
}

func (j *javascriptOutput) Connect(ctx context.Context) error {
	return nil
	//fmt.Println("Connect")
	/*for _, out := range j.outputStreams {
		if !out.Connected() {
			return errors.New("output not connected")
		}
	}
	return nil
	*/
}

func (j *javascriptOutput) loop() {
	ackInterruptChan := make(chan struct{})
	var ackPending int64

	defer func() {
		// Wait for pending acks to be resolved, or forceful termination
	ackWaitLoop:
		for atomic.LoadInt64(&ackPending) > 0 {
			select {
			case <-ackInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-j.shutSig.CloseNowChan():
				break ackWaitLoop
			}
		}
		for _, c := range j.outputTSChans {
			close(c)
		}
		//_ = closeAllOutputs(context.Background(), j.outputStreams)
		for _, o := range j.outputStreams {
			o.TriggerCloseNow()
		}
		for _, o := range j.outputStreams {
			if err := o.WaitForClose(context.Background()); err != nil {
			}
		}
		j.shutSig.ShutdownComplete()
	}()

	for !j.shutSig.ShouldCloseNow() {
		//var ts message.Transaction
		var open bool
		select {
		case _, open = <-j.transactions:
			if !open {
				return
			}
		case <-j.shutSig.CloseNowChan():
			return
		}
	}
}

func (j *javascriptOutput) WriteBatch(ctx context.Context, msgb message.Batch) error {
	j.mut.Lock()
	defer j.mut.Unlock()

	//fmt.Println("WriteBatch")
	return output.IterateBatchedSend(msgb, func(i int, msg *message.Part) error {
		// Create new JS VM
		vm := getVM(msg, j.requireRegistry, &ctx, &j.mgr, j.caches, j, &j.logger)
		// Run JS file
		_, err := vm.RunProgram(j.program)
		return err
	})
}

func (j *javascriptOutput) Close(ctx context.Context) error {
	return nil
}

func (j *javascriptOutput) TriggerCloseNow() {
	j.shutSig.CloseNow()
}

func (j *javascriptOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-j.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
