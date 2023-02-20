package javascript

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

func init() {
	//fmt.Println("init")
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newJavaScript(conf.JavaScript, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2ToV1Processor("javascript", p, mgr), nil
	}, docs.ComponentSpec{
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
			docs.FieldString("registry_global_folders", "List of global folders that will be used to load modules from if the requested JS module is not found elsewhere. If not defined, the the working path will be used.").Array(),
		).ChildDefaultAndTypesFromStruct(processor.NewJavaScriptConfig()),
	})
	if err != nil {
		panic(err)
	}
}

// ------------------------------------------------------------------------------
type cacheOperator func(ctx context.Context, cache cache.V1, key string, value []byte, ttl *time.Duration) ([]byte, bool, error)

type javascriptProc struct {
	program         *goja.Program
	requireRegistry *require.Registry
	//logger          *service.Logger
	logger log.Modular
	mut    sync.Mutex

	mgr    bundle.NewManagement
	caches []string
}

func newJavaScript(conf processor.JavaScriptConfig, mgr bundle.NewManagement) (*javascriptProc, error) {
	if conf.Code != "" && conf.File != "" {
		return nil, errors.New("both 'code' and 'file' fields are specified but only one is allowed")
	}
	if conf.Code == "" && conf.File == "" {
		return nil, errors.New("neither 'code' nor 'file' fields are specified but one of them is required")
	}
	cacheRes := conf.CacheRes

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
	return &javascriptProc{program: program, requireRegistry: requireRegistry, logger: logger, mgr: mgr, caches: cacheRes}, nil
}

func (j *javascriptProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	j.mut.Lock()
	defer j.mut.Unlock()
	//fmt.Println("Process")
	// Create new JS VM
	vm := getVM(msg, j.requireRegistry, &ctx, &j.mgr, j.caches, nil, &j.logger)

	// Run JS file
	_, err := vm.RunProgram(j.program)
	if err != nil {
		return nil, err
	}
	//return []*service.Message{m}, nil
	return []*message.Part{msg}, nil
}

func (j *javascriptProc) Close(ctx context.Context) error {
	return nil
}
