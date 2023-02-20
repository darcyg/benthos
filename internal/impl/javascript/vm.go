package javascript

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

func getVM(msgpart *message.Part, requireRegistry *require.Registry, ctx *context.Context, mgr *bundle.NewManagement, caches []string, j *javascriptOutput, logger *log.Modular) *goja.Runtime {
	vm := goja.New()

	requireRegistry.Enable(vm)
	console.Enable(vm)

	// Set functions on javascript VM

	// fetch
	//
	// Usage:
	// fetch(url : string, httpHeaders : string[optional], method : string[optional], payload : string[optional])
	//
	// Parameters:
	// - url[string]: The URL to use.
	// - httpHeaders[string]: HTTP headers that you want to set on the HTTP request. Notation is as follows: "Accept: application/json" will set the "Accept" header to value "application/json". If you'd like to set multiple HTTP headers, separate them by "\n", e.g. "Accept: application/json\nX-Foo: Bar". If you don't want to set headers, set an empty string (""). Default: ""
	// - method[string]: The method to use, e. g. "GET", "POST", "PUT", ... Default: "GET"
	// - payload[string]: The payload to use, e. g. '{"foo": "bar}'. If you don't want to send a payload, use an empty string (""). Default: ""
	//
	// Full example with all optional fields:
	// fetch("https://test.api.com", "Accept: application/json", "POST", '{"foo": "bar"}')
	//
	// Return value:
	// Map (object) with fields:
	// status[int]: The status code returned by the HTTP call.
	// body[string]: The body returned by the HTTP call.
	setFunction(vm, "fetch", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			url         string
			httpHeaders = ""
			method      = "GET"
			payload     = ""
		)
		err := parseArgs(call, &url, &httpHeaders, &method, &payload)
		if err != nil {
			return nil, err
		}

		var payloadReader io.Reader
		if payload != "" {
			payloadReader = strings.NewReader(payload)
		}

		req, err := http.NewRequest(method, url, payloadReader)
		if err != nil {
			return nil, err
		}

		// Parse HTTP headers
		// prepare for textproto.ReadMIMEHeaders() by making sure it ends with a blank line
		normalizedHeaders := strings.TrimSpace(httpHeaders) + "\n\n"
		tpr := textproto.NewReader(bufio.NewReader(strings.NewReader(normalizedHeaders)))
		mimeHeader, err := tpr.ReadMIMEHeader()
		if err != nil {
			return nil, err
		}
		req.Header = http.Header(mimeHeader)

		// Do request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return map[string]interface{}{"status": resp.StatusCode, "body": string(respBody)}, nil
	})

	// getMeta
	//
	// Usage:
	// getMeta(key : string)
	//
	// Parameters:
	// - key[string]: The key to access meta with.
	//
	// Return value:
	// String value associated with the key.
	setFunction(vm, "getMeta", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			name string
		)
		err := parseArgs(call, &name)
		if err != nil {
			return nil, err
		}
		//result, ok := msgpart.MetaGet(name)
		result, ok := msgpart.MetaGetMut(name)
		if ok {
			result = query.IToString(result)
		}
		if !ok {
			return nil, errors.New("not found")
		}

		return result, nil
	})

	// setMeta
	//
	// Usage:
	// setMeta(key : string, value : string)
	//
	// Parameters:
	// - key[string]: The key to set in meta.
	// - value[string]: The value to set in meta.
	setFunction(vm, "setMeta", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			name, value string
		)
		err := parseArgs(call, &name, &value)
		if err != nil {
			return "", err
		}
		//msgpart.MetaSet(name, value) // TODO: Does this mutate the metadata correctly?
		if value == "" {
			msgpart.MetaDelete(name)
		} else {
			msgpart.MetaSetMut(name, value)
		}
		return nil, nil
	})

	// setRoot
	//
	// Usage:
	// setRoot(value : any)
	//
	// Parameters:
	// - value[any]: The value that will be used to replace root.
	setFunction(vm, "setRoot", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			value interface{}
		)
		err := parseArgs(call, &value)
		if err != nil {
			return nil, err
		}
		msgpart.SetStructured(value)
		return nil, nil
	})

	// getRoot
	//
	// Usage:
	// getRoot()
	//
	// Return value:
	// The root data. It is safe to mutate the contents of the returned value. To mutate the message root, use `setRoot`.
	setFunction(vm, "getRoot", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		return msgpart.AsStructuredMut()
	})

	// getCacheRes
	//
	// Usage:
	// getCacheRes(res: string, key : string)
	//
	// Parameters:
	// - res[string]: The res is cache res name.
	// - key[string]: The key to access cache with.
	//
	// Return value:
	// String value associated with the key.
	setFunction(vm, "getCacheRes", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var key string
		var cacheName string
		err := parseArgs(call, &cacheName, &key)
		if err != nil {
			return nil, err
		}
		var _result []byte
		isOK := false
		for _, _cache := range caches {
			if _cache == cacheName {
				isOK = true
			}
		}
		if !isOK {
			return nil, errors.New("not cache res")
		}

		if cerr := (*mgr).AccessCache(context.Background(), cacheName, func(cache cache.V1) {
			_result, err = cache.Get(*ctx, key)
		}); cerr != nil {
			err = cerr
		}

		/*if err != nil {
			if err != component.ErrKeyAlreadyExists {
				(*mgr).Logger().Debugf("Operator failed for key '%s': %v\n", key, err)
			} else {
				(*mgr).Logger().Debugf("Key already exists: %v\n", key)
			}
			//processor.MarkErr(part, spans[index], err)
			//return nil
		}
		*/
		var result string
		if err == nil {
			result = string(_result)
		} else {
			return nil, errors.New("not found")
		}

		return result, nil
	})

	// setCacheRes
	//
	// Usage:
	// setCacheRes(res: string, key : string, value : string)
	//
	// Parameters:
	// - res[string]: The res is cache res name.
	// - key[string]: The key to set in cache.
	// - value[string]: The value to set in cache.
	setFunction(vm, "setCacheRes", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			cacheName, key, value string
		)
		var ttl *time.Duration
		err := parseArgs(call, &cacheName, &key, &value)
		if err != nil {
			return "", err
		}
		isOK := false
		for _, _cache := range caches {
			if _cache == cacheName {
				isOK = true
			}
		}
		if !isOK {
			return nil, errors.New("not cache res")
		}
		if cerr := (*mgr).AccessCache(context.Background(), cacheName, func(cache cache.V1) {
			err = cache.Set(*ctx, key, []byte(value), ttl)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			return nil, errors.New("not set")
		}
		return nil, nil
	})

	// getCache
	//
	// Usage:
	// getCache(key : string)
	//
	// Parameters:
	// - key[string]: The key to access cache with.
	//
	// Return value:
	// String value associated with the key.
	setFunction(vm, "getCache", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var key string
		var cacheName string
		err := parseArgs(call, &key)
		if err != nil {
			return nil, err
		}
		var _result []byte
		if len(caches) > 0 {
			cacheName = caches[0]
		} else {
			return nil, errors.New("not cache res")
		}

		if cerr := (*mgr).AccessCache(context.Background(), cacheName, func(cache cache.V1) {
			_result, err = cache.Get(*ctx, key)
		}); cerr != nil {
			err = cerr
		}

		/*if err != nil {
			if err != component.ErrKeyAlreadyExists {
				(*mgr).Logger().Debugf("Operator failed for key '%s': %v\n", key, err)
			} else {
				(*mgr).Logger().Debugf("Key already exists: %v\n", key)
			}
			//processor.MarkErr(part, spans[index], err)
			//return nil
		}
		*/
		var result string
		if err == nil {
			result = string(_result)
		} else {
			return nil, errors.New("not found")
		}

		return result, nil
	})

	// setCache
	//
	// Usage:
	// setCache(key : string, value : string)
	//
	// Parameters:
	// - key[string]: The key to set in cache.
	// - value[string]: The value to set in cache.
	setFunction(vm, "setCache", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			cacheName, key, value string
		)
		var ttl *time.Duration
		err := parseArgs(call, &key, &value)
		if err != nil {
			return "", err
		}
		if len(caches) > 0 {
			cacheName = caches[0]
		} else {
			return nil, errors.New("not cache res")
		}

		if cerr := (*mgr).AccessCache(context.Background(), cacheName, func(cache cache.V1) {
			err = cache.Set(*ctx, key, []byte(value), ttl)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			return nil, errors.New("not set")
		}
		return nil, nil
	})

	// benthos_output()
	//
	// Usage:
	// benthos_output(res : string, value : string)
	//
	// Parameters:
	// - res[string]: The res is output res name.
	// - value[string]: The value to set in cache.
	setFunction(vm, "benthos_output", logger, func(call goja.FunctionCall, rt *goja.Runtime, l *log.Modular) (interface{}, error) {
		var (
			resName, value string
		)
		if j == nil {
			return nil, errors.New("run in processor")
		}
		err := parseArgs(call, &resName, &value)
		if err != nil {
			return "", err
		}
		idx := -1
		if len(j.outputs) > 0 {
			for i := range j.outputs {
				if resName == j.outputs[i] {
					idx = i
				}
			}
		}
		if idx < 0 {
			return nil, errors.New("not output res")
		}
		select {
		case (*j).outputTSChans[idx] <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(value)}), nil):
		case <-time.After(time.Second):
			return nil, errors.New("output timeout")
		}
		/*if cerr := (*mgr).AccessCache(context.Background(), cacheName, func(cache cache.V1) {
			err = cache.Set(*ctx, key, []byte(value), ttl)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			return nil, errors.New("not set")
		}
		*/
		return nil, nil
	})

	return vm
}

func setFunction(vm *goja.Runtime, name string, logger *log.Modular, function func(goja.FunctionCall, *goja.Runtime, *log.Modular) (interface{}, error)) {
	vm.Set(name, func(call goja.FunctionCall, rt *goja.Runtime) goja.Value {
		l := logger //.With("function", name)
		fields := map[string]string{}
		fields["function"] = name
		(*l).WithFields(fields)
		result, err := function(call, rt, l)
		if err != nil {
			// TODO: Do we really want to log all errors that a function returns? E. g. `getMeta` will return an error if the key can't be found.
			(*l).Errorln(err.Error())
			return goja.Null()
		}
		return rt.ToValue(result)
	})
}

func parseArgs(call goja.FunctionCall, ptrs ...interface{}) error {
	if len(ptrs) < len(call.Arguments) {
		return fmt.Errorf("have %d arguments, but only %d pointers to parse into", len(call.Arguments), len(ptrs))
	}

	for i := 0; i < len(call.Arguments); i++ {
		arg, ptr := call.Argument(i), ptrs[i]
		var err error

		if goja.IsUndefined(arg) {
			return fmt.Errorf("argument at position %d is undefined", i)
		}

		switch p := ptr.(type) {
		case *string:
			*p = arg.String()
		case *int:
			*p = int(arg.ToInteger())
		case *int64:
			*p = arg.ToInteger()
		case *float64:
			*p = arg.ToFloat()
		case *map[string]interface{}:
			*p, err = getMapFromValue(arg)
		case *bool:
			*p = arg.ToBoolean()
		case *[]interface{}:
			*p, err = getSliceFromValue(arg)
		case *[]map[string]interface{}:
			*p, err = getMapSliceFromValue(arg)
		case *goja.Value:
			*p = arg
		case *interface{}:
			*p = arg.Export()
		default:
			return fmt.Errorf("encountered unhandled type %T while trying to parse %v into %v", arg.ExportType().String(), arg, p)
		}

		if err != nil {
			return fmt.Errorf("could not parse %v (%s) into %v (%T): %v", arg, arg.ExportType().String(), ptr, ptr, err)
		}
	}

	return nil
}
