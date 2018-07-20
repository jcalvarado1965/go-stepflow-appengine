package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	stepflow "github.com/jcalvarado1965/go-stepflow"
	stepflow_ae "github.com/jcalvarado1965/go-stepflow-appengine"
	"google.golang.org/appengine"
)

func main() {

	httpClientFactory := stepflow_ae.NewHTTPClientFactory()
	logger := stepflow_ae.NewAppengineLogger()
	storage := stepflow_ae.NewAppengineStorage(logger)
	flowQueue := stepflow_ae.NewAppengineQueue(logger, storage)
	executor := stepflow.NewExecutor(httpClientFactory, logger, storage, flowQueue)

	http.HandleFunc("/workflow", func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)
		if r.Method != http.MethodPost {
			logger.Errorf(ctx, "Invalid method %s", r.Method)
			http.Error(w, "Only POST supported", http.StatusMethodNotAllowed)
			return
		}

		var workflow stepflow.Dataflow
		err := json.NewDecoder(r.Body).Decode(&workflow)
		if err != nil {
			logger.Errorf(ctx, "Error decoding workflow %s", err.Error())
			msg := fmt.Sprintf("Invalid workflow specification: %s", err.Error())
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		_, errs := executor.Start(ctx, &workflow)
		if len(errs) > 0 {
			msg := "Errors starting workflow:\n"
			for _, err := range errs {
				logger.Errorf(ctx, err.Error())
				msg += err.Error() + "\n"
			}
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
	})

	appengine.Main()
}
