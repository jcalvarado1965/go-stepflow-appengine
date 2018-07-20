package stepflow_ae

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	stepflow "github.com/jcalvarado1965/go-stepflow"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/memcache"
)

type appengineStorage struct {
	Logger stepflow.Logger
}

const dataflowRunKind = "DataflowRun"
const flowKind = "Flow"
const flowSplitKind = "FlowSplit"
const dataflowRunJSON = "dataflowRunJSON"

var errNotFound = errors.New("Not found")

type flowDataType string

const (
	flowDataNil        = flowDataType("Nil")
	flowDataByteArray  = flowDataType("ByteArray")
	flowDataString     = flowDataType("String")
	flowDataRawMessage = flowDataType("RawMessage")
)

type flowDatastore struct {
	stepflow.FlowNoData
	Data     []byte
	DataType string
}

// NewAppengineStorage creates a datastore storage service
func NewAppengineStorage(logger stepflow.Logger) stepflow.Storage {
	return &appengineStorage{
		Logger: logger,
	}
}

// DataflowRun cannot be stored directly due to struct pointers, so serialize to JSON and
// store that
func (as *appengineStorage) StoreDataflowRun(ctx context.Context, run *stepflow.DataflowRun) error {
	as.Logger.Debugf(ctx, "Storing dataflow run %v", run)
	key := datastore.NewKey(ctx, dataflowRunKind, string(run.ID), 0, nil)

	buff, err := json.Marshal(run)
	if err != nil {
		return err
	}
	entity := &datastore.PropertyList{
		datastore.Property{Name: dataflowRunJSON, Value: buff, NoIndex: true},
	}

	_, err = datastore.Put(ctx, key, entity)

	return err
}

func (as *appengineStorage) RetrieveDataflowRuns(ctx context.Context, keys []stepflow.DataflowRunID) map[stepflow.DataflowRunID]*stepflow.DataflowRun {
	var dsKeys []*datastore.Key
	for _, key := range keys {
		dsKeys = append(dsKeys, datastore.NewKey(ctx, dataflowRunKind, string(key), 0, nil))
	}

	as.Logger.Debugf(ctx, "Retrieving data flow runs with IDs %v", dsKeys)

	entities := make([]datastore.PropertyList, len(dsKeys))
	runs := make(map[stepflow.DataflowRunID]*stepflow.DataflowRun)
	if err := datastore.GetMulti(ctx, dsKeys, entities); err != nil {
		as.Logger.Errorf(ctx, "Error retrieving dataflow runs: %s", err.Error())
	} else {
		for _, entity := range entities {
			// there should only be the one property dataflowRunJSON
			if len(entity) == 0 || (entity)[0].Name != dataflowRunJSON {
				as.Logger.Errorf(ctx, "Entity does not have dataflowRunJSON property")
				return runs
			}

			var run stepflow.DataflowRun
			if buff, ok := (entity)[0].Value.([]byte); ok {
				if err := json.Unmarshal(buff, &run); err != nil {
					as.Logger.Errorf(ctx, "Error unmarshalling entity %s", err.Error())
					return runs
				}
				runs[run.ID] = &run
			} else {
				as.Logger.Errorf(ctx, "Entity dataflowRunJSON property is not []byte")
				return runs
			}
		}
	}
	return runs
}

func (as *appengineStorage) DeleteDataflowRun(ctx context.Context, key stepflow.DataflowRunID) error {
	dsKey := datastore.NewKey(ctx, dataflowRunKind, string(key), 0, nil)
	err := datastore.Delete(ctx, dsKey)

	return err
}

func (as *appengineStorage) StoreFlow(ctx context.Context, flow *stepflow.Flow) error {
	as.Logger.Debugf(ctx, "Storing flow %v", flow)
	var buff []byte
	dataType := flowDataNil
	if flow.Data != nil {
		switch data := flow.Data.(type) {
		case string:
			dataType = flowDataString
			buff = []byte(data)
		case []byte:
			dataType = flowDataByteArray
			buff = data
		case json.RawMessage:
			dataType = flowDataRawMessage
			buff = []byte(data)
		default:
			err := fmt.Errorf("Unrecognized data type %v", data)
			as.Logger.Errorf(ctx, err.Error())
			return err
		}
	}

	flowDS := &flowDatastore{
		FlowNoData: flow.FlowNoData,
		Data:       buff,
		DataType:   string(dataType),
	}

	key := datastore.NewKey(ctx, flowKind, string(flow.ID), 0, nil)
	_, err := datastore.Put(ctx, key, flowDS)

	return err
}

func (as *appengineStorage) RetrieveFlows(ctx context.Context, keys []stepflow.FlowID) map[stepflow.FlowID]*stepflow.Flow {
	var dsKeys []*datastore.Key
	for key := range keys {
		dsKeys = append(dsKeys, datastore.NewKey(ctx, flowKind, string(key), 0, nil))
	}

	var flowList []stepflow.Flow
	flows := make(map[stepflow.FlowID]*stepflow.Flow)
	if err := datastore.GetMulti(ctx, dsKeys, &flowList); err != nil {
		as.Logger.Errorf(ctx, "Error retrieving flows: %s", err.Error())
	} else {
		for _, flow := range flowList {
			flows[flow.ID] = &flow
		}
	}
	return flows
}

func (as *appengineStorage) DeleteFlow(ctx context.Context, key stepflow.FlowID) error {
	dsKey := datastore.NewKey(ctx, flowKind, string(key), 0, nil)
	err := datastore.Delete(ctx, dsKey)

	return err
}

func (as *appengineStorage) StoreFlowSplit(ctx context.Context, flowSplit *stepflow.FlowSplit) error {
	as.Logger.Debugf(ctx, "Storing flow split %v", flowSplit)
	key := datastore.NewKey(ctx, flowSplitKind, string(flowSplit.ID), 0, nil)
	_, err := datastore.Put(ctx, key, flowSplit)

	return err
}

func (as *appengineStorage) RetrieveFlowSplits(ctx context.Context, keys []stepflow.FlowSplitID) map[stepflow.FlowSplitID]*stepflow.FlowSplit {
	var dsKeys []*datastore.Key
	for key := range keys {
		dsKeys = append(dsKeys, datastore.NewKey(ctx, flowSplitKind, string(key), 0, nil))
	}

	var flowSplitList []stepflow.FlowSplit
	flowSplits := make(map[stepflow.FlowSplitID]*stepflow.FlowSplit)
	if err := datastore.GetMulti(ctx, dsKeys, &flowSplitList); err != nil {
		as.Logger.Errorf(ctx, "Error retrieving flow splits: %s", err.Error())
	} else {
		for _, flowSplit := range flowSplits {
			flowSplits[flowSplit.ID] = flowSplit
		}
	}
	return flowSplits
}

func (as *appengineStorage) DeleteFlowSplit(ctx context.Context, key stepflow.FlowSplitID) error {
	dsKey := datastore.NewKey(ctx, flowSplitKind, string(key), 0, nil)
	err := datastore.Delete(ctx, dsKey)

	return err
}

func (as *appengineStorage) Increment(ctx context.Context, key string, initialValue int64, increment int64) int64 {
	newVal, _ := memcache.Increment(ctx, key, increment, uint64(initialValue))
	return int64(newVal)
}

func (as *appengineStorage) IncrementWithError(ctx context.Context, key string, increment int64, errIncrement int64) (count int64, errCount int64) {
	const errUnit int64 = 1 << 32
	const lowMask int64 = errUnit - 1
	totalIncr := increment + errUnit*errIncrement
	incremented := as.Increment(ctx, key, totalIncr, totalIncr)
	return incremented & lowMask, incremented / errUnit
}
