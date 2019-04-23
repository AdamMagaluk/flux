package zetta

import (
	"context"
	"fmt"
	"time"

	"github.com/cskr/pubsub"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/pkg/errors"
)

/*

// Create new streams?

import "zetta"

zetta.from(stream: "thermostat/**\/temperature")
	|> range(start:-4h, stop:-2h)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
	|> sum()`
	|> yield("")
*/

const FromZettaKind = "fromZetta"

/*
{
  "type": "event",
  "topic": "Detroit/arm/a5cb1a72-c3e7-47b6-818d-6d81b16e9ed4/state",
  "subscriptionId": 2,
  "timestamp": 1442944840135,
  "data": "moving-claw"
}
*/
type ZettaMessage struct {
	Type      string    `json:"type"`
	Topic     string    `json:"topic"`
	Timestamp time.Time `json:"timestamp"`
	Device    string
	Humidity  float64 `json:"hum"`
}

type FromZettaOpSpec struct {
	Stream string `json:"stream"`
}

func init() {
	fromZettaSignature := semantic.FunctionPolySignature{
		Parameters: map[string]semantic.PolyType{
			"stream": semantic.String,
		},
		Required: semantic.LabelSet{"stream"},
		Return:   flux.TableObjectType,
	}

	flux.RegisterPackageValue("zetta", "from", flux.FunctionValue(FromZettaKind, createFromZettaOpSpec, fromZettaSignature))
	flux.RegisterOpSpec(FromZettaKind, newFromZettaOp)
	plan.RegisterProcedureSpec(FromZettaKind, newFromZettaProcedure, FromZettaKind)
	execute.RegisterSource(FromZettaKind, createFromZettaSource)
}

func contains(ss []string, s string) bool {
	for _, st := range ss {
		if st == s {
			return true
		}
	}
	return false
}

func createFromZettaOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	spec := new(FromZettaOpSpec)

	if stream, err := args.GetRequiredString("stream"); err != nil {
		return nil, err
	} else {
		spec.Stream = stream
	}

	return spec, nil
}

func newFromZettaOp() flux.OperationSpec {
	return new(FromZettaOpSpec)
}

func (s *FromZettaOpSpec) Kind() flux.OperationKind {
	return FromZettaKind
}

type FromZettaProcedureSpec struct {
	plan.DefaultCost
	Stream string
}

func newFromZettaProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*FromZettaOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &FromZettaProcedureSpec{
		Stream: spec.Stream,
	}, nil
}

func (s *FromZettaProcedureSpec) Kind() plan.ProcedureKind {
	return FromZettaKind
}

func (s *FromZettaProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(FromZettaProcedureSpec)
	ns.Stream = s.Stream
	return ns
}

func createFromZettaSource(s plan.ProcedureSpec, dsid execute.DatasetID, a execute.Administration) (execute.Source, error) {
	spec, ok := s.(*FromZettaProcedureSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", s)
	}

	ps, ok := a.Dependencies()[FromZettaKind].(*pubsub.PubSub)
	if !ok {
		return nil, fmt.Errorf("pubsub missing from dependancy %v", s)
	}

	ch := ps.Sub(spec.Stream)
	sub := sourceSubscription{
		stream: spec.Stream,
		reader: ch,
		ps:     ps,
	}
	return NewZettaSource(spec, sub, dsid)
}

type sourceSubscription struct {
	stream string
	reader chan interface{}
	ps     *pubsub.PubSub
}

func (s *sourceSubscription) close() {
	s.ps.Unsub(s.reader, s.stream)
	close(s.reader)
}

/*
{
	timestmap: 1231231,
	device:
}
*/

func NewZettaSource(spec *FromZettaProcedureSpec, sub sourceSubscription, dsid execute.DatasetID) (execute.Source, error) {
	decoder := NewMultiResultDecoder(&sub, &ResultDecoderConfig{})

	return &zettaSource{
		d:       dsid,
		decoder: decoder,
	}, nil
}

type zettaSource struct {
	d       execute.DatasetID
	decoder *MultiResultDecoder
	ts      []execute.Transformation
}

func (ss *zettaSource) AddTransformation(t execute.Transformation) {
	ss.ts = append(ss.ts, t)
}

// Method not using the MultiResultDecoder
// func (ss *zettaSource) Run(ctx context.Context) {
// 	fmt.Println("zettaSource.Run")

// 	result, err := ss.decoder.Decode()
// 	if err != nil {
// 		err = errors.Wrap(err, "decode error")
// 	} else {
// 		fmt.Println("zettaSource.Process")
// 		err = result.Tables().Do(func(tbl flux.Table) error {
// 			fmt.Println("zettaSource.Process Callback")
// 			for _, t := range ss.ts {
// 				if err := t.Process(ss.d, tbl); err != nil {
// 					return err
// 				}
// 			}
// 			return nil
// 		})
// 	}

// 	fmt.Println("zettaSource.Finish")
// 	for _, t := range ss.ts {
// 		t.Finish(ss.d, err)
// 	}
// }

func (ss *zettaSource) Run(ctx context.Context) {
	// Uses the MultiResultDecoder and will continue to pull data from the pubsub
	// broker.
	results, err := ss.decoder.Decode()
	if err != nil {
		err = errors.Wrap(err, "decode error")
	} else {
		// More always returns true.
		for results.More() {
			result := results.Next()
			if err := result.Tables().Do(func(tbl flux.Table) error {
				for _, t := range ss.ts {
					if err := t.Process(ss.d, tbl); err != nil {
						err = errors.Wrap(err, "decode error")
						break
					}
				}
				return nil
			}); err != nil {
				err = errors.Wrap(err, "decode error")
				break
			}
		}
	}
	for _, t := range ss.ts {
		t.Finish(ss.d, err)
	}
}

func InjectFromDependencies(depsMap execute.Dependencies, deps *pubsub.PubSub) error {
	depsMap[FromZettaKind] = deps
	return nil
}
