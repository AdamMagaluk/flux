// This source gets input from a socket connection and produces tables given a decoder.
// This is a good candidate for streaming use cases. For now, it produces a single table for everything
// that it receives from the start to the end of the connection.
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
		return nil, fmt.Errorf("pubsub missing from dependancy", s)
	}

	ch := ps.Sub(spec.Stream)
	return NewZettaSource(spec, ch, ps, dsid)
}

/*
{
	timestmap: 1231231,
	device:
}
*/

func NewZettaSource(spec *FromZettaProcedureSpec, rc chan interface{}, ps *pubsub.PubSub, dsid execute.DatasetID) (execute.Source, error) {
	decoder := NewResultDecoder(&rc, ps, &ResultDecoderConfig{})

	return &zettaSource{
		d:       dsid,
		rc:      rc,
		ps:      ps,
		decoder: decoder,
	}, nil
}

type zettaSource struct {
	d       execute.DatasetID
	rc      chan interface{}
	ps      *pubsub.PubSub
	decoder *ResultDecoder
	ts      []execute.Transformation
}

func (ss *zettaSource) AddTransformation(t execute.Transformation) {
	ss.ts = append(ss.ts, t)
}

func (ss *zettaSource) Run(ctx context.Context) {
	fmt.Println("zettaSource.Run")

	result, err := ss.decoder.Decode()
	if err != nil {
		err = errors.Wrap(err, "decode error")
	} else {
		fmt.Println("zettaSource.Process")
		err = result.Tables().Do(func(tbl flux.Table) error {
			fmt.Println("zettaSource.Process Callback")
			for _, t := range ss.ts {
				if err := t.Process(ss.d, tbl); err != nil {
					return err
				}
			}
			return nil
		})
	}

	fmt.Println("zettaSource.Finish")
	for _, t := range ss.ts {
		t.Finish(ss.d, err)
	}
}

func InjectFromDependencies(depsMap execute.Dependencies, deps *pubsub.PubSub) error {
	depsMap[FromZettaKind] = deps
	return nil
}
