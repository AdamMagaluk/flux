package zetta

import (
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
)

// ResultDecoder decodes raw input strings from a reader into a flux.Result.
// It uses a separator to split the input into tokens and generate table rows.
// Tokens are kept as they are and put into a table with schema `_time`, `_value`.
// The `_value` column contains tokens.
// The `_time` column contains the timestamps for when each `_value` has been read.
// Strings in `_value` are obtained from the io.Reader passed to the Decode function.
// ResultDecoder outputs one table once the reader reaches EOF.
type ResultDecoder struct {
	sub    *sourceSubscription
	config *ResultDecoderConfig
}

// NewResultDecoder creates a new result decoder from config.
func NewResultDecoder(sub *sourceSubscription, config *ResultDecoderConfig) *ResultDecoder {
	return &ResultDecoder{config: config, sub: sub}
}

// ResultDecoderConfig is the configuration for a result decoder.
type ResultDecoderConfig struct {
}

func (rd *ResultDecoder) Do(f func(flux.Table) error) error {
	// Hardcod col data for now.
	timeCol := flux.ColMeta{Label: "_time", Type: flux.TTime}
	valueCol := flux.ColMeta{Label: "_value", Type: flux.TFloat}
	typeCol := flux.ColMeta{Label: "type", Type: flux.TString}
	topicCol := flux.ColMeta{Label: "topic", Type: flux.TString}
	deviceCol := flux.ColMeta{Label: "device", Type: flux.TString}

	key := execute.NewGroupKey(nil, nil)
	builder := execute.NewColListTableBuilder(key, &memory.Allocator{})
	timeIdx, err := builder.AddCol(timeCol)
	if err != nil {
		return err
	}
	valueIdx, err := builder.AddCol(valueCol)
	if err != nil {
		return err
	}
	typeIdx, err := builder.AddCol(typeCol)
	if err != nil {
		return err
	}
	topicIdx, err := builder.AddCol(topicCol)
	if err != nil {
		return err
	}
	deviceIdx, err := builder.AddCol(deviceCol)
	if err != nil {
		return err
	}

	// Max batch time is 100ms.
	maxDuration := time.Millisecond * 100
	// Timer will fire after duration.
	bactchTimer := time.NewTimer(maxDuration)

	for {
		select {
		case e, ok := <-rd.sub.reader:
			if !ok {
				// Channel closed.
				fmt.Println("Channel closed")
				goto RETURN
			}
			m, ok := e.(ZettaMessage)
			if !ok {
				fmt.Println("Not ZettaMessage")
				goto RETURN
			}

			err = builder.AppendTime(timeIdx, values.ConvertTime(m.Timestamp))
			if err != nil {
				return err
			}

			err = builder.AppendFloat(valueIdx, m.Humidity)
			if err != nil {
				return err
			}

			err = builder.AppendString(typeIdx, m.Type)
			if err != nil {
				return err
			}

			err = builder.AppendString(topicIdx, m.Topic)
			if err != nil {
				return err
			}

			err = builder.AppendString(deviceIdx, m.Device)
			if err != nil {
				return err
			}
		case <-bactchTimer.C:
			goto RETURN
		}
	}

RETURN:
	tbl, err := builder.Table()
	if err != nil {
		return err
	}
	return f(tbl)
}

func (*ResultDecoder) Name() string {
	return "_result"
}

func (rd *ResultDecoder) Tables() flux.TableIterator {
	return rd
}

func (rd *ResultDecoder) Decode() (flux.Result, error) {
	return rd, nil
}

// MultiResultDecoder reads multiple results from a single csv file.
// Results are delimited by an empty line.
type MultiResultDecoder struct {
	sub    *sourceSubscription
	config *ResultDecoderConfig
}

// NewMultiResultDecoder creates a new MultiResultDecoder.
func NewMultiResultDecoder(s *sourceSubscription, c *ResultDecoderConfig) *MultiResultDecoder {
	return &MultiResultDecoder{
		sub:    s,
		config: c,
	}
}

func (d *MultiResultDecoder) Decode() (flux.ResultIterator, error) {
	return &resultIterator{
		sub:    d.sub,
		config: d.config,
	}, nil
}

// resultIterator iterates through the results encoded in r.
type resultIterator struct {
	config *ResultDecoderConfig
	sub    *sourceSubscription
	next   *ResultDecoder
	err    error

	canceled bool
}

func (r *resultIterator) More() bool {
	return true
}

func (r *resultIterator) Next() flux.Result {
	return NewResultDecoder(r.sub, r.config)
}

func (r *resultIterator) Release() {
	if r.canceled {
		return
	}

	// Close the subscription.
	r.sub.close()

	r.canceled = true
}

func (r *resultIterator) Err() error {
	return r.err
}

func (r *resultIterator) Statistics() flux.Statistics {
	return flux.Statistics{}
}
