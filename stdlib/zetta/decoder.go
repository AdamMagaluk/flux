package zetta

import (
	"fmt"
	"time"

	"github.com/cskr/pubsub"
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
	reader *chan interface{}
	ps     *pubsub.PubSub
	config *ResultDecoderConfig
}

// NewResultDecoder creates a new result decoder from config.
func NewResultDecoder(reader *chan interface{}, ps *pubsub.PubSub, config *ResultDecoderConfig) *ResultDecoder {
	return &ResultDecoder{config: config, ps: ps, reader: reader}
}

// ResultDecoderConfig is the configuration for a result decoder.
type ResultDecoderConfig struct {
}

func (rd *ResultDecoder) Do(f func(flux.Table) error) error {

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

	fmt.Println("Zetta.ResultDecoder.Do")
	for {
		select {
		case e, ok := <-*rd.reader:
			if !ok {
				fmt.Println("Channel closed")
				// Channel closed.
				goto RETURN
			}
			m, ok := e.(ZettaMessage)
			if !ok {
				fmt.Println("Not ZettaMessage")
				goto RETURN
			}
			fmt.Println("Got Event", m)

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
		case <-time.After(1 * time.Second):
			fmt.Println("timeout 1")
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
