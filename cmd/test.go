package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/cskr/pubsub"
	"github.com/influxdata/flux"
	_ "github.com/influxdata/flux/builtin"
	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/stdlib/zetta"
)

func main() {
	ctx := context.Background()

	ps := pubsub.New(0)
	go publish(ps)

	config := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     1,
		MemoryBytesQuota:     math.MaxInt64,
	}

	config.ExecutorDependencies["fromZetta"] = ps

	controller := control.New(config)

	c := lang.FluxCompiler{
		Query: `
		import "zetta"
		zetta.from(stream: "device/123/12")
		  |> window(every: 1s)
			|> yield()`,
	}

	qry, err := controller.Query(ctx, c)
	if err != nil {
		log.Panic(err)
	}

	results := flux.NewResultIteratorFromQuery(qry)
	defer results.Release()

	for results.More() {
		fmt.Println("Main.HasMore")
		result := results.Next()
		if err := result.Tables().Do(func(tbl flux.Table) error {
			if err := tbl.Do(func(cr flux.ColReader) error {
				fmt.Println("Main.Do Callback")
				colMeta := cr.Cols()
				fmt.Printf("Metadata: ")
				for i := 0; i < len(colMeta); i++ {
					fmt.Printf("%s:%s ", colMeta[i].Label, colMeta[i].Type)
				}
				fmt.Println()

				l := cr.Len()
				for i := 0; i < l; i++ {
					fmt.Printf("Record %d:", i)
					for j := 0; j < len(colMeta); j++ {
						printValue(i, j, colMeta[j].Type, cr)
					}
					fmt.Println()
				}

				return nil
			}); err != nil {
				log.Panic(err)
			}
			return nil
		}); err != nil {
			log.Panic(err)
		}
	}
	fmt.Println("NO more.")
}

func printValue(i, j int, c flux.ColType, cr flux.ColReader) {
	switch c {
	case flux.TBool:
		if cr.Bools(j).IsValid(i) {
			fmt.Printf("%v ", cr.Bools(j).Value(i))
		}
	case flux.TInt:
		if cr.Ints(j).IsValid(i) {
			fmt.Printf("%d ", cr.Ints(j).Value(i))
		}
	case flux.TUInt:
		if cr.UInts(j).IsValid(i) {
			fmt.Printf("%d ", cr.UInts(j).Value(i))
		}
	case flux.TFloat:
		if cr.Floats(j).IsValid(i) {
			fmt.Printf("%f ", cr.Floats(j).Value(i))
		}
	case flux.TString:
		if cr.Strings(j).IsValid(i) {
			fmt.Printf("%s ", cr.Strings(j).ValueString(i))
		}
	case flux.TTime:
		if cr.Times(j).IsValid(i) {
			fmt.Printf("%v ", cr.Times(j).Value(i))
		}
	}
}

func publish(ps *pubsub.PubSub) {
	time.Sleep(time.Millisecond * 1000)

	for i := 0; i < 100; i++ {
		hum := float64(i)
		m := zetta.ZettaMessage{
			Type:      "event",
			Topic:     "device/123/12",
			Timestamp: time.Now(),
			Device:    "123/12",
			Humidity:  hum,
		}
		ps.Pub(m, "device/123/12")
		time.Sleep(time.Millisecond * 100)
	}
}
