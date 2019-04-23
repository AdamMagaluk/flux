package zetta_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/stdlib/zetta"

	_ "github.com/influxdata/flux/builtin" // We need to import the builtins for the tests to work.
)

func TestFromZetta_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from no args",
			Raw: `import "zetta"
zetta.from()`,
			WantErr: true,
		},
		{
			Name: "from ok",
			Raw: `import "zetta"
zetta.from(stream: "device/123/12", decoder: "line") |> range(start:-4h, stop:-2h) |> sum()`,
			Want: &flux.Spec{
				Operations: []*flux.Operation{
					{
						ID: "fromSocket0",
						Spec: &zetta.FromZettaOpSpec{
							Stream: "device/123/12",
						},
					},
					{
						ID: "range1",
						Spec: &universe.RangeOpSpec{
							Start: flux.Time{
								Relative:   -4 * time.Hour,
								IsRelative: true,
							},
							Stop: flux.Time{
								Relative:   -2 * time.Hour,
								IsRelative: true,
							},
							TimeColumn:  "_time",
							StartColumn: "_start",
							StopColumn:  "_stop",
						},
					},
					{
						ID: "sum2",
						Spec: &universe.SumOpSpec{
							AggregateConfig: execute.DefaultAggregateConfig,
						},
					},
				},
				Edges: []flux.Edge{
					{Parent: "fromSocket0", Child: "range1"},
					{Parent: "range1", Child: "sum2"},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestFromZettaOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"fromZetta","kind":"fromZetta","spec":{"stream":"device/123/21"}}`)
	op := &flux.Operation{
		ID: "fromSocket",
		Spec: &zetta.FromZettaOpSpec{
			Stream: "device/123/21",
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func makeZettaMessage(device, topic string, humid float64) zetta.ZettaMessage {
	return zetta.ZettaMessage{
		Type:      "event",
		Topic:     topic,
		Timestamp: time.Now(),
		Device:    device,
		Humidity:  humid,
	}
}
func TestFromZettaSource_Run(t *testing.T) {
	testCases := []struct {
		name  string
		spec  *zetta.FromZettaProcedureSpec
		input []zetta.ZettaMessage
		want  []*executetest.Table
	}{
		{
			name: "csv",
			spec: &zetta.FromZettaProcedureSpec{Stream: "device/123/hum"},
			input: []zetta.ZettaMessage{
				makeZettaMessage("device1", "device/123", 12.1),
				makeZettaMessage("device1", "device/123", 12.2),
				makeZettaMessage("device1", "device/123", 12.3),
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"tag1", "tag2", "boolean"},
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "double", Type: flux.TFloat},
						{Label: "boolean", Type: flux.TBool},
					},
					Data: [][]interface{}{
						{execute.Time(0), "b", "b", 0.42, false},
						{execute.Time(0), "b", "b", 0.1, false},
						{execute.Time(0), "b", "b", -0.3, false},
						{execute.Time(0), "b", "b", 10.0, false},
						{execute.Time(0), "b", "b", 5.33, false},
					},
				},
				{
					KeyCols: []string{"tag1", "tag2", "boolean"},
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "double", Type: flux.TFloat},
						{Label: "boolean", Type: flux.TBool},
					},
					Data: [][]interface{}{
						{execute.Time(0), "a", "b", 0.42, true},
						{execute.Time(0), "a", "b", 0.1, true},
						{execute.Time(0), "a", "b", -0.3, true},
						{execute.Time(0), "a", "b", 10.0, true},
						{execute.Time(0), "a", "b", 5.33, true},
					},
				},
				{
					KeyCols: []string{"tag1", "tag2", "boolean"},
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "double", Type: flux.TFloat},
						{Label: "boolean", Type: flux.TBool},
					},
					Data: [][]interface{}{
						{execute.Time(0), "b", "b", 0.42, true},
						{execute.Time(0), "b", "b", 0.1, true},
						{execute.Time(0), "b", "b", -0.3, true},
						{execute.Time(0), "b", "b", 10.0, true},
						{execute.Time(0), "b", "b", 5.33, true},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			id := executetest.RandomDatasetID()
			d := executetest.NewDataset(id)
			c := execute.NewTableBuilderCache(executetest.UnlimitedAllocator)
			c.SetTriggerSpec(plan.DefaultTriggerSpec)

			in := make(chan zetta.ZettaMessage, 3)
			for _, m := range tc.input {
				in <- m
			}
			close(in)

			ss, err := zetta.NewSocketSource(tc.spec, in, id)
			if err != nil {
				t.Fatal(err)
			}

			// Add `yield` in order to add `from` output tables to cache.
			ss.AddTransformation(executetest.NewYieldTransformation(d, c))
			ss.Run(context.Background())

			// Retrieve tables from cache.
			got, err := executetest.TablesFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeTables(got)
			executetest.NormalizeTables(tc.want)

			fmt.Println(got[0])

			// if !cmp.Equal(tc.want, got, cmpopts.EquateNaNs()) {
			// 	t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(tc.want, got))
			// }

		})

	}
}
