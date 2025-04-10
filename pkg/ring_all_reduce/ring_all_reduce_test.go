package ringallreduce

import (
	"testing"
)

func TestRingAllReduce(t *testing.T) {
	procs := 4
	chunkSize := 1

	result := Execute(procs, chunkSize)

	expected := float64((procs * (procs + 1)) / 2)
	for procIdx, proc := range result {
		for j, v := range proc.Data {
			if v != expected {
				t.Errorf("Node %d, element %d: expected %f, got %f", procIdx, j, expected, v)
			}
		}
	}
}
