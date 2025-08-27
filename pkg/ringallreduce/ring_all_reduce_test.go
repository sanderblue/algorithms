package ringallreduce

import (
	"sync"
	"testing"
)

func TestRingAllReduce(t *testing.T) {
	procs := 4
	chunkSize := 1

	r := New()

	result := r.Execute(procs, chunkSize)

	expected := float64((procs * (procs + 1)) / 2)
	for procIdx, proc := range result {
		for j, v := range proc.Data {
			if v != expected {
				t.Errorf("Node %d, element %d: expected %f, got %f", procIdx, j, expected, v)
			}
		}
	}
}

func TestRingAllReduce_Execute_UniformData(t *testing.T) {
	tests := []struct {
		name      string
		procs     int
		chunkSize int
	}{
		{name: "p=2,chunk=1", procs: 2, chunkSize: 1},
		{name: "p=4,chunk=3", procs: 4, chunkSize: 3},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			result := r.Execute(tc.procs, tc.chunkSize)

			expected := float64((tc.procs * (tc.procs + 1)) / 2)
			for procIdx, proc := range result {
				for j, v := range proc.Data {
					if v != expected {
						t.Errorf("Execute uniform: node=%d, elem=%d: expected %f, got %f", procIdx, j, expected, v)
					}
				}
			}
		})
	}
}

func TestRingAllReduce_CustomData_DistinctChunks(t *testing.T) {
	tests := []struct {
		name      string
		procs     int
		chunkSize int
	}{
		{name: "p=3,chunk=1", procs: 3, chunkSize: 1},
		{name: "p=4,chunk=4", procs: 4, chunkSize: 4},
		{name: "p=5,chunk=2", procs: 5, chunkSize: 2},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p := tc.procs
			chunkSize := tc.chunkSize
			totalSize := p * chunkSize

			channels := make([]chan Msg, p)
			for i := 0; i < p; i++ {
				channels[i] = make(chan Msg, 2)
			}

			processes := make([]*Node, p)
			for i := 0; i < p; i++ {
				data := make([]float64, totalSize)
				for j := 0; j < totalSize; j++ {
					c := j / chunkSize
					k := j % chunkSize
					// Per-chunk distinctive base, plus per-process variation.
					// This exposes both mis-indexing and reduction mistakes.
					data[j] = float64(1000*c + 10*k + i) // i varies across processes
				}
				processes[i] = &Node{
					Rank:      i,
					P:         p,
					ChunkSize: chunkSize,
					Data:      data,
					In:        channels[i],
					Out:       channels[(i+1)%p],
				}
			}

			var wg sync.WaitGroup
			wg.Add(p)
			for i := 0; i < p; i++ {
				go processes[i].Run(&wg)
			}
			wg.Wait()

			// Expected per element:
			// Sum over i=0..p-1 of (1000*c + 10*k + i) = p*(1000*c + 10*k) + p*(p-1)/2
			sumI := float64(p*(p-1)) / 2.0
			for procIdx, proc := range processes {
				if len(proc.Data) != totalSize {
					t.Fatalf("node=%d: expected len=%d, got %d", procIdx, totalSize, len(proc.Data))
				}
				for j := 0; j < totalSize; j++ {
					c := j / chunkSize
					k := j % chunkSize
					base := float64(1000*c + 10*k)
					expected := float64(p)*base + sumI
					if proc.Data[j] != expected {
						t.Errorf("distinct chunks: node=%d, elem=%d (chunk=%d,offset=%d): expected %v, got %v",
							procIdx, j, c, k, expected, proc.Data[j])
					}
				}
			}
		})
	}
}
