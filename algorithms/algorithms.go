package algorithms

import (
	"github.com/sanderblue/algorithms/pkg/ringallreduce"
)

type Algorithms struct {
	RingAllReduce ringallreduce.RingAllReduce
}

func New() *Algorithms {
	return &Algorithms{
		RingAllReduce: ringallreduce.New(),
	}
}
