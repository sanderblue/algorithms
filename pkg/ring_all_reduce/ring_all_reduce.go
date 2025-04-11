// References:
//
// https://www.cs.fsu.edu/~xyuan/paper/09jpdc.pdf

package ringallreduce

import (
	"fmt"
	"sync"
)

type RingAllReduce struct{}

func New() *RingAllReduce {
	return &RingAllReduce{}
}

// Msg models a message sent between processes.
type Msg struct {
	ChunkIdx int       // which chunk the message contains
	Data     []float64 // the slice of data for that chunk
}

// Node models a participant in the ring all–reduce.
type Node struct {
	Rank      int       // process index (0..P-1)
	P         int       // total number of processes
	ChunkSize int       // size of a single chunk (each vector length is P*ChunkSize)
	Data      []float64 // local data buffer; logically divided into P chunks
	In        chan Msg  // channel from which this process receives messages (from its left neighbor)
	Out       chan Msg  // channel to which this process sends messages (to its right neighbor)
}

// Run executes the ring all–reduce algorithm for one process.
// It performs a reduce–scatter phase followed by an allgather phase.
func (proc *Node) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	// -------------------------------------------------
	// Reduce–Scatter phase:
	// In P–1 steps, each process sends a chunk (using indices computed cyclically)
	// and receives a corresponding chunk from its left neighbor.
	// The received chunk is added (element–wise) to the chunk in the local buffer.
	// At the end of this phase, each process holds a fully reduced segment.
	// The designated segment is at index: D = (Rank - (P-1) + P) mod P,
	// which simplifies to: D = (Rank + 1) mod P.
	// -------------------------------------------------
	for s := 0; s < proc.P-1; s++ {
		sendIdx := (proc.Rank - s + proc.P) % proc.P
		recvIdx := (proc.Rank - s - 1 + proc.P) % proc.P

		// Copy chunk to send.
		startSend := sendIdx * proc.ChunkSize
		msgData := make([]float64, proc.ChunkSize)
		copy(msgData, proc.Data[startSend:startSend+proc.ChunkSize])
		proc.Out <- Msg{ChunkIdx: sendIdx, Data: msgData}

		// Receive message.
		received := <-proc.In
		if received.ChunkIdx != recvIdx {
			fmt.Printf("Node %d (Reduce–Scatter): Expected chunk %d but received %d\n",
				proc.Rank, recvIdx, received.ChunkIdx)
		}
		// Element–wise reduction.
		startRecv := recvIdx * proc.ChunkSize
		for i := 0; i < proc.ChunkSize; i++ {
			proc.Data[startRecv+i] += received.Data[i]
		}
	}

	// -------------------------------------------------
	// Allgather phase:
	// After reduce–scatter, each process holds a complete reduced chunk.
	// We choose the reduced chunk to reside at index D = (Rank + 1) mod P.
	// In the allgather phase the goal is to circulate the reduced chunks so that each
	// process ends with the complete reduced vector.
	// We perform a rotation for s = 0, 1, …, P–2:
	//   sendIdx = (Rank + 1 + s) mod P
	//   recvIdx = (Rank + 2 + s) mod P
	// This way, the designated reduced segment is first sent to the right and all segments are filled in.
	// -------------------------------------------------
	for s := 0; s < proc.P-1; s++ {
		sendIdx := (proc.Rank + 1 + s) % proc.P
		recvIdx := (proc.Rank + 2 + s) % proc.P

		// Copy chunk to send.
		startSend := sendIdx * proc.ChunkSize
		msgData := make([]float64, proc.ChunkSize)
		copy(msgData, proc.Data[startSend:startSend+proc.ChunkSize])
		proc.Out <- Msg{ChunkIdx: sendIdx, Data: msgData}

		// Receive chunk and place it into the proper position.
		received := <-proc.In
		if received.ChunkIdx != recvIdx {
			fmt.Printf("Node %d (Allgather): Expected chunk %d but received %d\n",
				proc.Rank, recvIdx, received.ChunkIdx)
		}
		startRecv := recvIdx * proc.ChunkSize
		copy(proc.Data[startRecv:startRecv+proc.ChunkSize], received.Data)
	}
}

// Each process’ vector is composed of n chunks (total length = n * chunkSize = vector)
func (r *RingAllReduce) Execute(procs int, chunkSize int) []*Node {
	// For demonstration, simulate 4 processes.
	p := procs
	totalSize := p * chunkSize // total number of elements

	// Create a channel for each process.
	// We arrange the ring so that process i sends to process (i+1) mod p.
	channels := make([]chan Msg, p)
	for i := 0; i < p; i++ {
		channels[i] = make(chan Msg, 2) // buffered to help avoid deadlock.
	}

	// Initialize processes.
	// Each process’s vector is filled with a constant equal to (Rank+1).
	// Therefore, the element–wise reduction (using addition) should yield sum 1+2+3+4 = 10.
	processes := make([]*Node, p)
	for i := 0; i < p; i++ {
		data := make([]float64, totalSize)
		for j := 0; j < totalSize; j++ {
			data[j] = float64(i + 1)
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

	// Run the algorithm concurrently.
	var wg sync.WaitGroup
	wg.Add(p)
	for i := 0; i < p; i++ {
		go processes[i].Run(&wg)
	}
	wg.Wait()

	// Print final data.
	// Every element should equal 10.
	for i := 0; i < p; i++ {
		fmt.Printf("Node %d final data: %v\n", i, processes[i].Data)
	}

	return processes
}
