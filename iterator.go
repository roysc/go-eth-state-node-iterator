//
// Copyright © 2020 Vulcanize, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package iterator

import (
	"bytes"
	"math/bits"

	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
)

type PrefixBoundIterator struct {
	trie.NodeIterator
	EndPath []byte
	primed  bool
}

// Next advances until reaching EndPath, or the underlying iterator becomes invalid
func (it *PrefixBoundIterator) Next(descend bool) bool {
	// NodeIterator starts in an invalid state and must be advanced once before accessing values.
	// Since this begins valid (pointing to the lower bound element), the first Next must be a no-op.
	if !it.primed {
		it.primed = true
		return (it.EndPath == nil) || bytes.Compare(it.Path(), it.EndPath) < 0
	}

	if !it.NodeIterator.Next(descend) {
		return false
	}
	return (it.EndPath == nil) || bytes.Compare(it.Path(), it.EndPath) < 0
}

// Iterator with an upper bound value (hex path prefix)
func NewPrefixBoundIterator(tree state.Trie, from, to []byte) *PrefixBoundIterator {
	it := tree.NodeIterator(nil)
	for it.Next(true) {
		if bytes.Compare(from, it.Path()) <= 0 {
			break
		}
	}
	return &PrefixBoundIterator{NodeIterator: it, EndPath: to}
}

type prefixGenerator struct {
	current   []byte
	step      byte
	stepIndex uint
}

func newPrefixGenerator(nbins uint) prefixGenerator {
	if bits.OnesCount(nbins) != 1 {
		panic("nbins must be a power of 2")
	}
	// determine step dist. and path index at which to step
	var step byte
	var stepIndex uint
	for ; nbins != 0; stepIndex++ {
		divisor := byte(nbins & 0xF)
		if divisor != 0 {
			step = 0x10 / divisor
		}
		nbins = nbins >> 4
	}
	return prefixGenerator{
		current:   make([]byte, stepIndex),
		step:      step,
		stepIndex: stepIndex - 1,
	}
}

func (gen *prefixGenerator) Value() []byte {
	return gen.current
}

func (gen *prefixGenerator) HasNext() bool {
	return gen.current[0] < 0x10
}

func (gen *prefixGenerator) Next() {
	// increment the cursor, and handle overflow
	gen.current[gen.stepIndex] += gen.step
	overflow := false
	for ix := 0; ix < len(gen.current); ix++ {
		rix := len(gen.current) - 1 - ix // index in prefix is reverse
		if overflow {                    // apply overflow
			gen.current[rix]++
			overflow = false
		}
		// detect overflow at this index
		if rix != 0 && gen.current[rix] > 0xf {
			gen.current[rix] = 0
			overflow = true
		}
	}
}

// MakePaths generates paths that cut trie domain into `nbins` uniform conterminous bins (w/ opt. prefix)
// eg. MakePaths([], 2) => [[0] [8]]
//	   MakePaths([4], 32) => [[4 0 0] [4 0 8] [4 1 0]... [4 f 8]]
func MakePaths(prefix []byte, nbins uint) [][]byte {
	var res [][]byte
	for it := newPrefixGenerator(nbins); it.HasNext(); it.Next() {
		next := make([]byte, len(prefix))
		copy(next, prefix)
		next = append(next, it.Value()...)
		res = append(res, next)
	}
	return res
}

// truncate zeros from end of a path
func truncateZeros(path []byte) []byte {
	l := len(path)
	for ; l > 0 && path[l-1] == 0; l-- {
	}
	return path[:l]
}

func eachInterval(prefix []byte, nbins uint, cb func([]byte, []byte)) {
	paths := MakePaths(prefix, nbins)
	paths = append(paths, nil) // include tail
	paths[0] = nil             // set bin 0 left bound to nil to include root
	for i := 0; i < len(paths)-1; i++ {
		cb(truncateZeros(paths[i]), truncateZeros(paths[i+1]))
	}
}

// SubtrieIterators cuts a trie by path prefix, returning `nbins` iterators covering its subtries
func SubtrieIterators(tree state.Trie, nbins uint) []trie.NodeIterator {
	var iters []trie.NodeIterator
	eachInterval(nil, nbins, func(from, to []byte) {
		iters = append(iters, NewPrefixBoundIterator(tree, from, to))
	})
	return iters
}
