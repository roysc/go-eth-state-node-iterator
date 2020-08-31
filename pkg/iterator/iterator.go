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
	// "fmt"
	"bytes"

	// "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/trie"
)

// Simplified trie node iterator
type NodeIterator interface {
	Next(bool) bool
	Error() error
	Hash() common.Hash
	Path() []byte
	Leaf() bool
}

type prefixBoundIterator struct {
	current trie.NodeIterator
	endKey  []byte
}

func (it *prefixBoundIterator) Next(descend bool) bool {
	if it.endKey == nil {
		return it.current.Next(descend)
	}
	cmp := bytes.Compare(it.current.Path(), it.endKey) // stop before endKey
	if cmp >= 0 {
		return false
	}
	return it.current.Next(descend)
}

func (it *prefixBoundIterator) Error() error {
	return it.current.Error()
}
func (it *prefixBoundIterator) Hash() common.Hash {
	return it.current.Hash()
}
func (it *prefixBoundIterator) Path() []byte {
	return it.current.Path()
}
func (it *prefixBoundIterator) Leaf() bool {
	return it.current.Leaf()
}

// Iterator with an upper bound value (hex path prefix)
func NewPrefixBoundIterator(it trie.NodeIterator, to []byte) NodeIterator {
	return &prefixBoundIterator{current: it, endKey: to}
}

// array of 0..f with prefix
func prefixedNibbles(prefix []byte) [][]byte {
	var ret [][]byte
	for i := byte(0); i < 16; i++ {
		elem := make([]byte, len(prefix))
		copy(elem, prefix)
		elem = append(elem, i)
		ret = append(ret, elem)
	}
	return ret
}

// Generates ordered cartesian product of all nibbles of given length, w/ optional prefix
// eg. MakePaths([4], 2) => [[4 0 0] [4 0 1] ... [4 f f]]
func MakePaths(prefix []byte, length int) [][]byte {
	paths := [][]byte{prefix}
	for depth := 0; depth < length; depth++ {
		var newPaths [][]byte
		for _, path := range paths {
			for _, newPath := range prefixedNibbles(path) {
				newPaths = append(newPaths, newPath)
			}
		}
		paths = newPaths
	}
	return paths
}

// Apply a function to 16^cutdepth subtries divided by path prefix
func VisitSubtries(tree state.Trie, cutDepth int, callback func(NodeIterator)) {
	prefixes := MakePaths(nil, cutDepth)
	// pre- and postpend nil to include root & tail
	prefixes = append(prefixes, nil)
	prefixes = append([][]byte{nil}, prefixes...)

	for i := 0; i < len(prefixes)-1; i++ {
		key := prefixes[i]
		if len(key)%2 != 0 {	// zero-pad for odd-length keys
			key = append(key, 0)
		}
		it := tree.NodeIterator(HexToKeyBytes(key))
		callback(NewPrefixBoundIterator(it, prefixes[i+1]))
	}
}
