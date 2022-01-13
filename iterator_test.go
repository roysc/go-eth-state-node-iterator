package iterator_test

import (
	"testing"

	iter "github.com/vulcanize/go-eth-state-node-iterator"
)

func TestMakePaths(t *testing.T) {
	var prefix []byte
	for i := 0; i < 4; i++ {
		nbins := uint(1) << i
		paths := iter.MakePaths(prefix, nbins)
		t.Log(paths)
		if len(paths) != int(nbins) {
			t.Logf("failed: TestMakePaths")
			t.Error("wrong number of paths", len(paths))
		}
	}
}
