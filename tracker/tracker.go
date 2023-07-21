package tracker

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"

	iter "github.com/cerc-io/eth-iterator-utils"
)

type Tracker struct {
	recoveryFile string

	startChan chan *Iterator
	stopChan  chan *Iterator
	started   map[*Iterator]struct{}
	stopped   []*Iterator
	running   bool
}

type Iterator struct {
	trie.NodeIterator
	tracker *Tracker
}

func New(file string, bufsize uint) Tracker {
	return Tracker{
		recoveryFile: file,
		startChan:    make(chan *Iterator, bufsize),
		stopChan:     make(chan *Iterator, bufsize),
		started:      map[*Iterator]struct{}{},
		running:      true,
	}
}

func (tr *Tracker) CaptureSignal(cancelCtx context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Error("Signal received (%v), stopping", "signal", sig)
		// cancel context on receiving a signal
		// on ctx cancellation, all the iterators complete processing of their current node before stopping
		cancelCtx()
	}()
}

// Tracked wraps an iterator in a Iterator. This should not be called once halts are possible.
func (tr *Tracker) Tracked(it trie.NodeIterator) (ret *Iterator) {
	ret = &Iterator{it, tr}
	tr.startChan <- ret
	return
}

// StopIterator explicitly stops an iterator
func (tr *Tracker) StopIterator(it *Iterator) {
	tr.stopChan <- it
}

// dumps iterator path and bounds to a text file so it can be restored later
func (tr *Tracker) dump() error {
	log.Debug("Dumping recovery state", "to", tr.recoveryFile)
	var rows [][]string
	for it := range tr.started {
		var endPath []byte
		if impl, ok := it.NodeIterator.(*iter.PrefixBoundIterator); ok {
			endPath = impl.EndPath
		}

		rows = append(rows, []string{
			fmt.Sprintf("%x", it.Path()),
			fmt.Sprintf("%x", endPath),
		})
	}

	file, err := os.Create(tr.recoveryFile)
	if err != nil {
		return err
	}
	defer file.Close()
	out := csv.NewWriter(file)

	return out.WriteAll(rows)
}

// Restore attempts to read iterator state from file
// if file doesn't exist, returns an empty slice with no error
func (tr *Tracker) Restore(makeIterator iter.IteratorConstructor) ([]trie.NodeIterator, error) {
	file, err := os.Open(tr.recoveryFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	log.Debug("Restoring recovery state", "from", tr.recoveryFile)

	in := csv.NewReader(file)
	in.FieldsPerRecord = 2
	rows, err := in.ReadAll()
	if err != nil {
		return nil, err
	}

	var ret []trie.NodeIterator
	for _, row := range rows {
		// pick up where each recovered iterator left off
		var recoveredPath []byte
		var endPath []byte

		if len(row[0]) != 0 {
			if _, err = fmt.Sscanf(row[0], "%x", &recoveredPath); err != nil {
				return nil, err
			}
		}
		if len(row[1]) != 0 {
			if _, err = fmt.Sscanf(row[1], "%x", &endPath); err != nil {
				return nil, err
			}
		}

		// force the lower bound path to an even length (required by geth API/HexToKeyBytes)
		if len(recoveredPath)&0b1 == 1 {
			// decrement first to avoid skipped nodes
			decrementPath(recoveredPath)
			recoveredPath = append(recoveredPath, 0)
		}
		it := makeIterator(iter.HexToKeyBytes(recoveredPath))
		boundIt := iter.NewPrefixBoundIterator(it, endPath)
		ret = append(ret, tr.Tracked(boundIt))
	}

	log.Debug("Restored iterators", "count", len(ret))
	return ret, nil
}

func (tr *Tracker) HaltAndDump() error {
	tr.running = false

	// drain any pending iterators
	close(tr.startChan)
	for start := range tr.startChan {
		tr.started[start] = struct{}{}
	}
	close(tr.stopChan)
	for stop := range tr.stopChan {
		tr.stopped = append(tr.stopped, stop)
	}

	for _, stop := range tr.stopped {
		delete(tr.started, stop)
	}

	if len(tr.started) == 0 {
		// if the tracker state is empty, erase any existing recovery file
		err := os.Remove(tr.recoveryFile)
		if os.IsNotExist(err) {
			err = nil
		}
		return err
	}

	return tr.dump()
}

func (it *Iterator) Next(descend bool) bool {
	ret := it.NodeIterator.Next(descend)

	if !ret {
		if it.tracker.running {
			it.tracker.stopChan <- it
		} else {
			log.Error("Iterator stopped after tracker halted", "path", it.Path())
		}
	}
	return ret
}

// Subtracts 1 from the last byte in a path slice, carrying if needed.
// Does nothing, returning false, for all-zero inputs.
func decrementPath(path []byte) bool {
	// check for all zeros
	allzero := true
	for i := 0; i < len(path); i++ {
		allzero = allzero && path[i] == 0
	}
	if allzero {
		return false
	}
	for i := len(path) - 1; i >= 0; i-- {
		val := path[i]
		path[i]--
		if val == 0 {
			path[i] = 0xf
		} else {
			return true
		}
	}
	return true
}
