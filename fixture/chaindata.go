package fixture

import (
	"os"
	"path/filepath"
	"runtime"
)

var (
	ChainDataPath, AncientDataPath string
)

func init() {
	_, path, _, _ := runtime.Caller(0)
	wd := filepath.Dir(path)

	ChainDataPath = filepath.Join(wd, "..", "fixture", "chaindata")
	AncientDataPath = filepath.Join(ChainDataPath, "ancient")

	if _, err := os.Stat(ChainDataPath); err != nil {
		panic("must populate chaindata at " + ChainDataPath)
	}
}
