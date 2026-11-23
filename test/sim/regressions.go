package sim

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RegressionSeed is one line of a regression file: a seed that once failed
// (or guards a past bug) and the profile to replay it with.
type RegressionSeed struct {
	Seed    uint64
	Profile string
	Source  string // file:line, for error messages
	Comment string
}

// ParseRegressions reads regression lines of the form
//
//	<seed> <profile> [# comment]
//
// Blank lines and lines starting with # are ignored.
func ParseRegressions(name string, f *os.File) ([]RegressionSeed, error) {
	var out []RegressionSeed
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text, comment, _ := strings.Cut(sc.Text(), "#")
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
		src := fmt.Sprintf("%s:%d", name, line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s: want \"<seed> <profile>\", got %q", src, strings.TrimSpace(text))
		}
		seed, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: bad seed: %w", src, err)
		}
		if _, err := LookupProfile(fields[1]); err != nil {
			return nil, fmt.Errorf("%s: %w", src, err)
		}
		out = append(out, RegressionSeed{Seed: seed, Profile: fields[1], Source: src, Comment: strings.TrimSpace(comment)})
	}
	return out, sc.Err()
}

// LoadRegressions reads every *.txt file in dir, in name order.
func LoadRegressions(dir string) ([]RegressionSeed, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.txt"))
	if err != nil {
		return nil, err
	}
	var out []RegressionSeed
	for _, name := range files { // Glob returns sorted names
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		seeds, err := ParseRegressions(filepath.Base(name), f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, seeds...)
	}
	return out, nil
}
