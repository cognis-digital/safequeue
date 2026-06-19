package queue

import "os"

// openAppend opens a file for appending; used by tests to simulate crashes.
func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
}
