package manageos

import (
	"fmt"
	"syscall"
)

func AugmentStackSizeLimit() error {
	// Set ulimit for stack
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_STACK, &rLimit)
	if err != nil {
		return fmt.Errorf("getting rlimit: %s", err)
	}
	rLimit.Cur = 67104768

	err = syscall.Setrlimit(syscall.RLIMIT_STACK, &rLimit)
	if err != nil {
		return fmt.Errorf("setting rlimit: %s", err)
	}

	return nil
}
