// +build darwin

package main

import "golang.org/x/sys/unix"

func isTerm(fd uintptr) bool {
	_, err := unix.IoctlGetTermios(int(fd), unix.TIOCGETA)
	return err == nil
}
