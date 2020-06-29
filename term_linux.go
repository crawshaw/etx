// +build linux

package main

import "golang.org/x/sys/unix"

func isTerm(fd uintptr) bool {
	_, err := unix.IoctlGetTermios(int(fd), unix.TCGETS)
	return err == nil
}
