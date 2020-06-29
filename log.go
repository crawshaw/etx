package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"crawshaw.io/sqlite"
)

type colorset string // semi-colon separated integers, e.g. "%d;%d"

const (
	noColor  = colorset("")
	fgYellow = colorset("33")
	fgBlue   = colorset("34")
)

func log(conn *sqlite.Conn, keyPrefix string) error {
	smartTerm := os.Getenv("TERM") != "dumb" && isTerm(os.Stdout.Fd())
	if !smartTerm {
		printf := func(_ colorset, format string, args ...interface{}) {
			fmt.Printf(format, args...)
		}
		return logStream(printf, conn, keyPrefix)
	}

	pipeR, pipeW := io.Pipe()
	defer pipeR.Close()

	cmd := exec.Command("less", "-F", "-r")
	cmd.Stdin = pipeR
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	cmdErrCh := make(chan error)
	go func() {
		cmdErrCh <- cmd.Wait()
		pipeW.Close()
	}()

	printf := func(color colorset, format string, args ...interface{}) {
		if color != "" {
			format = fmt.Sprintf("\x1b[%sm%s\x1b[0m", color, format)
		}
		fmt.Fprintf(pipeW, format, args...)
	}
	err := logStream(printf, conn, keyPrefix)
	pipeW.Close()
	if err2 := <-cmdErrCh; err == nil {
		err = err2
	}
	return err
}

func logStream(printf func(color colorset, format string, args ...interface{}), conn *sqlite.Conn, keyPrefix string) error {
	stmt := conn.Prep(`
		select
			history.mod_revision,
			revtime.watch_time,
			group_concat(history.key, '` + "\t" + `') as keys
		from history join revtime
		on history.mod_revision = revtime.mod_revision
		group by history.mod_revision
		order by history.mod_revision desc;`)
	for {
		if hasNext, err := stmt.Step(); err != nil {
			return err
		} else if !hasNext {
			break
		}
		modRev := stmt.GetInt64("mod_revision")
		t := time.Unix(0, stmt.GetInt64("watch_time"))
		keys := strings.Split(stmt.GetText("keys"), "\t")

		var prefixes []string
		byprefix := make(map[string][]string)
		for _, key := range keys {
			parts := strings.Split(key, "/")
			if len(parts) < 4 {
				prefixes = append(prefixes, key)
				continue
			}
			// "", "cdb", "node", ...
			prefix := "/" + parts[1] + "/" + parts[2] + "/"
			byprefix[prefix] = append(byprefix[prefix], strings.TrimPrefix(key, prefix))
		}
		for prefix := range byprefix {
			prefixes = append(prefixes, prefix)
		}
		sort.Strings(prefixes)

		printf(fgYellow, "%12d\t", modRev)
		printf(fgBlue, "%s\t", t.Format("Jan-02 15:04:05"))
		for i, prefix := range prefixes {
			if i > 0 {
				printf(noColor, ", ")
			}
			suffixes := byprefix[prefix]
			if len(suffixes) == 0 {
				printf(noColor, "%s", prefix)
				continue
			} else if len(suffixes) == 1 {
				printf(noColor, "%s%s", prefix, suffixes[0])
				continue
			}
			printf(noColor, "%s{", prefix)
			for i, suffix := range suffixes {
				if i > 0 {
					printf(noColor, ", ")
				}
				printf(noColor, "%s", suffix)
			}
			printf(noColor, "}")
		}
		printf(noColor, "\n")
	}
	return nil
}
