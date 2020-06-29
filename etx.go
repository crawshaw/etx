package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

func main() {
	flag.Parse()
	if err := runSubcommand(flag.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func runSubcommand(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("no subcommand specified; run `%s help` for more info", os.Args[0])
	}
	switch args[0] {
	case "help":
		// TODO merge with -h
		fmt.Printf("%s: an etcd historian\n", os.Args[0])
		fmt.Printf("\n\tbetter help todo\n")
		return nil
	case "version":
		bi, _ := debug.ReadBuildInfo()
		if bi == nil {
			return fmt.Errorf("version: cannot read build info")
		}
		fmt.Printf("%s: %s %s %s\n", os.Args[0], bi.Main.Path, bi.Main.Version, bi.Main.Sum)
		return nil
	case "watch":
		if err := runWatch(args[1:]); err != nil {
			return fmt.Errorf("watch: %w", err)
		}
		return nil
	case "log":
		if err := runLog(args[1:]); err != nil {
			return fmt.Errorf("log: %w", err)
		}
		return nil
	case "show":
		if err := runShow(args[1:]); err != nil {
			return fmt.Errorf("show: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown subcommand %s", args[0])
	}
}

func runShow(args []string) error {
	fs := flag.NewFlagSet("log", 0)
	flagFile := defineFileFlag(fs)
	// TODO flagPrefix := definePrefixFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *flagFile == "" {
		return errors.New("-file not defined")
	}
	if len(fs.Args()) != 1 {
		return errors.New("show expects one arg: a revision number")
	}
	rev, err := strconv.ParseInt(fs.Args()[0], 10, 64)
	if err != nil {
		return err
	}
	conn, err := sqlite.OpenConn(*flagFile, 0) // TODO readonly
	if err != nil {
		return err
	}
	defer conn.Close()
	return show(conn, rev)
}

func runLog(args []string) error {
	fs := flag.NewFlagSet("log", 0)
	flagFile := defineFileFlag(fs)
	// TODO flagPrefix := definePrefixFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *flagFile == "" {
		return errors.New("-file not defined")
	}
	conn, err := sqlite.OpenConn(*flagFile, 0) // TODO readonly
	if err != nil {
		return err
	}
	defer conn.Close()
	return log(conn, "/")
}

func runWatch(args []string) error {
	ctx := context.Background()

	fs := flag.NewFlagSet("watch", 0)
	flagFile := defineFileFlag(fs)
	flagAddr, flagAuth := defineEtcdRemoteFlags(fs)
	flagPrefix := definePrefixFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *flagFile == "" {
		return errors.New("-file not defined")
	}
	os.MkdirAll(filepath.Dir(*flagFile), 0777)
	if *flagAddr == "" {
		return errors.New("-addr not defined")
	}

	conn, err := sqlite.OpenConn(*flagFile, 0)
	if err != nil {
		return err
	}
	if err := sqlitex.ExecScript(conn, schema); err != nil {
		return fmt.Errorf("schema prep: %w", err)
	}
	maxModRev, err := sqlitex.ResultInt64(conn.Prep("select ifnull(max(mod_revision), 0) from history;"))
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%s watch: starting at revision %d\n", os.Args[0], maxModRev)
	go func() {
		if err := fillTo(ctx, conn, rev(maxModRev)); err != nil {
			fmt.Fprintf(os.Stderr, "background fill failed: %w", err)
		}
	}()
	err = watch(ctx, conn, *flagAddr, *flagAuth, *flagPrefix, rev(maxModRev))
	if err != nil {
		return err
	}
	return nil
}

func defineEtcdRemoteFlags(fs *flag.FlagSet) (flagAddr *string, flagAuth *string) {
	flagAddr = fs.String("addr", "http://127.0.0.1:2379", "etcd endpoint address")
	flagAuth = fs.String("auth", "", "etcd auth header value")
	return flagAddr, flagAuth
}

func definePrefixFlag(fs *flag.FlagSet) *string {
	return fs.String("prefix", "/", "etcd key prefix")
}

func defineFileFlag(fs *flag.FlagSet) *string {
	defaultValue := ""
	if dir, err := userDataDir(); err == nil {
		defaultValue = filepath.Join(dir, "etx", "etx.db")
	}
	return fs.String("file", defaultValue, "database file")
}

func userDataDir() (string, error) {
	if runtime.GOOS == "darwin" {
		dir := os.Getenv("HOME")
		if dir == "" {
			return "", errors.New("no $HOME")
		}
		return dir + "/Library/Application Support", nil
	}

	if dir := os.Getenv("XDG_DATA_HOME"); dir != "" {
		return dir, nil
	}
	dir := os.Getenv("HOME")
	if dir == "" {
		return "", errors.New("no $XDG_DATA_HOME and no $HOME")
	}
	dir += "/.local/share"
	return dir, nil
}

const schema = `
create table if not exists history (
	key          text    not null,
	value        text    not null,
	mod_revision integer not null,

	primary key (key, mod_revision)
);
create table if not exists revtime (
	mod_revision integer not null primary key,
	watch_time   integer not null
);

`
