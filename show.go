package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"crawshaw.io/sqlite"
)

func show(conn *sqlite.Conn, rev int64) error {
	stmt := conn.Prep(`select
			key,
			old,
			new
		from (
			select key, value as new from history
			where mod_revision = $rev
		) left join (
			select key, value as old from history
			where mod_revision < $rev
			and key in (select key from history where mod_revision = $rev)
			order by mod_revision desc limit 1
		)
		using (key) order by key;`)
	stmt.SetInt64("$rev", rev)

	type kv struct {
		key string
		old []byte
		new []byte
	}
	var kvs []kv
	for {
		if hasNext, err := stmt.Step(); err != nil {
			return err
		} else if !hasNext {
			break
		}

		v := kv{
			key: stmt.GetText("key"),
			old: []byte(stmt.GetText("old")),
			new: []byte(stmt.GetText("new")),
		}

		buf := new(bytes.Buffer)
		if err := json.Indent(buf, v.old, "", "\t"); err == nil {
			v.old = buf.Bytes()
			v.old = append(v.old, '\n')
		}
		buf = new(bytes.Buffer)
		if err := json.Indent(buf, v.new, "", "\t"); err == nil {
			v.new = buf.Bytes()
			v.new = append(v.new, '\n')
		}
		kvs = append(kvs, v)
	}

	tmpdir, err := ioutil.TempDir("", "etx-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	if os.PathSeparator != '/' {
		panic("etx show depends on '/' paths")
	}
	for _, v := range kvs {
		a := filepath.Join(tmpdir, "a", v.key)
		b := filepath.Join(tmpdir, "b", v.key)
		if err := os.MkdirAll(filepath.Dir(a), 0700); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(b), 0700); err != nil {
			return err
		}
		if err := ioutil.WriteFile(a, v.old, 0600); err != nil {
			return err
		}
		if err := ioutil.WriteFile(b, v.new, 0600); err != nil {
			return err
		}
	}

	// We use git diff because it supports colors on macOS.
	cmd := exec.Command("git", "diff", "--no-index", "--", "a", "b")
	cmd.Dir = tmpdir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run() // diff returns exit code 1 on printing a diff

	return nil
}
