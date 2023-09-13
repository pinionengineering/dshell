package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/desertbit/grumble"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"

	// supported datastores
	badger "github.com/ipfs/go-ds-badger"
	flatfs "github.com/ipfs/go-ds-flatfs"
	leveldb "github.com/ipfs/go-ds-leveldb"
	dsblob "github.com/pinionengineering/go-ds-blob"

	// go-ds-blob backends
	// https://gocloud.dev/howto/blob/
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
)

var (
	// global datastore we are operating
	DS datastore.Datastore
	// retured when the user tries to use commands with an un-opened datastore
	ERRNoDatastore = fmt.Errorf("no datastore. hint: use the open command")
)

func main() {
	defer func() {
		if DS != nil {
			DS.Close()
		}
	}()

	app := grumble.New(&grumble.Config{
		Name:        "dshell",
		Description: "A shell for interacting with datastores.",
		Prompt:      "dshell()-> ",
	})

	app.AddCommand(&grumble.Command{
		Name: "open",
		Help: "open a datastore",
		Args: func(a *grumble.Args) {
			a.String("kind", "the kind of datastore to open")
			a.String("path", "the path to the datastore")
		},
		Run: openCommand,
	})

	app.AddCommand(&grumble.Command{
		Name: "kinds",
		Help: "list the kinds of datastores that can be opened",
		Run:  kindsCommand,
	})

	app.AddCommand(&grumble.Command{
		Name:  "get",
		Help:  "get a value from the datastore and print it",
		Usage: "get [-b] <key>",
		Flags: func(f *grumble.Flags) {
			f.Bool("b", "binary", false, "print binary value to stdout")
			f.StringL("save", "", "save the value to a file")
		},
		Args: func(a *grumble.Args) {
			a.String("key", "the key to get")
		},
		Run: requireDS(getCommand),
	})

	app.AddCommand(&grumble.Command{
		Name:  "put",
		Help:  "put a value into the datastore",
		Usage: "put <key> <value>",
		Args: func(a *grumble.Args) {
			a.String("key", "the key to put")
			a.String("value", "the value to put")
		},
		Run: requireDS(putCommand),
	})

	app.AddCommand(&grumble.Command{
		Name:  "del",
		Help:  "delete a value from the datastore",
		Usage: "del <key>",
		Args: func(a *grumble.Args) {
			a.String("key", "the key to delete")
		},
		Run: requireDS(delCommand),
	})

	app.AddCommand(&grumble.Command{
		Name:  "has",
		Help:  "check if a key exists in the datastore",
		Usage: "has <key>",
		Args: func(a *grumble.Args) {
			a.String("key", "the key to check")
		},
		Run: requireDS(hasCommand),
	})

	app.AddCommand(&grumble.Command{
		Name:  "sync",
		Help:  "sync a key to the backend, if supported",
		Usage: "sync <key>",
		Args: func(a *grumble.Args) {
			a.String("key", "the key to sync")
		},
		Run: requireDS(syncCommand),
	})

	app.AddCommand(&grumble.Command{
		Name: "size",
		Help: "get size of key",
		Args: func(a *grumble.Args) {
			a.String("key", "the key to get the size of")
		},
		Run: requireDS(sizeCommand),
	})

	app.AddCommand(&grumble.Command{
		Name: "query",
		Help: "query the datastore",
		Flags: func(f *grumble.Flags) {
			f.Bool("b", "binary", false, "print binary value to stdout")
			f.Int("l", "limit", 0, "limit the number of results")
			f.Int("o", "offset", 0, "offset the results")
			f.Bool("k", "keys-only", false, "only return keys")
			f.StringL("save", "", "save the value to a directory")
			f.StringL("fkp", "", "filter by key prefix")
			f.StringL("fkl", "", "filter by key less than")
			f.StringL("fkle", "", "filter by key less than or equal")
			f.StringL("fkg", "", "filter by key greater than")
			f.StringL("fkge", "", "filter by key greater than or equal")
			f.StringL("fke", "", "filter by key equal to")
			f.StringL("fvl", "", "filter by value less than")
			f.StringL("fvle", "", "filter by value less than or equal")
			f.StringL("fvg", "", "filter by value greater than")
			f.StringL("fvge", "", "filter by value greater than or equal")
			f.StringL("fve", "", "filter by value equal to")
			f.StringL("ok", "", "order by key")
			f.StringL("ov", "", "order by value")
		},
		Args: func(a *grumble.Args) {
			a.String("prefix", "the prefix to query", grumble.Default("/"))
		},
		Run: requireDS(queryCommand),
	})

	if err := app.Run(); err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("runerr: %w", err))
		os.Exit(1)
	}
}

func requireDS(f func(*grumble.Context) error) func(*grumble.Context) error {
	return func(c *grumble.Context) error {
		if DS == nil {
			return ERRNoDatastore
		}
		return f(c)
	}
}

func openCommand(c *grumble.Context) error {
	kind := c.Args.String("kind")
	path := c.Args.String("path")
	fpath, err := expandHome(path)
	if err != nil {
		return err
	}
	fpath, err = filepath.Abs(fpath)
	if err != nil {
		return err
	}
	switch kind {
	case "badger":
		DS, err = badger.NewDatastore(fpath, nil)
	case "leveldb":
		DS, err = leveldb.NewDatastore(fpath, nil)
	case "flatfs":
		DS, err = flatfs.Open(fpath, false)
	case "blob":
		DS, err = dsblob.New(context.Background(), path)
	default:
		err = fmt.Errorf("unknown datastore kind: %s", kind)
	}
	if err == nil {
		c.App.SetPrompt(fmt.Sprintf("dshell(%s:%s)-> ", kind, path))
	}
	return err
}

func kindsCommand(c *grumble.Context) error {
	c.App.Println("badger")
	c.App.Println("blob")
	c.App.Println("flatfs")
	c.App.Println("leveldb")
	return nil
}

func getCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	save := c.Flags.String("save")
	val, err := DS.Get(context.Background(), key)
	if err != nil {
		return err
	}
	if save != "" {
		save, err = expandHome(save)
		if err != nil {
			return err
		}
		save, err = filepath.Abs(save)
		if err != nil {
			return err
		}
		f, err := os.Create(save)
		if err != nil {
			return err
		}
		defer f.Close()
		n, err := f.Write(val)
		if err != nil {
			return err
		}
		c.App.Println("wrote ", n, " bytes to ", save)
		return nil
	}
	if c.Flags.Bool("binary") {
		_, err := c.App.Write(val)
		if err != nil {
			return err
		}
	} else {
		c.App.Println(string(val))
	}
	return nil
}

func putCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	val := []byte(c.Args.String("value"))
	return DS.Put(context.Background(), key, val)
}

func delCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	return DS.Delete(context.Background(), key)
}

func hasCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	has, err := DS.Has(context.Background(), key)
	if err != nil {
		return err
	}
	c.App.Println(has)
	return nil
}

func syncCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	return DS.Sync(context.Background(), key)
}

func sizeCommand(c *grumble.Context) error {
	key := datastore.NewKey(c.Args.String("key"))
	size, err := DS.GetSize(context.Background(), key)
	if err != nil {
		return err
	}
	c.App.Println(size)
	return nil
}

func queryCommand(c *grumble.Context) error {
	prefix := c.Args.String("prefix")
	offset := c.Flags.Int("offset")
	limit := c.Flags.Int("limit")
	keysOnly := c.Flags.Bool("keys-only")
	save := c.Flags.String("save")
	filters := make([]query.Filter, 0)
	if f := c.Flags.String("fkp"); f != "" {
		filters = append(filters, query.FilterKeyPrefix{
			Prefix: f,
		})
	}
	if f := c.Flags.String("fkl"); f != "" {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.LessThan,
			Key: f,
		})
	}
	if f := c.Flags.String("fkle"); f != "" {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.LessThanOrEqual,
			Key: f,
		})
	}
	if f := c.Flags.String("fkg"); f != "" {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.GreaterThan,
			Key: f,
		})
	}
	if f := c.Flags.String("fkge"); f != "" {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.GreaterThanOrEqual,
			Key: f,
		})
	}
	if f := c.Flags.String("fke"); f != "" {
		filters = append(filters, query.FilterKeyCompare{
			Op:  query.Equal,
			Key: f,
		})
	}
	if f := c.Flags.String("fvl"); f != "" {
		filters = append(filters, query.FilterValueCompare{
			Op:    query.LessThan,
			Value: []byte(f),
		})
	}
	if f := c.Flags.String("fvle"); f != "" {
		filters = append(filters, query.FilterValueCompare{
			Op:    query.LessThanOrEqual,
			Value: []byte(f),
		})
	}
	if f := c.Flags.String("fvg"); f != "" {
		filters = append(filters, query.FilterValueCompare{
			Op:    query.GreaterThan,
			Value: []byte(f),
		})
	}
	if f := c.Flags.String("fvge"); f != "" {
		filters = append(filters, query.FilterValueCompare{
			Op:    query.GreaterThanOrEqual,
			Value: []byte(f),
		})
	}
	if f := c.Flags.String("fve"); f != "" {
		filters = append(filters, query.FilterValueCompare{
			Op:    query.Equal,
			Value: []byte(f),
		})
	}

	orders := make([]query.Order, 0)
	if f := c.Flags.String("ok"); f != "" {
		orders = append(orders, query.OrderByKey{})
	}
	if f := c.Flags.String("ov"); f != "" {
		orders = append(orders, query.OrderByValue{})
	}

	qry := query.Query{
		Prefix:   prefix,
		Limit:    limit,
		Offset:   offset,
		KeysOnly: keysOnly,
	}

	c.App.Println("QUERY: ", qry)

	// datastores that do not support filters or orders will return an error
	// even for an empty slice, so the slice must be nil if empty.
	if len(filters) > 0 {
		qry.Filters = filters
	}
	if len(orders) > 0 {
		qry.Orders = orders
	}

	res, err := DS.Query(context.Background(), qry)
	if err != nil {
		return err
	}

	if save != "" {
		save, err = expandHome(save)
		if err != nil {
			return err
		}
		save, err = filepath.Abs(save)
		if err != nil {
			return err
		}
	}
	for e := range res.Next() {
		if save != "" {
			fn := filepath.Join(save, e.Key)
			dname := filepath.Dir(fn)
			err := os.MkdirAll(dname, 0755)
			if err != nil {
				return err
			}
			f, err := os.Create(fn)
			if err != nil {
				return err
			}
			n, err := f.Write(e.Value)
			if err != nil {
				return err
			}
			err = f.Close()
			if err != nil {
				return err
			}
			c.App.Println("wrote ", n, " bytes to ", f.Name())
			continue
		}

		c.App.Println("KEY: ", e.Key)
		if !keysOnly {
			if c.Flags.Bool("binary") {
				_, err := c.App.Write(e.Value)
				if err != nil {
					return err
				}
			} else {
				c.App.Println(string(e.Value))
			}
		}
	}
	return nil
}

// expand tilde to home directory
func expandHome(path string) (string, error) {
	if len(path) == 0 || path[0] != '~' {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path, err
	}
	return filepath.Join(home, path[1:]), nil
}
