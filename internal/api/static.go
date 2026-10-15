package api

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path"
)

// staticHandler serves the built UI from dir. Paths that don't match a file
// get index.html, so the UI's client-side routes (e.g. /topics/orders) work
// as deep links. http.Dir confines lookups to dir.
func staticHandler(dir string) http.Handler {
	root := http.Dir(dir)
	files := http.FileServer(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isFile(root, path.Clean("/"+r.URL.Path)) {
			http.ServeFile(w, r, path.Join(dir, "index.html"))
			return
		}
		files.ServeHTTP(w, r)
	})
}

func isFile(root http.FileSystem, name string) bool {
	f, err := root.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	return err == nil && !st.IsDir()
}

// apiNotFound answers unknown /api/ paths with the JSON error body, not the UI.
func apiNotFound(w http.ResponseWriter, r *http.Request) {
	writeErrorCode(w, http.StatusNotFound, CodeNotFound, "no such endpoint: "+r.URL.Path)
}

// checkUIDir fails fast at start-up if KAFKA3O_UI_DIR has no index.html.
func checkUIDir(dir string) error {
	if _, err := os.Stat(path.Join(dir, "index.html")); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errors.New("KAFKA3O_UI_DIR has no index.html: " + dir)
		}
		return err
	}
	return nil
}
