package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/yinqiwen/go-didagle"
)

type Page struct {
	Title string
	Body  []byte
}

func (p *Page) save() error {
	filename := p.Title + ".txt"
	return ioutil.WriteFile(filename, p.Body, 0600)
}

func loadPage(title string) (*Page, error) {
	filename := title + ".txt"
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return &Page{Title: title, Body: body}, nil
}

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	t, _ := template.ParseFiles(tmpl + ".html")
	t.Execute(w, p)
}

func viewHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[len("/view/"):]
	p, _ := loadPage(title)
	renderTemplate(w, "view", p)
}

func editHandler(w http.ResponseWriter, r *http.Request) {
	title := r.URL.Path[len("/edit/"):]
	p, err := loadPage(title)
	if err != nil {
		p = &Page{Title: title}
	}
	renderTemplate(w, "edit", p)
}

type WebRes struct {
	Path string `json:",omitempty"`
	Err  string `json:",omitempty"`
}

func main() {
	log.Printf("Start web server")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "edit.html")
	})
	http.Handle("/pngs/", http.StripPrefix("/pngs/", http.FileServer(http.Dir("./pngs"))))
	var cursor int64
	http.HandleFunc("/gen_png", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		ops := r.FormValue("ops")
		script := r.FormValue("script")
		// log.Printf("Receive form ops:%s", ops)
		// log.Printf("Receive form script:%s", script)
		dag, err := didagle.NewDAGConfigByContent(ops, script)
		rs := &WebRes{}
		if nil != err {
			rs.Err = fmt.Sprintf("%v", err)
			b, _ := json.Marshal(rs)
			w.WriteHeader(400)
			w.Header().Set("Content-Type", "application/json")
			w.Write(b)
			return
		}
		path := fmt.Sprintf("/pngs/%d", cursor)
		err = dag.GenPng("." + path)
		if nil != err {
			log.Printf("Error:%v", err)
			rs.Err = fmt.Sprintf("%v", err)
			b, _ := json.Marshal(rs)
			w.WriteHeader(400)
			w.Header().Set("Content-Type", "application/json")
			w.Write(b)
			return
		}
		png := path + ".png"

		rs.Path = png
		//w.WriteHeader(200)
		b, _ := json.Marshal(rs)

		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
		log.Printf("Response:%v", string(b))
		cursor++
		if 100 == cursor {
			cursor = 0
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))

}
