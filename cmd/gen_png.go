package main

import (
	"flag"
	"log"

	"github.com/yinqiwen/go-didagle"
)

func main() {
	meta := flag.String("meta", "", "Specify input op meta file")
	script := flag.String("toml", "", "Specify input toml script")
	flag.Parse()

	if len(*meta) == 0 || len(*script) == 0 {
		flag.Usage()
		return
	}
	cfg, err := didagle.NewDAGConfigByFile(*meta, *script)
	if nil != err {
		log.Printf("%v", err)
		return
	}
	cfg.GenPng("")
}
