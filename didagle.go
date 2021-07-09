package didagle

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

type DAGConfig struct {
	opMeta []OperatorMeta
	graph  GraphCluster

	scriptPath string
}

func (p *DAGConfig) loadTomlScriptFile(tomlScript string) error {
	if _, err := toml.DecodeFile(tomlScript, &p.graph); err != nil {
		log.Printf("Failed to parse toml script file:%s with err:%v", tomlScript, err)
		return err
	}
	p.graph.name = filepath.Base(tomlScript)
	err := p.graph.build(p.opMeta)
	if nil != err {
		log.Printf("Failed to build graph with err:%v", err)
		return err
	}
	return nil
}

func (p *DAGConfig) loadTomlScriptContent(tomlScript string) error {
	if _, err := toml.Decode(tomlScript, &p.graph); err != nil {
		log.Printf("Failed to parse toml script file:%s with err:%v", tomlScript, err)
		return err
	}
	p.graph.name = "DefaultCluster"
	err := p.graph.build(p.opMeta)
	if nil != err {
		log.Printf("Failed to build graph with err:%v", err)
		return err
	}
	return nil
}

func (p *DAGConfig) DumpDot() string {
	builder := &strings.Builder{}
	p.graph.dumpDot(builder)
	return builder.String()
}

func (p *DAGConfig) GenPng(filePath string) error {
	if len(filePath) > 0 {
		p.scriptPath = filePath
	}
	dot := p.DumpDot()
	dotFile := p.scriptPath + ".dot"
	err := ioutil.WriteFile(dotFile, []byte(dot), 0755)
	if nil != err {
		log.Printf("Failed to write dot with err:%v", err)
		return err
	}
	pngFile := p.scriptPath + ".png"
	_, err = exec.Command("dot", "-Tpng", dotFile, "-o", pngFile).Output()
	if err != nil {
		log.Printf(" exec cmd  failed with err:%v", err)
		return err
	}
	fmt.Printf("Write png into %s\n", pngFile)
	// fmt.Println(string(out[:]))
	return nil
}

func NewDAGConfigByFile(opMetaFile string, tomlScript string) (*DAGConfig, error) {
	jsonFile, err := os.Open(opMetaFile)
	if err != nil {
		log.Printf("Failed to load op meta file:%s with err:%v", opMetaFile, err)
		return nil, err
	}
	config := &DAGConfig{}
	err = json.NewDecoder(jsonFile).Decode(&config.opMeta)
	if nil != err {
		log.Printf("Failed to parse op meta file:%s with err:%v", opMetaFile, err)
		return nil, err
	}
	err = config.loadTomlScriptFile(tomlScript)
	if nil != err {
		return nil, err
	}
	config.scriptPath = tomlScript
	return config, nil
}

func NewDAGConfigByContent(opMeta string, tomlScript string) (*DAGConfig, error) {
	opMeta = strings.TrimSpace(opMeta)
	config := &DAGConfig{}
	if len(opMeta) > 0 {
		err := json.Unmarshal([]byte(opMeta), &config.opMeta)
		if nil != err {
			log.Printf("Failed to parse op meta %s with err:%v", opMeta, err)
			return nil, err
		}
	}

	err := config.loadTomlScriptContent(tomlScript)
	if nil != err {
		return nil, err
	}
	config.scriptPath = tomlScript
	return config, nil
}
