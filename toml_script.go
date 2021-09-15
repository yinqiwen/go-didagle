package didagle

import (
	"fmt"
	"log"
	"strings"
)

const V_RESULT_OK int = 1
const V_RESULT_ERR int = 2
const V_RESULT_ALL int = 3

type GraphData struct {
	ID         string   `toml:"id"`
	Field      string   `toml:"field"`
	Aggregate  []string `toml:"aggregate"`
	Cond       string   `toml:"cond"`
	Required   bool     `toml:"required"`
	Move       bool     `toml:"move"`
	IsExtern   bool     `toml:"extern"`
	IsInOut    bool
	IsMapInput bool
}

type CondParams struct {
	Match string                 `toml:"match"`
	Args  map[string]interface{} `toml:"args"`
}

type Vertex struct {
	ID           string       `toml:"id"`
	Processor    string       `toml:"processor"`
	Cond         string       `toml:"cond"`
	Expect       string       `toml:"expect"`
	ExpectConfig string       `toml:"expect_config"`
	SelectArgs   []CondParams `toml:"select_args"`

	Cluster        string   `toml:"cluster"`
	Graph          string   `toml:"graph"`
	Successor      []string `toml:"successor"`
	SuccessorOnOk  []string `toml:"if"`
	SuccessorOnErr []string `toml:"else"`
	Deps           []string `toml:"deps"`
	DepsOnOk       []string `toml:"deps_on_ok"`
	DepsOnErr      []string `toml:"deps_on_err"`

	Input  []GraphData `toml:"input"`
	Output []GraphData `toml:"output"`
	Start  bool        `toml:"start"`

	successorVertex map[string]*Vertex
	depsResults     map[string]int
	isIdGenerated   bool
	isGenerated     bool
	g               *Graph
}

func (p *Vertex) dumpDotDefine(s *strings.Builder) {
	s.WriteString("    ")
	s.WriteString(p.getDotId())
	s.WriteString(" [label=\"")
	s.WriteString(p.getDotLabel())
	s.WriteString("\"")
	if len(p.Cond) > 0 {
		s.WriteString(" shape=diamond color=black fillcolor=aquamarine style=filled")
	} else if len(p.Graph) > 0 {
		s.WriteString(" shape=box3d, color=blue fillcolor=aquamarine style=filled")
	} else {
		s.WriteString(" color=black fillcolor=linen style=filled")
	}
	s.WriteString("];\n")
}

func (p *Vertex) dumpDotEdge(s *strings.Builder) {
	//log.Printf("Dump edge for %s/%s with deps:%d", p.g.Name, p.getDotLabel(), len(p.depsResults))
	if len(p.ExpectConfig) > 0 {
		expectConfigId := p.g.Name + "_" + p.ExpectConfig
		expectConfigId = strings.ReplaceAll(expectConfigId, "!", "")
		s.WriteString("    ")
		s.WriteString(expectConfigId)
		s.WriteString(" -> ")
		s.WriteString(p.getDotId())
		if p.ExpectConfig[0] == '!' {
			s.WriteString(" [style=dashed color=red label=\"err\"];\n")
		} else {
			s.WriteString(" [style=bold label=\"ok\"];\n")
		}

		s.WriteString("    ")
		s.WriteString(p.g.Name + "__START__")
		s.WriteString(" -> ")
		s.WriteString(expectConfigId + ";\n")
	}
	if nil == p.successorVertex || len(p.successorVertex) == 0 {
		s.WriteString("    " + p.getDotId() + " -> " + p.g.Name + "__STOP__;\n")
	}
	if nil == p.depsResults || len(p.depsResults) == 0 {
		s.WriteString("    " + p.g.Name + "__START__ -> " + p.getDotId() + ";\n")
	}

	if nil != p.depsResults && len(p.depsResults) > 0 {
		for id, expect := range p.depsResults {
			dep := p.g.getVertexById(id)
			s.WriteString("    " + dep.getDotId() + " -> " + p.getDotId())
			switch expect {
			case V_RESULT_OK:
				s.WriteString(" [style=dashed label=\"ok\"];\n")
			case V_RESULT_ERR:
				s.WriteString(" [style=dashed color=red label=\"err\"];\n")
			default:
				s.WriteString(" [style=bold label=\"all\"];\n")
			}
		}
	}
}

func (p *Vertex) findVertexInSuccessors(v *Vertex, visisted map[string]bool) bool {
	if nil != p.successorVertex {
		_, exist := p.successorVertex[v.ID]
		if exist {
			return true
		}
		visisted[v.ID] = true
		for _, successor := range p.successorVertex {
			if _, exist := visisted[successor.ID]; !exist {
				if successor.findVertexInSuccessors(v, visisted) {
					return true
				}
			}
		}
	}
	return false
}

func (p *Vertex) isSuccessorsEmpty() bool {
	return nil == p.successorVertex || len(p.successorVertex) == 0
}
func (p *Vertex) isDepsEmpty() bool {
	return nil == p.depsResults || len(p.depsResults) == 0
}

func (p *Vertex) verify() error {
	if !p.Start {
		if p.isDepsEmpty() && p.isSuccessorsEmpty() {
			return fmt.Errorf("Vertex:%s/%s has no deps and successors", p.g.Name, p.getDotLabel())
		}
	} else {
		if !p.isDepsEmpty() {
			return fmt.Errorf("Vertex:%s/%s is start vertex, but has non empty deps.", p.g.Name, p.getDotLabel())
		}
	}
	return nil
}

func (p *Vertex) getDotId() string {
	return p.g.Name + "_" + p.ID
}
func (p *Vertex) getDotLabel() string {
	if len(p.Cond) > 0 {
		return strings.ReplaceAll(p.Cond, "\"", "\\\"")
	}
	if len(p.Processor) > 0 {
		if !p.isIdGenerated {
			return p.ID
		}
		// if len(p.SelectArgs) > 0{
		// 	ss :=
		// }
		return p.Processor
	}
	if len(p.Graph) > 0 {
		return fmt.Sprintf("%s::%s", p.Cluster, p.Graph)
	}
	return "unknown"
}

func (p *Vertex) buildInputOutput() error {
	if len(p.Processor) == 0 {
		return nil
	}
	meta := p.g.cluster.getOpMeta(p.Processor)
	if nil == meta {
		return fmt.Errorf("No Processor:%s found", p.Processor)
	}
	for _, opInput := range meta.Input {
		match := false
		for _, localInput := range p.Input {
			if localInput.Field == opInput.Name {
				match = true
				break
			}
		}
		if !match {
			filed := GraphData{
				ID:         opInput.Name,
				Field:      opInput.Name,
				IsExtern:   opInput.Flags.Extern > 0,
				IsInOut:    opInput.Flags.InOut > 0,
				IsMapInput: opInput.Flags.Agrregate > 0,
			}
			p.Input = append(p.Input, filed)
		}
	}

	for _, opOutput := range meta.Output {
		match := false
		for _, localOutput := range p.Output {
			if localOutput.Field == opOutput.Name {
				match = true
				break
			}
		}
		if !match {
			filed := GraphData{
				ID:    opOutput.Name,
				Field: opOutput.Name,
			}
			p.Output = append(p.Output, filed)
		}
	}
	return nil
}
func (p *Vertex) depend(v *Vertex, expected int) {
	if nil == p.depsResults {
		p.depsResults = make(map[string]int)
	}
	p.depsResults[v.ID] = expected
	if nil == v.successorVertex {
		v.successorVertex = make(map[string]*Vertex)
	}
	v.successorVertex[p.ID] = p
	//log.Printf("####[%s/%s]depend %s->%s  %d", p.g.Name, p.getDotLabel(), v.getDotLabel(), p.getDotLabel(), len(p.depsResults))
}
func (p *Vertex) buildDeps(deps []string, expectedResult int) error {
	for _, id := range deps {
		dep := p.g.getVertexById(id)
		if nil == dep {
			return fmt.Errorf("[%s/%s]No dep vertex id:%s", p.g.Name, p.getDotLabel(), id)
		}
		p.depend(dep, expectedResult)
	}
	return nil
}

func (p *Vertex) buildSuccessor(sucessors []string, expectedResult int) error {
	for _, id := range sucessors {
		successor := p.g.getVertexById(id)
		if nil == successor {
			return fmt.Errorf("[%s]No successor id:%s", p.getDotLabel(), id)
		}
		successor.depend(p, expectedResult)
	}
	return nil
}

func (p *Vertex) build() error {
	for _, cond := range p.SelectArgs {
		if !p.g.cluster.ContainsConfigSetting(cond.Match) {
			return fmt.Errorf("No config_setting with name:%s defined.", cond.Match)
		}
	}

	for _, data := range p.Input {
		if len(data.Aggregate) == 0 && !data.IsMapInput {
			dep := p.g.getVertexByData(data.ID)
			if nil == dep && !data.IsExtern {
				return fmt.Errorf("[%s/%s]No dep input id:%s", p.g.Name, p.getDotLabel(), data.ID)
			}
			if nil == dep {
				continue
			}
			if data.IsInOut && dep == p {
				continue
			}
			if data.Required {
				p.depend(dep, V_RESULT_OK)
			} else {
				p.depend(dep, V_RESULT_ALL)
			}
		} else {
			for _, id := range data.Aggregate {
				dep := p.g.getVertexByData(id)
				if nil == dep && !data.IsExtern {
					return fmt.Errorf("[%s/%s]No dep input id:%s", p.g.Name, p.getDotLabel(), data.ID)
				}
				if nil == dep {
					continue
				}
				if data.Required {
					p.depend(dep, V_RESULT_OK)
				} else {
					p.depend(dep, V_RESULT_ALL)
				}
			}
		}
	}
	if err := p.buildDeps(p.DepsOnErr, V_RESULT_ERR); nil != err {
		return err
	}
	if err := p.buildDeps(p.DepsOnOk, V_RESULT_OK); nil != err {
		return err
	}
	if err := p.buildDeps(p.Deps, V_RESULT_ALL); nil != err {
		return err
	}
	if err := p.buildSuccessor(p.SuccessorOnErr, V_RESULT_ERR); nil != err {
		return err
	}
	if err := p.buildSuccessor(p.SuccessorOnOk, V_RESULT_OK); nil != err {
		return err
	}
	if err := p.buildSuccessor(p.Successor, V_RESULT_ALL); nil != err {
		return err
	}
	//log.Printf("%s/%s has if:%v else %v, all %v", p.g.Name, p.getDotLabel(), p.SuccessorOnOk, p.SuccessorOnErr, p.Successor)
	return nil
}

type ConfigSetting struct {
	Name      string `toml:"name"`
	Cond      string `toml:"cond"`
	Processor string `toml:"processor"`
}

type Graph struct {
	Name   string   `toml:"name"`
	Vertex []Vertex `toml:"vertex"`

	cluster     *GraphCluster
	genVertexs  map[string]*Vertex
	vertexMap   map[string]*Vertex
	dataMapping map[string]*Vertex

	genIdx int
}

func (p *Graph) dumpDot(buffer *strings.Builder) {
	buffer.WriteString("  subgraph cluster_")
	buffer.WriteString(p.Name)
	buffer.WriteString("{\n")
	buffer.WriteString("    style = rounded;\n")
	buffer.WriteString(fmt.Sprintf("    label = \"%s\";\n", p.Name))
	buffer.WriteString("    ")
	buffer.WriteString(p.Name + "__START__")
	buffer.WriteString("[color=black fillcolor=deepskyblue style=filled shape=Msquare label=\"START\"];\n")
	buffer.WriteString("    ")
	buffer.WriteString(p.Name + "__STOP__")
	buffer.WriteString("[color=black fillcolor=deepskyblue style=filled shape=Msquare label=\"STOP\"];\n")

	for _, v := range p.vertexMap {
		v.dumpDotDefine(buffer)
	}

	for _, c := range p.cluster.ConfigSetting {
		buffer.WriteString("    ")
		buffer.WriteString(p.Name + "_" + c.Name)
		buffer.WriteString(" [label=\"")
		buffer.WriteString(c.Name)
		buffer.WriteString("\"")
		buffer.WriteString(" shape=diamond color=black fillcolor=aquamarine style=filled];\n")
	}

	for _, v := range p.vertexMap {
		if v.isGenerated {
			continue
		}
		v.dumpDotEdge(buffer)
	}
	buffer.WriteString("};\n")
}

func (p *Graph) genVertexId() string {
	id := fmt.Sprintf("%s_%d", p.Name, p.genIdx)
	p.genIdx++
	return id
}
func (p *Graph) getVertexByData(data string) *Vertex {
	v, exist := p.dataMapping[data]
	if exist {
		return v
	}
	return nil
}
func (p *Graph) getVertexById(id string) *Vertex {
	v, exist := p.vertexMap[id]
	if exist {
		return v
	}
	return nil
}

func (p *Graph) genCondVertex(cond string) *Vertex {
	v := &Vertex{}
	v.ID = p.genVertexId()
	v.isIdGenerated = true
	v.isGenerated = true
	v.Processor = p.cluster.DefaultExprProcessor
	v.Cond = cond
	v.g = p
	p.vertexMap[v.ID] = v
	p.genVertexs[v.ID] = v
	return v
}

func CheckCyclic(v *Vertex, visited map[string]bool, recursionStack map[string]bool) bool{
	if !visited[v.ID] {
		visited[v.ID] = true
		recursionStack[v.ID] = true
		for _, successor := range v.successorVertex {
			if !visited[successor.ID] && CheckCyclic(successor, visited, recursionStack) {
				return true
			} else if recursionStack[successor.ID] {
				return true
			}
		}
	}
	recursionStack[v.ID] = false
	return false
}

func (p *Graph) testCircle() bool {
	visited := make(map[string]bool, len(p.vertexMap))
	recursionStack := make(map[string]bool, len(p.vertexMap))

	for _, v := range p.vertexMap {
		visited[v.ID] = false
		recursionStack[v.ID] = false
	}

	for _, v := range p.vertexMap {
		if CheckCyclic(v, visited, recursionStack) {
			log.Printf("Graph:%s has a circle with vertex:%s", p.Name, v.ID)
			return true
		}
	}
	return false
}

func (p *Graph) build() error {
	p.vertexMap = make(map[string]*Vertex)
	p.dataMapping = make(map[string]*Vertex)
	p.genVertexs = make(map[string]*Vertex)
	genCondNodes := make(map[string]*Vertex)
	for i := range p.Vertex {
		v := &p.Vertex[i]

		if len(v.ID) == 0 {
			if len(v.Processor) > 0 {
				v.ID = v.Processor
			} else {
				v.ID = p.genVertexId()
				v.isIdGenerated = true
			}
		}

		if len(v.Graph) > 0 {
			if len(v.Cluster) == 0 || v.Cluster == "." {
				v.Cluster = p.cluster.name
			}
		}

		if len(v.Expect) > 0 && len(v.ExpectConfig) > 0 {
			return fmt.Errorf("Vertex:%s can NOT both config 'expect' & 'expect_config'", v.ID)
		}
		if len(v.ExpectConfig) > 0 {
			if !p.cluster.ContainsConfigSetting(v.ExpectConfig) {
				return fmt.Errorf("No config_setting with name:%s defined", v.ExpectConfig)
			}
		}
		if len(v.Expect) > 0 {
			if _, exist := genCondNodes[v.Expect]; !exist {
				genCondNodes[v.Expect] = p.genCondVertex(v.Expect)
			}
			condVertex := genCondNodes[v.Expect]
			match := false
			for _, id := range v.DepsOnOk {
				if id == condVertex.ID {
					match = true
					break
				}
			}
			if !match {
				v.DepsOnOk = append(v.DepsOnOk, condVertex.ID)
			}
		}
		if _, exist := p.vertexMap[v.ID]; exist {
			return fmt.Errorf("Duplcate vertex id:%s", v.ID)
		}
		v.g = p
		p.vertexMap[v.ID] = v
		if p.cluster.StrictDsl {
			err := v.buildInputOutput()
			if nil != err {
				return err
			}
		}
		//inOutFields := make(map[string]bool)
		for idx := range v.Input {
			data := &v.Input[idx]
			if len(data.Field) == 0 {
				return fmt.Errorf("Empty data field in intput for node:%s", v.ID)
			}
			if len(data.ID) == 0 {
				data.ID = data.Field
			}
			// if data.IsInOut {
			// 	inOutFields[data.ID] = true
			// }
		}
		for idx := range v.Output {
			data := &v.Output[idx]
			if len(data.Field) == 0 {
				return fmt.Errorf("Empty data field in output for node:%s", v.ID)
			}
			if len(data.ID) == 0 {
				data.ID = data.Field
			}
			if p.cluster.StrictDsl {
				if prev, exist := p.dataMapping[data.ID]; exist {
					return fmt.Errorf("Duplicate data name:%s in vertex:%s/%s, prev vertex:%s", data.ID, v.g.Name, v.getDotLabel(), prev.getDotLabel())
				}
			}
			//do NOT mapping out if this field is inout
			// if _, exist := inOutFields[data.ID]; exist {
			// 	continue
			// }
			p.dataMapping[data.ID] = v
		}

	}

	for _, v := range p.vertexMap {
		err := v.build()
		if nil != err {
			return err
		}
	}
	for _, v := range p.vertexMap {
		if len(v.Cond) > 0 {
			continue
		}
		err := v.verify()
		if nil != err {
			return err
		}
		// if v.isDepsEmpty() && v.isSuccessorsEmpty() {
		// 	return fmt.Errorf("Vertex:%s/%s has no deps and successors", v.g.Name, v.getDotLabel())
		// }
	}
	if p.testCircle() {
		return fmt.Errorf("Circle Exist")
	}
	return nil
}

type GraphCluster struct {
	Desc                   string          `toml:"desc"`
	StrictDsl              bool            `toml:"strict_dsl"`
	DefaultExprProcessor   string          `toml:"default_expr_processor"`
	DefaultDefaultPoolSize string          `toml:"default_context_pool_size"`
	Graph                  []Graph         `toml:"graph"`
	ConfigSetting          []ConfigSetting `toml:"config_setting"`

	name string

	graphMap map[string]*Graph
	opsMap   map[string]OperatorMeta
}

func (p *GraphCluster) ContainsConfigSetting(name string) bool {
	for _, c := range p.ConfigSetting {
		if len(name) > 0 && name[0] == '!' {
			if c.Name == name[1:] {
				return true
			}
		} else {
			if c.Name == name {
				return true
			}
		}

	}
	return false
}

func (p *GraphCluster) getOpMeta(name string) *OperatorMeta {
	v, exist := p.opsMap[name]
	if !exist {
		return nil
	}
	return &v
}

func (p *GraphCluster) build(ops []OperatorMeta) error {
	p.opsMap = make(map[string]OperatorMeta)
	for _, op := range ops {
		p.opsMap[op.Name] = op
	}
	p.graphMap = make(map[string]*Graph)
	for i := range p.Graph {
		g := &p.Graph[i]
		g.cluster = p
		if _, exist := p.graphMap[g.Name]; exist {
			return fmt.Errorf("Duplicate graph name:%v", g.Name)
		}
		p.graphMap[g.Name] = g
		err := g.build()
		if nil != err {
			return err
		}
	}
	return nil
}

func (p *GraphCluster) dumpDot(buffer *strings.Builder) {
	buffer.WriteString("digraph G {\n")
	buffer.WriteString("    rankdir=LR;\n")
	for i := len(p.Graph) - 1; i >= 0; i-- {
		p.Graph[i].dumpDot(buffer)
	}
	buffer.WriteString("}\n")
}
