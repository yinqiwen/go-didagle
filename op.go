package didagle

type FieldFlags struct {
	Extern    int `json:"is_extern"`
	InOut     int `json:"is_in_out"`
	Agrregate int `json:"is_aggregate"`
}

type FieldMeta struct {
	Name  string     `json:"name"`
	ID    int64      `json:"id"`
	Type  string     `json:"type"`
	Flags FieldFlags `json:"flags"`
}

type OperatorMeta struct {
	Name   string      `json:"name"`
	Input  []FieldMeta `json:"input"`
	Output []FieldMeta `json:"output"`
}
