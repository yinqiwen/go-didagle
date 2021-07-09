package didagle

type FieldMeta struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}

type OperatorMeta struct {
	Name   string      `json:"name"`
	Input  []FieldMeta `json:"input"`
	Output []FieldMeta `json:"output"`
}
