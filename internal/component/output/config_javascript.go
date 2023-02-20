package output

// JavaScript contains configuration fields for the JavaScript processor.
type JavaScriptConfig struct {
	Code          string   `json:"code" yaml:"code"`
	File          string   `json:"file" yaml:"file"`
	CacheRes      []string `json:"cache_res" yaml:"cache_res"`
	OutputRes     []string `json:"output_res" yaml:"output_res"`
	GlobalFolders []string `json:"registry_global_folders" yaml:"registry_global_folders"`
}

// NewSubprocessConfig returns a SubprocessConfig with default values.
func NewJavaScriptConfig() JavaScriptConfig {
	return JavaScriptConfig{
		Code:          "",
		File:          "",
		CacheRes:      []string{},
		OutputRes:     []string{},
		GlobalFolders: []string{},
	}
}
