package helpers

import "encoding/json"

func GetJSONString(input interface{}) string {
	output, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return "unable to create json string"
	}
	return string(output)
}
