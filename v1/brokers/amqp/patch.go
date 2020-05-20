package amqp

import (
	"encoding/json"
	"fmt"
)
import "errors"

// 由于go的machinery和python的数据格式不一样，python的多了中括号，没办法解开，换成这种方式
func GetBodyFromPythonCelery(celeryBody []byte) (reply []byte, err error) {
	body := string(celeryBody)
	res := make([]interface{}, 0)
	err = json.Unmarshal([]byte(body), &res)
	if err != nil {
		return
	}

	if len(res) < 2 {
		err = errors.New("bad data fomart")
		return
	}

	goCeleryBodyMap := res[1].(map[string]interface{})
	reply, err = json.Marshal(goCeleryBodyMap)
	if err != nil {
		return
	}
	return
}

// 发送给python celery的数据，重新组装
func GetPythonMsg(msg []byte) []byte {
	msgStr := "[[], " + string(msg) + ", {\"callbacks\": null, \"errbacks\": null, \"chain\": null, \"chord\": null}]"
	return []byte(msgStr)
}