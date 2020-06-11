package amqp

import (
	"encoding/json"
	"errors"
	"github.com/582727501/machinery/v1/config"
	"github.com/582727501/machinery/v1/tasks"
)

// 由于go的machinery和python的数据格式不一样，python的多了中括号，没办法解开，换成这种方式
func GetBodyFromCelery(celeryBody []byte) (reply []byte, err error) {
	if config.CeleryMode == false {
		reply = celeryBody
		return
	}
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
func GetCeleryMsg(msg []byte) (celeryMsg []byte, err error) {
	if config.CeleryMode == false {
		celeryMsg = msg
		return
	}

	var signature = tasks.Signature{}
	err = json.Unmarshal(msg, &signature)
	if err != nil {
		return
	}

	//重新组装
	var result = map[string]interface{}{}
	for _, v := range signature.Args {
		result[v.Name] = v.Value
	}

	var newMsg []byte
	newMsg, err = json.Marshal(result)
	if err != nil {
		return
	}

	msgStr := "[[], " + string(newMsg) + ", {\"callbacks\": null, \"errbacks\": null, \"chain\": null, \"chord\": null}]"
	celeryMsg = []byte(msgStr)
	return
}