package utils

import (
	"encoding/json"
	"testing"
)

func TestBloom(t *testing.T) {
	blm := NewBloom(8)
	inKey := "GGDDGGGFEFEF"
	blObj, err := blm.SetBloom(inKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("写入key %s : %+v", inKey, blObj)

	isExt := blm.ExistsBloom(inKey)
	t.Logf("key %s 可能存在%t", inKey, isExt)

	inKey = "rrDSDS"
	blObj, err = blm.SetBloom(inKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("写入key %s : %+v", inKey, blObj)

	inKey = "GGDDGGGFEFEFd"
	isExt = blm.ExistsBloom(inKey)
	t.Logf("key %s 可能存在%t", inKey, isExt)

	isDel := blm.RemoveBloom(inKey)
	t.Logf("key %s 删除过滤器%t", inKey, isDel)

	inKey = "GGDDGGGFEFEF"
	isDel = blm.RemoveBloom(inKey)
	t.Logf("key %s 删除过滤器%t", inKey, isDel)

	res, _ := json.Marshal(blm.ExtHash)
	t.Logf("当前过滤器数据：%s", res)

}
