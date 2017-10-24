package ustore_test

import (
	"strings"
	"testing"
	"ustore"
)

func TestOk(t *testing.T) {
	ok := ustore.StatusOK()
	if !ok.Ok() {
		t.Errorf("expected ok status, but get: '%s'", ok.ToString())
	}
	const expected_str = "OK"
	if strings.Compare(ok.ToString(), expected_str) != 0 {
		t.Errorf("expected str is '%s', but get: '%s'", expected_str, ok.ToString())
	}
}

func TestNotFound1Msg(t *testing.T) {
	notfound := ustore.StatusNotFound("msg1")
	if !notfound.IsNotFound() {
		t.Errorf("expected notfound status, but get: '%s'", notfound.ToString())
	}
	const expected_str = "NotFound: msg1"
	if strings.Compare(notfound.ToString(), expected_str) != 0 {
		t.Errorf("expected str is '%s', but get: '%s'", expected_str, notfound.ToString())
	}
}

func TestNotFound2Msgs(t *testing.T) {
	notfound := ustore.StatusNotFound("msg1", "msg2")
	if !notfound.IsNotFound() {
		t.Errorf("expected notfound status, but get: '%s'", notfound.ToString())
	}
	const expected_str = "NotFound: msg1: msg2"
	if strings.Compare(notfound.ToString(), expected_str) != 0 {
		t.Errorf("expected str is '%s', but get: '%s'", expected_str, notfound.ToString())
	}
}
