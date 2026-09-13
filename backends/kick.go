package backends

/*
#include <stdlib.h>
#include <mosquitto_broker.h>

static int kick_client_by_username(const char *username) {
	return mosquitto_kick_client_by_username(username, false);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

func KickClientByUsername(username string) error {
	cUsername := C.CString(username)
	defer C.free(unsafe.Pointer(cUsername))

	if ret := C.kick_client_by_username(cUsername); ret != 0 {
		return fmt.Errorf("kick client failed: %d", int(ret))
	}

	return nil
}