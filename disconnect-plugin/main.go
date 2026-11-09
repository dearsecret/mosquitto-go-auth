package main

/*
#include <stdlib.h>
*/
import "C"

import (
    "log"
)

//export GoOnDisconnect
func GoOnDisconnect(
    username *C.char,
    clientID *C.char,
    reason C.int,
) {
    user := ""
    client := ""

    if username != nil {
        user = C.GoString(username)
    }

    if clientID != nil {
        client = C.GoString(clientID)
    }

    log.Printf(
        "[mosquitto-disconnect] username=%q client_id=%q reason=%d",
        user,
        client,
        int(reason),
    )
}

func main() {}