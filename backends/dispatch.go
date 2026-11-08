package backends

/*
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mqtt_protocol.h>

static int dispatch_message(
    const char *topic,
    const void *payload,
    int payloadlen,
    int qos,
    bool retain,
    uint32_t expiry
) {
    mosquitto_property *properties = NULL;
    int ret;

    if (expiry > 0) {
        ret = mosquitto_property_add_int32(
            &properties,
            MQTT_PROP_MESSAGE_EXPIRY_INTERVAL,
            expiry
        );

        if (ret != MOSQ_ERR_SUCCESS) {
            if (properties != NULL) {
                mosquitto_property_free_all(&properties);
            }
            return ret;
        }
    }

    ret = mosquitto_broker_publish_copy(
        NULL,
        topic,
        payloadlen,
        payload,
        qos,
        retain,
        properties
    );

    // On success, the broker owns properties.
    // On failure, the caller must free them.
    if (ret != MOSQ_ERR_SUCCESS && properties != NULL) {
        mosquitto_property_free_all(&properties);
    }

    return ret;
}
*/
import "C"

import (
    "fmt"
    "strings"
    "unsafe"
)

func Dispatch(
    topic string,
    payload []byte,
    qos int,
    retain bool,
    expiry uint32,
) error {
    if topic == "" {
        return fmt.Errorf("dispatch topic is empty")
    }

    if strings.ContainsAny(topic, "+#") {
        return fmt.Errorf("dispatch topic must not contain wildcards: %q", topic)
    }

    if strings.ContainsRune(topic, '\x00') {
        return fmt.Errorf("dispatch topic contains a null byte")
    }

    if qos < 0 || qos > 2 {
        return fmt.Errorf("invalid dispatch qos: %d", qos)
    }

    if len(payload) > 268435455 {
        return fmt.Errorf("dispatch payload is too large: %d bytes", len(payload))
    }

    cTopic := C.CString(topic)
    defer C.free(unsafe.Pointer(cTopic))

    var payloadPtr unsafe.Pointer
    if len(payload) > 0 {
        payloadPtr = unsafe.Pointer(&payload[0])
    }

    ret := C.dispatch_message(
        cTopic,
        payloadPtr,
        C.int(len(payload)),
        C.int(qos),
        C.bool(retain),
        C.uint32_t(expiry),
    )

    if ret != 0 {
        return fmt.Errorf("mosquitto broker dispatch failed: %d", int(ret))
    }

    return nil
}