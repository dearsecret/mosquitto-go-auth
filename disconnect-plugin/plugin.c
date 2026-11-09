#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <mosquitto.h>

static mosquitto_plugin_id_t *mosq_pid = NULL;

/* Implemented in Go. */
extern void GoOnDisconnect(char *username, char *client_id, int reason);

static int disconnect_callback(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_disconnect *ed = event_data;
    const char *username;
    const char *client_id;

    (void)event;
    (void)userdata;

    if (ed == NULL || ed->client == NULL) {
        return MOSQ_ERR_SUCCESS;
    }

    username = mosquitto_client_username(ed->client);
    client_id = mosquitto_client_id(ed->client);

    GoOnDisconnect(
        (char *)username,
        (char *)client_id,
        ed->reason
    );

    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_version(
    int supported_version_count,
    const int *supported_versions
)
{
    int i;

    for (i = 0; i < supported_version_count; i++) {
        if (supported_versions[i] == 5) {
            return 5;
        }
    }

    return -1;
}

int mosquitto_plugin_init(
    mosquitto_plugin_id_t *identifier,
    void **user_data,
    struct mosquitto_opt *opts,
    int opt_count
)
{
    (void)user_data;
    (void)opts;
    (void)opt_count;

    mosq_pid = identifier;

    return mosquitto_callback_register(
        mosq_pid,
        MOSQ_EVT_DISCONNECT,
        disconnect_callback,
        NULL,
        NULL
    );
}

int mosquitto_plugin_cleanup(
    void *user_data,
    struct mosquitto_opt *opts,
    int opt_count
)
{
    (void)user_data;
    (void)opts;
    (void)opt_count;

    return mosquitto_callback_unregister(
        mosq_pid,
        MOSQ_EVT_DISCONNECT,
        disconnect_callback,
        NULL
    );
}