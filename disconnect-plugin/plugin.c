
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <mosquitto.h>

static mosquitto_plugin_id_t *mosq_pid = NULL;

/* Implemented in Go. */
extern void GoSetRedisOption(char *key, char *value);
extern int GoInitRedis(void);
extern void GoCloseRedis(void);

extern void GoOnConnect(char *username, char *client_id);
extern void GoOnDisconnect(char *username, char *client_id, int reason);

/* CONNECT callback */
static int connect_callback(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_connect *ed = event_data;
    const char *username;
    const char *client_id;

    (void)event;
    (void)userdata;

    if (ed == NULL || ed->client == NULL) {
        return MOSQ_ERR_SUCCESS;
    }

    username = mosquitto_client_username(ed->client);
    client_id = mosquitto_client_id(ed->client);

    GoOnConnect(
        (char *)username,
        (char *)client_id
    );

    return MOSQ_ERR_SUCCESS;
}

/* DISCONNECT callback */
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
    int i;
    int rc;

    (void)user_data;

    mosq_pid = identifier;

    /* Redis 설정을 Go로 전달 */
    for (i = 0; i < opt_count; i++) {
        GoSetRedisOption(opts[i].key, opts[i].value);
    }

    /* Redis 클라이언트 초기화 */
    rc = GoInitRedis();
    if (rc != 0) {
        return MOSQ_ERR_UNKNOWN;
    }

    /* CONNECT callback 등록 */
    rc = mosquitto_callback_register(
        mosq_pid,
        MOSQ_EVT_CONNECT,
        connect_callback,
        NULL,
        NULL
    );

    if (rc != MOSQ_ERR_SUCCESS) {
        GoCloseRedis();
        return rc;
    }

    /* DISCONNECT callback 등록 */
    rc = mosquitto_callback_register(
        mosq_pid,
        MOSQ_EVT_DISCONNECT,
        disconnect_callback,
        NULL,
        NULL
    );

    if (rc != MOSQ_ERR_SUCCESS) {
        mosquitto_callback_unregister(
            mosq_pid,
            MOSQ_EVT_CONNECT,
            connect_callback,
            NULL
        );

        GoCloseRedis();
        return rc;
    }

    return MOSQ_ERR_SUCCESS;
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

    mosquitto_callback_unregister(
        mosq_pid,
        MOSQ_EVT_CONNECT,
        connect_callback,
        NULL
    );

    mosquitto_callback_unregister(
        mosq_pid,
        MOSQ_EVT_DISCONNECT,
        disconnect_callback,
        NULL
    );

    GoCloseRedis();

    return MOSQ_ERR_SUCCESS;
}