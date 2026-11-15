/* PHP stream glue the Zend API only offers as macros. */
#define _GNU_SOURCE
#include <unistd.h>

#include "php.h"
#include "php_streams.h"
#include "php_network.h"

/* Wrap a duplicate of `fd` in a read-only PHP stream resource. Closing the
 * stream closes only the duplicate. Returns 0, or -1 on failure. */
int omq_php_fd_to_stream(int fd, zval *out)
{
    int dup_fd = dup(fd);
    if (dup_fd < 0) {
        return -1;
    }

    php_stream *stream = php_stream_fopen_from_fd(dup_fd, "r", NULL);
    if (stream == NULL) {
        close(dup_fd);
        return -1;
    }

    php_stream_to_zval(stream, out);
    return 0;
}

/* The pollable fd behind a PHP stream resource, or -1. */
int omq_php_stream_to_fd(zval *value)
{
    php_stream *stream;
    php_socket_t fd = -1;

    if (Z_TYPE_P(value) != IS_RESOURCE) {
        return -1;
    }

    php_stream_from_zval_no_verify(stream, value);
    if (stream == NULL) {
        return -1;
    }

    if (php_stream_can_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL) != SUCCESS
        || php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL, (void **) &fd, 0) != SUCCESS) {
        return -1;
    }

    return (int) fd;
}
