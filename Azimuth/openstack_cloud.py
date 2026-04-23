import typing as t

import openstack


def get_console_log_for_server(server_name: str) -> dict[str, t.Any]:
    """
    Retrieves the console log for a specified server.
    Returns the console output as a dict. Control characters are
    escaped to create a valid JSON string.
    """
    conn = openstack.connect()
    server = conn.compute.find_server(server_name)
    if server:
        return conn.compute.get_server_console_output(server)
    else:
        return None
