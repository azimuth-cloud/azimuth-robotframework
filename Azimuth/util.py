import time
import typing as t

import azimuth_sdk


def wait_for_resource(
    resource,
    id: str,
    predicate: t.Callable[[t.Dict[str, t.Any]], bool],
    interval: int
) -> t.Dict[str, t.Any]:
    """
    Waits for the specified predicate to become True for the instance with the given ID.
    """
    while True:
        instance = resource.fetch(id)
        if predicate(instance):
            return instance
        else:
            time.sleep(interval)


def wait_for_resource_property(
    resource,
    id: str,
    property: str,
    target_value: t.Any,
    working_values: t.Collection[t.Any],
    error_message_property: str,
    interval: int,
) -> t.Dict[str, t.Any]:
    """
    Waits for the specified property on the instance with the given ID to reach a target
    value. It will only continue while the property is in the working values.
    """
    def predicate(instance):
        property_value = getattr(instance, property)
        if property_value == target_value:
            return True
        elif property_value in working_values:
            return False
        else:
            message = f"unexpected {property} - {property_value}"
            error_message = getattr(instance, error_message_property, None)
            if error_message:
                message = f"{message} - {error_message}"
            raise AssertionError(message)
    return wait_for_resource(resource, id, predicate, interval)


def delete_resource(resource, id: str, interval: int):
    """
    Deletes the instance with the specified ID and waits for it to be deleted.
    """
    # Keep trying to delete the resource until we don't get a 409
    # This allows it to be used as a teardown
    while True:
        try:
            resource.delete(id)
        except azimuth_sdk.APIError as exc:
            if exc.status_code != 409:
                raise
        else:
            break
        time.sleep(interval)
    while True:
        try:
            _ = resource.fetch(id)
        except azimuth_sdk.APIError as exc:
            if exc.status_code == 404:
                return
            else:
                raise
        time.sleep(interval)
