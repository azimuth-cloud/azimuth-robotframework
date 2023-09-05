import time
import typing as t

import azimuth_sdk


def wait_for_resource_property(
    resource,
    id: str,
    property: str,
    target_value: t.Any,
    working_values: t.Collection[t.Any],
    interval: int
):
    """
    Waits for the specified property on the instance with the given ID to reach a target
    value. It will only continue while the property is in the working values.
    """
    while True:
        instance = resource.fetch(id)
        property_value = getattr(instance, property)
        if property_value in working_values:
            time.sleep(interval)
            continue
        elif property_value == target_value:
            return instance
        else:
            raise AssertionError(f"unexpected {property} - {property_value}")
            

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
