from hetznercloud.actions import HetznerCloudAction
from .exceptions import HetznerInvalidArgumentException, HetznerActionException
from .shared import _get_results

import time

class HetznerCloudVolumesAction(object):
    def __init__(self, config):
        self._config = config

    def create(self, size, name, automount=False, format=None, location=None, server=None):
        if location is None and server is None:
            raise HetznerInvalidArgumentException("location_id and server")
        if automount and format is None:
            raise HetznerInvalidArgumentException("Format must be specified if automount enabled")

        body = {"size": size, "name": name}
        if automount is not None:
            body["automount"] = automount
        if format is not None:
            body["format"] = format
        if location is not None:
            body["location"] = location
        if server is not None:
            body["server"] = server

        status_code, results = _get_results(self._config, "volumes", method="POST", body=body)
        if status_code != 201:
            raise HetznerActionException(results)

        return HetznerCloudVolume._load_from_json(self._config, results["volume"]), \
            HetznerCloudAction._load_from_json(self._config, results["action"])

    def get_all(self):
        status_code, results = _get_results(self._config, "volumes")
        if status_code != 200:
            raise HetznerActionException(results)

        for result in results["volumes"]:
            yield HetznerCloudVolume._load_from_json(self._config, result)

    def get(self, id):
        status_code, result = _get_results(self._config, "volumes/%s" % id)
        if status_code != 200:
            raise HetznerActionException(result)

        return HetznerCloudVolume._load_from_json(self._config, result["volume"])


class HetznerCloudVolume(object):
    def __init__(self, config):
        self._config = config
        self.id = 0
        self.created = None
        self.name = ""
        self.server = None
        self.location_id = 0
        self.size = 0
        self.linux_device = ""
        self.status = ""
        self.format = ""

    def attach_to_server(self, server_id, automount):
        if not server_id:
            raise HetznerInvalidArgumentException("server_id")

        status_code, result = _get_results(self._config, "volumes/%s/actions/attach" % self.id, method="POST",
                                           body={"server": server_id, "automount": automount})
        if status_code != 201:
            raise HetznerActionException(result)

        self.server = server_id

        return HetznerCloudAction._load_from_json(self._config, result["action"])

    def delete(self):
        status_code, result = _get_results(self._config, "volumes/%s" % self.id, method="DELETE")
        if status_code != 204:
            raise HetznerActionException(result)

    def detach_from_server(self):
        status_code, result = _get_results(self._config, "volumes/%s/actions/detach" % self.id, method="POST")
        if status_code != 201:
            raise HetznerActionException(result)

        self.server = 0

        return HetznerCloudAction._load_from_json(self._config, result["action"])

    def wait_until_status_is(self, status, attempts=20, wait_seconds=1):
        """
        Sleeps the executing thread (a second each loop) until the status is either what the user requires or the
        attempt count is exceeded, in which case an exception is thrown.

        :param status: The status the action needs to be.
        :param attempts: The number of attempts to query the action's status.
        :param wait_seconds: The number of seconds to wait for between each attempt.
        :return: An exception, unless the status matches the status parameter.
        """
        if self.status == status:
            return

        action = HetznerCloudVolumesAction(self._config)
        for i in range(0, attempts):
            volume_status = action.get(self.id).status
            if volume_status == status:
                self.status = volume_status
                return

            time.sleep(wait_seconds)

        raise HetznerWaitAttemptsExceededException()

    @staticmethod
    def _load_from_json(config, json):
        volume = HetznerCloudVolume(config)

        volume.id = int(json["id"])
        volume.created = json["created"]
        volume.name = json["name"]
        volume.server = int(json["server"]) if json["server"] is not None else 0
        volume.location_id = int(json["location"]["id"])
        volume.size = json["size"]
        volume.linux_device = json["linux_device"]
        volume.status = json["status"]
        volume.format = json["format"]

        return volume
