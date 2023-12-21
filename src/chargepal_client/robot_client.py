from typing import Optional
from threading import Event, Thread
import grpc
import time
from chargepal_client.local_server_pb2_grpc import LocalServerStub


class RobotClient:
    IP_ADDRESS = "192.168.158.25"
    SERVER_PORT = 9000
    PING_INTERVAL = 1.0

    def __init__(self) -> None:
        self.next_time = time.time() + self.PING_INTERVAL
        self.stop_event = Event()  # Note: set() to stop
        self.ping_count = 0
        self.ping_thread = Thread(target=self.ping)
        self.ping_thread.start()

    def connect_server(
        self, ip_address: Optional[str] = None, port: Optional[int] = None
    ) -> LocalServerStub:
        """Return a stub for local server services."""
        return LocalServerStub(
            grpc.insecure_channel(
                f"{ip_address if ip_address else self.IP_ADDRESS}:{port if port else self.SERVER_PORT}"
            )
        )

    def ping(self) -> None:
        while not self.stop_event.wait(self.next_time - time.time()):
            self.ping_count += 1
            self.next_time += self.PING_INTERVAL
