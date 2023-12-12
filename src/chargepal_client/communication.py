from typing import List, Type
from types import TracebackType
from threading import Thread
from multiprocessing.connection import Client, Listener


class Communication:
    SERVER_ADDRESS = ("localhost", 1024)

    def __init__(self, port: int) -> None:
        assert (
            1024 <= port <= 65535 and port != self.SERVER_ADDRESS[1]
        ), f"Robot port must be in 1024-65535 and different from server port {self.SERVER_ADDRESS[1]}."
        self.active = True
        self.messages: List[str] = []
        self.port = port
        self.robot_address = ("localhost", port)
        self.send("REQUEST_PORT")
        self.listener = Listener(self.robot_address)
        self.listener_thread = Thread(target=self.listen)
        self.listener_thread.start()

    def __enter__(self) -> "Communication":
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self.shutdown()

    def send(self, message: str) -> None:
        """Establish connection to send one message to server."""
        try:
            client = Client(self.SERVER_ADDRESS)
            client.send(f"{self.port} {message}")
            client.close()
        except ConnectionRefusedError as e:
            raise e

    def listen(self) -> None:
        """Listen for server messages on this robot's dedicated port."""
        connection = self.listener.accept()
        while self.active:
            # Note: 08.12.2023 Cannot abort this connection cleanly
            #  without receiving a message from the server.
            self.on_message_received(str(connection.recv()))
        connection.close()

    def on_message_received(self, message: str) -> None:
        self.messages.append(message)

    def shutdown(self) -> None:
        if self.active:
            self.active = False
            Client(self.robot_address).close()
            self.listener.close()
            self.listener_thread.join()
