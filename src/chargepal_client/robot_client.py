from typing import Any, Type, TypeVar
from concurrent.futures import ThreadPoolExecutor
import grpc
import logging
from chargepal_client import local_server_pb2_grpc
from chargepal_client.local_server_pb2 import EmptyMessage
from chargepal_client.local_server_pb2_grpc import LocalServerStub, RobotClientServicer


T = TypeVar("T", bound="RobotClient")


class RobotClient(RobotClientServicer):
    IP_ADDRESS = "192.168.158.25"
    SERVER_PORT = 9000

    def __init__(self, grpc_server: grpc.Server) -> None:
        super().__init__()
        self.grpc_server = grpc_server  # Note: Must be refereced for persistence.
        self.ping_count = 0

    @classmethod
    def serve(cls: Type[T], port: int) -> T:
        """
        Create a gRPC server on the robot client to receive messages initiazed by the local server.
        """
        logging.basicConfig()
        grpc_server = grpc.server(ThreadPoolExecutor(max_workers=42))
        client = cls(grpc_server)
        local_server_pb2_grpc.add_RobotClientServicer_to_server(client, grpc_server)
        grpc_server.add_insecure_port(f"{cls.IP_ADDRESS}:{port}")
        grpc_server.start()
        return client

    def Ping(self, request: EmptyMessage, context: Any) -> EmptyMessage:
        self.ping_count += 1
        return EmptyMessage()

    def connect_server(self) -> LocalServerStub:
        """Return a stub for local server services."""
        return LocalServerStub(
            grpc.insecure_channel(f"{self.IP_ADDRESS}:{self.SERVER_PORT}")
        )
