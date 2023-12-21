#!/usr/bin/env python3
import os
import sys
import time
from grpc._channel import _InactiveRpcError
from chargepal_client.local_server_pb2 import PortInfo, TextMessage
from chargepal_client.robot_client import RobotClient


def communicate(port: int) -> bool:
    try:
        client = RobotClient.serve(port)
        print(
            f"Creating communication to server on port {client.SERVER_PORT} and from server on port {port}."
        )
        stub = client.connect_server()
        stub.ConnectClient(PortInfo(port=port))
        print("Enter messages to send, or an empty one to stop.")
        while True:
            message = input("Input message: ")
            if not message:
                break
            stub.SendTextMessage(TextMessage(text=message))
            print(
                f"{client.ping_count} ping message{'s' if client.ping_count != 1 else ''}"
                " received from server since last input."
            )
            client.ping_count = 0
        return True
    except _InactiveRpcError:
        pass
    return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {os.path.basename(__file__)} <inbound port number>")
    else:
        port = int(sys.argv[1])
        try:
            while not communicate(port):
                # Try to reconnect every second.
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
