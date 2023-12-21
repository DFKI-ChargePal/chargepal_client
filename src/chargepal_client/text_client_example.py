#!/usr/bin/env python3
import time
from grpc._channel import _InactiveRpcError
from chargepal_client.local_server_pb2 import TextMessage
from chargepal_client.robot_client import RobotClient


def communicate() -> bool:
    try:
        client = RobotClient()
        print(f"Creating communication to server on port {client.SERVER_PORT}.")
        stub = client.connect_server()
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
        print("Server not available.")
    finally:
        client.stop_event.set()
    return False


if __name__ == "__main__":
    try:
        while not communicate():
            # Try to reconnect every second.
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
