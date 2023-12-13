#!/usr/bin/env python3
import os
import sys
import time
from chargepal_client.communication import Communication


class TestCommunication(Communication):
    def listen(self) -> None:
        try:
            super().listen()
        except EOFError:
            print("\nServer shutdown received. Press <Return> to continue.")
            self.active = False


def communicate(port: int) -> bool:
    """Communicate with server to send and receive messages."""
    try:
        with TestCommunication(port) as communication:
            print(
                f"Creating communication to server on port {communication.SERVER_ADDRESS[1]}"
                f" and from server on port {port}."
            )
            print("Enter messages to send, or an empty one to stop.")
            while True:
                message = input("Input message: ")
                if not communication.active:
                    return False
                if not message:
                    break
                try:
                    communication.send(message)
                except ConnectionRefusedError as e:
                    print(e)
                    return False
                messages = communication.pop_messages()
                count = len(messages)
                print(
                    f"{count} ping message{'s' if count != 1 else ''}"
                    " received from server since last input."
                )
        return True
    except ConnectionRefusedError as e:
        print(e)
        print("Server unavailable.")
    return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {os.path.basename(__file__)} <inbound port number>")
    else:
        port = int(sys.argv[1])
        try:
            while not communicate(port):
                # Retry connecting every second.
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
