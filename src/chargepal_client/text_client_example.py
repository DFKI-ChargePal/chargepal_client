#!/usr/bin/env python3
from typing import Optional
import os
import sys
import time
from grpc._channel import _InactiveRpcError
from chargepal_client.local_server_pb2 import Job, RobotID, TextMessage
from chargepal_client.robot_client import RobotClient


def communicate(ip_address: Optional[str], port: Optional[str]) -> bool:
    try:
        client = RobotClient()
        ip_address = ip_address if ip_address else client.IP_ADDRESS
        port = port if port else client.SERVER_PORT
        print(f"Creating communication to server at {ip_address}:{port}.")
        stub = client.connect_server(ip_address=ip_address, port=port)
        print("Enter messages to send, or an empty one to stop.")
        while True:
            message = input("Input message: ")
            if not message:
                break
            if message == "FETCH_JOB":
                job: Job = stub.FetchJob(RobotID(name="ChargePal01"))
                print(job)
            else:
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
    if len(sys.argv) > 3:
        print(f"Usage: {os.path.basename(__file__)} [<IP address>] [<port>]")
    else:
        try:
            ip_address = sys.argv[1] if len(sys.argv) >= 2 else None
            port = sys.argv[2] if len(sys.argv) >= 3 else None
            while not communicate(ip_address, port):
                # Try to reconnect every second.
                time.sleep(1.0)
        except KeyboardInterrupt:
            pass
