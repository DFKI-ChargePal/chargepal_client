#!/usr/bin/env python3
import rospy
import rospkg
import threading
import std_msgs.msg
from chargepal_client.core import Core


class Grpc_Client(Core):
    def __init__(self):
        super().__init__(
            rospy.get_param("/server_address", "192.168.158.25:50058"),
            rospy.get_param("/robot_name"),
        )
        self.rospack = rospkg.RosPack()
        self.rdb_filepath = self.rospack.get_path("chargepal_bundle") + "/db/rdb.db"
        self.heartbeat_publisher = rospy.Publisher(
            "/grpc_server_status", std_msgs.msg.String, queue_size=10
        )

    def update_rdb(self):
        super().update_rdb(
            lambda: not rospy.is_shutdown(),
            self.rdb_filepath,
            self.heartbeat_publisher.publish,
        )

    def pull_ldb(self):
        super().pull_ldb(
            self.rdb_filepath,
            self.heartbeat_publisher.publish,
        )


def main():
    rospy.init_node("chargepal_grpc_client")
    client = Grpc_Client()
    rdb_update_thread = threading.Thread(target=client.update_rdb)
    rdb_update_thread.start()
    rospy.spin()
    rdb_update_thread.join()


if __name__ == "__main__":
    main()
