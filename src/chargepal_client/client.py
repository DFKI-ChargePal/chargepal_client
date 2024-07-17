#!/usr/bin/env python3
import rospy
import rospkg
import std_msgs.msg
from chargepal_client.core import Core


class Grpc_Client(Core):
    def __init__(self):
        super().__init__(
            rospy.get_param("/server_address"),
            rospy.get_param("/robot_name"),
        )
        self.rospack = rospkg.RosPack()
        self.rdb_file_path = rospy.get_param("/rdb_path")
        self.log_file_path = rospy.get_param("/log_file_path")
        self.heartbeat_publisher = rospy.Publisher(
            "/grpc_server_status", std_msgs.msg.String, queue_size=10
        )

    def background_requests(self):
        super().background_requests(
            lambda: not rospy.is_shutdown(),
            self.rdb_file_path,
            self.log_file_path,
            self.heartbeat_publisher.publish,
        )


def main():
    rospy.init_node("chargepal_grpc_client")
    client = Grpc_Client()
    client.background_requests()
    rospy.spin()


if __name__ == "__main__":
    main()
