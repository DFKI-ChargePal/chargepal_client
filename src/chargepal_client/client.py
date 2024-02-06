#!/usr/bin/env python3
import rospy
import rospkg
import grpc
import communication_pb2
import communication_pb2_grpc
import time
import threading
import sqlite3
from grpc import StatusCode
from grpc._channel import _Rendezvous
import std_msgs.msg

class Grpc_Client:
    def __init__(self):
        self.server_address = rospy.get_param('/server_address', '192.168.158.25:50058')
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = communication_pb2_grpc.CommunicationStub(self.channel)
        self.rospack = rospkg.RosPack()
        self.heartbeat_publisher = rospy.Publisher('/grpc_server_status', std_msgs.msg.String, queue_size=10)
        self.robot_name = rospy.get_param('/robot_name')

    def update_rdb(self):

        while not rospy.is_shutdown():
            request = communication_pb2.Request(robot_name=self.robot_name,request_name="update_rdb")
            try:
                response = self.stub.UpdateRDB(request)
                with open(self.rospack.get_path("chargepal_bundle")+'/db/rdb.db', 'wb') as rdb_file:
                    rdb_file.write(response.ldb)
                self.heartbeat_publisher.publish("SERVER_CONNECTED")
                #print("Database file received and replaced successfully.")

            except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    self.heartbeat_publisher.publish("SERVER_UNAVAILABLE")

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    self.heartbeat_publisher.publish("SERVER_DEADLINE_EXCEEDED")
                else:
                    self.heartbeat_publisher.publish(str(e.code()))


    def fetch_job(self):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,request_name="fetch_job")
        try:
            response = self.stub.FetchJob(request)
            status = "SUCCESSFUL"

        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status

    def free_bcs(self):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,request_name="ask_free_bcs")
        try:
            response = self.stub.AskFreeStation(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status

    def free_bws(self):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,request_name="ask_free_bws")
        try:
            response = self.stub.AskFreeStation(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status

    def push_to_ldb(self,table_name,rdb_string):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name, request_name="push_to_ldb", rdb_data=rdb_string, table_name=table_name)
        try:
            response = self.stub.PushToLDB(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status


    def update_job_monitor(self,job_type,job_status):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,job_name=job_type,job_status=job_status, request_name="update_job_monitor")
        try:
            response = self.stub.UpdateJobMonitor(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status

    def reset_station_blocker(self,request_name):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,request_name=request_name)
        try:
            response = self.stub.ResetStationBlocker(request)
            status = "SUCCESSFUL"

        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status


    def operation_time(self,cart):
        response = None
        request = communication_pb2.Request(cart_name=cart,request_name="operation_time")
        try:
            response = self.stub.ResetStationBlocker(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    status = "SERVER_UNAVAILABLE"

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    status = "SERVER_DEADLINE_EXCEEDED"
                else:
                    status = str(e.code())

        return response, status

    def pull_ldb(self):
        response = None
        request = communication_pb2.Request(robot_name=self.robot_name,request_name="update_rdb")
        try:
            response = self.stub.UpdateRDB(request)
            with open(self.rospack.get_path("chargepal_bundle")+'/db/rdb.db', 'wb') as rdb_file:
                rdb_file.write(response.ldb)
            with open(self.rospack.get_path("chargepal_bundle")+'/db/rdb_copy.db', 'wb') as rdb_file:
                rdb_file.write(response.ldb)
            self.heartbeat_publisher.publish("SERVER_CONNECTED")

        except grpc.RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                self.heartbeat_publisher.publish("SERVER_UNAVAILABLE")

            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                self.heartbeat_publisher.publish("SERVER_DEADLINE_EXCEEDED")
            else:
                self.heartbeat_publisher.publish(str(e.code()))

def main():
    rospy.init_node('chargepal_grpc_client')
    client = Grpc_Client()
    rdb_update_thread = threading.Thread(target=client.update_rdb)
    rdb_update_thread.start()
    rospy.spin()
    rdb_update_thread.join()


if __name__ == '__main__':
    main()
