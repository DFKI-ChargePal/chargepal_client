from typing import Callable, Optional, Tuple
from grpc import StatusCode
import grpc
import communication_pb2
import communication_pb2_grpc
import sqlite3
import ast


class Core:
    def __init__(self, server_address: str, robot_name: str):
        self.server_address = server_address
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = communication_pb2_grpc.CommunicationStub(self.channel)
        self.robot_name = robot_name

    def update_database(
        self, rdb_filepath: str, response: communication_pb2.Response_UpdateRDB
    ):
        conn_rdb = sqlite3.connect(rdb_filepath)
        cursor_rdb = conn_rdb.cursor()

        for table_data in response.tables:
            table_name = table_data.table_name
            rows = table_data.rows

            cursor_rdb.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            table_definition = cursor_rdb.fetchone()[0]
            # Extract column names from the table definition
            column_names = [
                part.split()[0] for part in table_definition.split("(")[1].split(",")
            ]

            # Iterate over rows
            for row_msg in rows:
                # Extract values from the Row message
                row_identifier = row_msg.row_identifier
                row_values = ast.literal_eval(row_msg.column_values)
                set_columns = ", ".join(
                    [f"{column_name} = ?" for column_name in column_names]
                )
                cursor_rdb.execute(
                    f"UPDATE {table_name} SET {set_columns} WHERE rowid = ?",
                    row_values + (row_identifier,),
                )
        conn_rdb.commit()
        conn_rdb.close()

    def update_rdb(
        self,
        loop_condition: Callable[[], bool],
        rdb_filepath: str,
        publisher_callback: Callable[[str], None],
    ) -> None:
        while loop_condition():
            request = communication_pb2.Request(
                robot_name=self.robot_name, request_name="update_rdb"
            )
            try:
                response = self.stub.UpdateRDB(request)

                self.update_database(rdb_filepath, response)

                publisher_callback("SERVER_CONNECTED")
                # print("Database file received and replaced successfully.")

            except grpc.RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    publisher_callback("SERVER_UNAVAILABLE")

                elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                    publisher_callback("SERVER_DEADLINE_EXCEEDED")
                else:
                    publisher_callback(str(e.code()))

    def fetch_job(self) -> Tuple[Optional[communication_pb2.Response_FetchJob], str]:
        response: Optional[communication_pb2.Response_Job] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name, request_name="fetch_job"
        )
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

    def free_bcs(self) -> Tuple[Optional[communication_pb2.Response_FreeStation], str]:
        response: Optional[communication_pb2.Response_FreeStation] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name, request_name="ask_free_bcs"
        )
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

    def free_bws(self) -> Tuple[Optional[communication_pb2.Response_FreeStation], str]:
        response: Optional[communication_pb2.Response_FreeStation] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name, request_name="ask_free_bws"
        )
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

    def push_to_ldb(
        self, table_name: str, rdb_string: str
    ) -> Tuple[Optional[communication_pb2.Response_PushToLDB], str]:
        response: Optional[communication_pb2.Response_PushToLDB] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name,
            request_name="push_to_ldb",
            rdb_data=rdb_string,
            table_name=table_name,
        )
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

    def update_job_monitor(
        self, job_type: str, job_status: str
    ) -> Tuple[Optional[communication_pb2.Response_UpdateJobMonitor], str]:
        response: Optional[communication_pb2.Response_UpdateJobMonitor] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name,
            job_name=job_type,
            job_status=job_status,
            request_name="update_job_monitor",
        )
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

    def reset_station_blocker(
        self, request_name: str
    ) -> Tuple[Optional[communication_pb2.Response_ResetStationBlocker], str]:
        response: Optional[communication_pb2.Response_ResetStationBlocker] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name, request_name=request_name
        )
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

    def operation_time(
        self, cart: str
    ) -> Tuple[Optional[communication_pb2.Response_OperationTime], str]:
        response: Optional[communication_pb2.Response_OperationTime] = None
        request = communication_pb2.Request(
            cart_name=cart, request_name="operation_time"
        )
        try:
            response = self.stub.OperationTime(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                status = "SERVER_UNAVAILABLE"

            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                status = "SERVER_DEADLINE_EXCEEDED"
            else:
                status = str(e.code())
        return response, status

    def ready_to_plug_in_ads(
        self, station: str
    ) -> Tuple[Optional[communication_pb2.Response_Ready2PlugInADS], str]:
        response: Optional[communication_pb2.Response_Ready2PlugInADS] = None
        request = communication_pb2.Request(
            station_name=station, request_name="ready_to_plug_in_ads"
        )
        try:
            response = self.stub.Ready2PlugInADS(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                status = "SERVER_UNAVAILABLE"

            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                status = "SERVER_DEADLINE_EXCEEDED"
            else:
                status = str(e.code())
        return response, status

    def pull_ldb(
        self,
        rdb_filepath: str,
        publisher_callback: Callable[[str], None],
    ):
        response: Optional[communication_pb2.Response_PullLDB] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name, request_name="pull_ldb"
        )
        try:
            response = self.stub.PullLDB(request)
            with open(rdb_filepath, "wb") as rdb_file:
                rdb_file.write(response.ldb)
            with open(rdb_filepath.replace("rdb.db", "rdb_copy.db"), "wb") as rdb_file:
                rdb_file.write(response.ldb)
            publisher_callback("SERVER_CONNECTED")
            return True
        except grpc.RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                publisher_callback("SERVER_UNAVAILABLE")

            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                publisher_callback("SERVER_DEADLINE_EXCEEDED")
            else:
                publisher_callback(str(e.code()))
            return False
