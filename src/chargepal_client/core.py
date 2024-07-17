from typing import Callable, Optional, Tuple, List
from grpc import StatusCode
import grpc
from chargepal_client import communication_pb2
from chargepal_client import communication_pb2_grpc
import sqlite3
import ast
from datetime import datetime
import concurrent.futures


class Core:
    def __init__(self, server_address: str, robot_name: str):
        self.server_address = server_address
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = communication_pb2_grpc.CommunicationStub(self.channel)
        self.robot_name = robot_name

    def create_rdbc_skeleton(
        self, rdb_filepath: str, table_name: str, column_definitions: str
    ):
        """
        Creates a skeleton for the RDBC (Robot Database Copy) by creating a new table in the RDBC file
        with the specified table name and column definitions.

        Args:
            rdb_filepath (str): The file path of the original RDB (Robot Database) file.
            table_name (str): The name of the table to be created in the RDBC file.
            column_definitions (str): The column definitions for the table.

        Returns:
            None
        """
        # Filter out "action_state" as it is being added in the battery_action_info table
        columns_list = column_definitions.split(", ")
        columns_list = [
            col for col in columns_list if not col.startswith("action_state")
        ]
        column_definitions_updated = ", ".join(columns_list)

        rdbc_filepath = rdb_filepath.replace("rdb.db", "rdb_copy.db")
        conn_rdbc = sqlite3.connect(rdbc_filepath)
        cursor_rdbc = conn_rdbc.cursor()
        create_table_sql = f"""
                    CREATE TABLE {table_name} (
                        {column_definitions_updated},
                        update_timestamp TEXT,
                        ldb_push BOOLEAN
                    )
                    """
        create_battery_actions = f""" CREATE TABLE IF NOT EXISTS battery_action_info (
                                    name TEXT,
                                    action_state TEXT,
                                    update_timestamp TEXT,
                                    ldb_push BOOLEAN
                                )
                                """
        cursor_rdbc.execute(create_table_sql)
        cursor_rdbc.execute(create_battery_actions)
        conn_rdbc.commit()
        conn_rdbc.close()

    def update_database(
        self, rdb_filepath: str, response: communication_pb2.Response_UpdateRDB
    ):
        """
        Updates the SQLite database with the data provided in the response.

        Args:
            rdb_filepath (str): The file path of the SQLite database.
            response (communication_pb2.Response_UpdateRDB): The response containing the data to update the database.

        Returns:
            None
        """
        conn_rdb = sqlite3.connect(rdb_filepath)
        cursor_rdb = conn_rdb.cursor()

        for table_data in response.tables:
            table_name = table_data.table_name
            rows = table_data.rows
            column_names = table_data.column_names

            cursor_rdb.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            table_definition = cursor_rdb.fetchone()

            if table_definition is None:
                if rows:
                    column_definitions = ", ".join(
                        [f"{col} TEXT" for col in column_names]
                    )
                    create_table_sql = f"""
                    CREATE TABLE {table_name} (
                        {column_definitions}
                    )
                    """
                    cursor_rdb.execute(create_table_sql)
                    column_names = list(column_names)
                    if table_name != "env_info" and table_name != "orders_in":
                        self.create_rdbc_skeleton(
                            rdb_filepath, table_name, column_definitions
                        )

            # Extract column names from the table definition
            else:
                table_definition = table_definition[0]
                column_definitions = table_definition.split("(")[1].split(")")[0]
                column_names = [
                    col.split()[0].strip() for col in column_definitions.split(",")
                ]

            # Iterate over rows
            for row_msg in rows:
                row_identifier = row_msg.row_identifier
                row_values = ast.literal_eval(row_msg.column_values)

                # Convert tuple to list
                row_values_list = list(row_values)

                # Prepare column names and values for INSERT/UPDATE
                columns = ", ".join(column_names)
                placeholders = ", ".join("?" for _ in column_names)
                update_set = ", ".join([f"{col} = ?" for col in column_names])

                # Check if the row exists
                cursor_rdb.execute(
                    f"SELECT 1 FROM {table_name} WHERE rowid = ?", (row_identifier,)
                )
                exists = cursor_rdb.fetchone() is not None

                if exists:
                    # Update the existing row
                    update_values = row_values_list + [row_identifier]
                    cursor_rdb.execute(
                        f"UPDATE {table_name} SET {update_set} WHERE rowid = ?",
                        update_values,
                    )
                else:
                    # Insert a new row
                    cursor_rdb.execute(
                        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                        row_values_list,
                    )

        conn_rdb.commit()
        conn_rdb.close()

    def handle_request(self, request, request_name)-> tuple:
        """
        Handles different types of gRPC requests based on request_name.

        Args:
            request: The request object to be sent.
            request_name (str): The name indicating the type of request ('update_rdb' or 'log_text').

        Returns:
            tuple: A tuple containing the response and an error (if any).
                - If successful, returns (response, None).
                - If there's an RpcError during the gRPC call, returns (None, e).
        """
        try:
            if request_name == "update_rdb":
                response = self.stub.UpdateRDB(request)
            else:
                response = self.stub.LogText(request)
            return response, None
        except grpc.RpcError as e:
            return None, e

    def read_logs(self, file_path) -> str:
        """
        Reads the contents of a file located at the specified file_path.

        Args:
            file_path (str): The path to the file to be read.

        Returns:
            str: The contents of the file as a string.
        """
        with open(file_path, "r") as file:
            file_contents = file.read()
            return file_contents

    def background_requests(
        self,
        loop_condition: Callable[[], bool],
        rdb_filepath: str,
        log_filepath: str,
        publisher_callback: Callable[[str], None],
    ) -> None:
        """
        Updates the Robot Database (RDB) by sending a request to the server and
        handling the response.

        Args:
            loop_condition: A callable that returns a boolean value indicating
                whether the update loop should continue or not.
            rdb_filepath: The filepath where the updated RDB should be saved.
            log_filepath: The filepath where the logs are.
            publisher_callback: A callback function that is called with a status
                message indicating the result of the update.

        Returns:
            None
        """
        while loop_condition():
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                update_rdb_request = communication_pb2.Request(
                    robot_name=self.robot_name, request_name="update_rdb"
                )
                log_text_request = communication_pb2.Request(
                    robot_name=self.robot_name,
                    request_name="log_text",
                    log_text=self.read_logs(log_filepath),
                )
                update_rdb = executor.submit(
                    self.handle_request, update_rdb_request, "update_rdb"
                )
                log_text = executor.submit(
                    self.handle_request, log_text_request, "log_text"
                )

                update_rdb_response, error_update_rdb = update_rdb.result()
                log_text_response, error_log_text = log_text.result()

                # Process the first response
                if error_update_rdb:
                    if error_update_rdb.code() == StatusCode.UNAVAILABLE:
                        publisher_callback("SERVER_UNAVAILABLE")
                    elif error_update_rdb.code() == StatusCode.DEADLINE_EXCEEDED:
                        publisher_callback("SERVER_DEADLINE_EXCEEDED")
                    else:
                        publisher_callback(str(error_update_rdb.code()))

                else:
                    self.update_database(rdb_filepath, update_rdb_response)

                if error_log_text:
                    if error_log_text.code() == StatusCode.UNAVAILABLE:
                        publisher_callback("SERVER_UNAVAILABLE")
                    elif error_log_text.code() == StatusCode.DEADLINE_EXCEEDED:
                        publisher_callback("SERVER_DEADLINE_EXCEEDED")
                    else:
                        publisher_callback(str(error_log_text.code()))

                else:
                    publisher_callback("SERVER_CONNECTED")

    def fetch_job(self) -> Tuple[Optional[communication_pb2.Response_FetchJob], str]:
        """
        Fetches a job from the server.

        Returns:
            A tuple containing the fetched job response (communication_pb2.Response_FetchJob) and the status (str).
            If the job fetch is successful, the status will be "SUCCESSFUL".
            If the server is unavailable, the status will be "SERVER_UNAVAILABLE".
            If the server deadline is exceeded, the status will be "SERVER_DEADLINE_EXCEEDED".
            If any other error occurs, the status will be the error code as a string.
        """
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
        """
        Sends a request to the server to ask for free battery charging stations (BCS).

        Returns:
            A tuple containing the response from the server and the status of the request.
            The response is an optional `communication_pb2.Response_FreeStation` object.
            The status is a string indicating the result of the request, which can be one of the following:
            - "SUCCESSFUL" if the request was successful.
            - "SERVER_UNAVAILABLE" if the server is currently unavailable.
            - "SERVER_DEADLINE_EXCEEDED" if the request deadline was exceeded.
            - A string representation of the error code if an unexpected error occurred.
        """
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
        """
        Requests information about free battery swapping stations from the server.

        Returns:
            A tuple containing the response from the server and the status of the request.
            - The response is an optional `communication_pb2.Response_FreeStation` object.
            - The status is a string indicating the result of the request. Possible values are:
                - "SUCCESSFUL" if the request was successful.
                - "SERVER_UNAVAILABLE" if the server is currently unavailable.
                - "SERVER_DEADLINE_EXCEEDED" if the request deadline was exceeded.
                - The error code as a string if any other error occurred.
        """
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
        self, rdbc_string: str
    ) -> Tuple[Optional[communication_pb2.Response_PushToLDB], str]:
        """
        Pushes the given RDBC string to the LDB server.

        Args:
            rdbc_string (str): The RDBC string to be pushed.

        Returns:
            Tuple[Optional[communication_pb2.Response_PushToLDB], str]: A tuple containing the response from the LDB server
            and the status of the push operation.

        Raises:
            grpc.RpcError: If an error occurs during the gRPC communication with the LDB server.
        """
        response: Optional[communication_pb2.Response_PushToLDB] = None
        request = communication_pb2.Request(
            robot_name=self.robot_name,
            request_name="push_to_ldb",
            rdbc_data=rdbc_string,
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
        """
        Updates the job monitor with the given job type and status.

        Args:
            job_type (str): The type of the job.
            job_status (str): The status of the job.

        Returns:
            Tuple[Optional[communication_pb2.Response_UpdateJobMonitor], str]: A tuple containing the response from the server and the status of the update.

        Raises:
            grpc.RpcError: If an error occurs during the RPC call.
        """
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
        """
        Resets the station blocker for a given request name.

        Args:
            request_name (str): The name of the request.

        Returns:
            Tuple[Optional[communication_pb2.Response_ResetStationBlocker], str]: A tuple containing the response and the status.
                - response (Optional[communication_pb2.Response_ResetStationBlocker]): The response object.
                - status (str): The status of the request.
        """
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
        """
        Retrieves the operation time for a given cart.

        Args:
            cart (str): The name of the cart.

        Returns:
            Tuple[Optional[communication_pb2.Response_OperationTime], str]: A tuple containing the response object and the status string.
                - The response object is an optional instance of communication_pb2.Response_OperationTime, which contains the operation time information.
                - The status string indicates the status of the operation, such as "SUCCESSFUL", "SERVER_UNAVAILABLE", or "SERVER_DEADLINE_EXCEEDED".
        """
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
        """
        Checks if the specified station is ready to plug in ADS (Adapter Station).

        Args:
            station (str): The name of the station to check.

        Returns:
            Tuple[Optional[communication_pb2.Response_Ready2PlugInADS], str]: A tuple containing the response from the server
            and the status of the request. The response is an optional `communication_pb2.Response_Ready2PlugInADS` object,
            and the status is a string indicating the result of the request.

        Raises:
            grpc.RpcError: If an error occurs during the gRPC call.

        """
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

    def battery_communication(
        self, cart_name: str, request_name: str, station_name: str
    ) -> Tuple[Optional[communication_pb2.Response_BatteryCommunication], str]:
        """
        Checks if the specified station is ready to plug in ADS (Adapter Station).

        Args:
        cart_name (str): The name of the cart.
        request_name (str): The name of the battery request.
        station_name (str): The name of the current robot location.

        Returns:
            Tuple[Optional[communication_pb2.Response_BatteryCommunication], str]: A tuple containing the response from the server
            and the status of the request. The response is an optional `communication_pb2.Response_BatteryCommunication` object,
            and the status is a string indicating the result of the request.

        Raises:
            grpc.RpcError: If an error occurs during the gRPC call.

        """
        response: Optional[communication_pb2.Response_BatteryCommunication] = None
        request = communication_pb2.Request(
            station_name=station_name, request_name=request_name, cart_name=cart_name
        )
        try:
            response = self.stub.BatteryCommunication(request)
            status = "SUCCESSFUL"
        except grpc.RpcError as e:
            if e.code() == StatusCode.UNAVAILABLE:
                status = "SERVER_UNAVAILABLE"
            elif e.code() == StatusCode.DEADLINE_EXCEEDED:
                status = "SERVER_DEADLINE_EXCEEDED"
            else:
                status = str(e.code())
        return response, status
