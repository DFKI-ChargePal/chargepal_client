#chargepal_client
This package helps in communicating to the grpc server.

**Note:**
Check if the `.py` and `.pyi` files are created inside `/chargepal_client/src/chargepal_client`. If not, inside `/chargepal_client/src/chargepal_client` run `python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. communication.proto`  to create them.

The table below shows the functions and its purpose

| Name | Description |
| ------ | ------ |
|    UpdateRDB    |   Updates the Robot Data Base      |
|  PullLDB      |    Pulls the Local Database(server)     |
|  UpdateJobMonitor      |  Updates the Job Monitor in the server      |
|   FetchJob     |   Fetches job from the server     |
|  AskFreeStation      | Asks the server for a free station       |
| PushToLDB       |  Pushes contents to Local Database       |
| ResetStationBlocker       |  Resets the said station blocker in the server      |
|  OperationTime      |  Fetches the charging time from the booking      |
| Ready2PlugInADS       |  Checks with the server if plugin in an Adapter Station can start       |
