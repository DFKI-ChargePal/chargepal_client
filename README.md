### Package for communication between robot and local server

To test,

- go into folder `src\chargepal_client`
- make sure the gRPC files exist
  - to generate: `python -m grpc_tools.protoc -Iprotos --python_out=. --pyi_out=. --grpc_python_out=. protos/local_server.proto`
- `python text_client_example.py`
- use server scripts from https://git.ni.dfki.de/chargepal/system-integration/server-packages/chargepal_local_server
