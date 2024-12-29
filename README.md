# BUILD

This Project is built upon these dependencies:

- boost
- msgpack-cxx
- leveldb
- glog
- fmt
- hiredis

Build this project with the following steps:

- make a build directory
  `mkdir ./build && make ./bin && cd ./build`
- make this project
  `cmake -DCMAKE_BUILD_TYPE=RELEASE .. && cmake --build . --target all`

And the binary will be produced to the directory  `bin`

# RUN

## Coordinator

A coordinator manages the systems control flow and manages the metadata.

A working directory is necessary for the coordinator to store the metadata.

The configuration for the coordinator is composed in a file in toml format.  And an example of this is well documented in `./coord_cfg.toml`

To run a coordinator, specify the path of the configuration file.
`./coordinator <config file>`

## Worker

A worker receives and stores the data, and act upon the control flows from the coordinator.

A working directory is necessary for a worker to store its data.

The configuration for the worker is composed in a file in toml format. And an example of this is well documented in `./worker_cfg.toml`

To run a coordinator, specify the path of the configuration file.
`./data_worker <config file>`