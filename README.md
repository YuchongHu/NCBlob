# Revisiting Network Coding for Warm Blob Storage

The repository includes the source code of NCBlob and the paper accepted by FAST 2025. 

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

## Publication

Chuang Gan, Yuchong Hu, Leyan Zhao, Xin Zhao, Pengyu Gong, and Dan Feng.
**"Revisiting Network Coding for Warm Blob Storage."**
Proceedings of the 23rd USENIX Conference on File and Storage Technologies (FAST 2025), February 2025.
(AR: 36/167 = 21.5%)

## Contact

Please email to Yuchong Hu ([yuchonghu@hust.edu.cn](mailto:yuchonghu@hust.edu.cn)) if you have any questions.

## Our other works

Welcome to follow our other works!

1. FAST 2021: https://github.com/YuchongHu/ecwide
2. ICDCS 2021: https://github.com/YuchongHu/stripe-merge
3. SoCC 2019: https://github.com/YuchongHu/echash
4. INFOCOM 2018: https://github.com/YuchongHu/ncscale
5. TOS: https://github.com/YuchongHu/doubler