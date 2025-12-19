# Self-Hosted Runner

In order to have consitent benchmarks for comparisons, this self-hosted runner
with run the benchmarks on consistent hardware.

First build the Docker container:

```bash
./build.sh
```

Go to https://github.com/HarperFast/rocksdb-js/settings/actions/runners/new?arch=x64&os=linux
and locate the `--token` under the "Configure" section. Copy the token value and
paste it into the `run.sh` script.

Finally run the Docker container and it should register with GitHub and start
accepting benchmark jobs.

```bash
./run.sh
```

Note that if more than one self-hosted runner is registered, GitHub will
distribute the benchmark tasks across the available runners.
