# Setup

The only host requirements are **Docker** (with Compose v2), **make** and **git**. Spark, Scala,
Java 11, sbt and Python all live inside the image, so the host's own Java/Python versions do not matter.

## macOS (Intel or Apple Silicon)
1. Install Docker Desktop. Under **Settings → Resources**, give it **≥ 8 GB memory** and ≥ 4 CPUs.
2. `make` ships with the Xcode command line tools: run `xcode-select --install` if `make` is missing.

## Windows (WSL2)
1. Install Docker Desktop and enable **Use the WSL 2 based engine**, plus integration with your Ubuntu distro.
2. Clone the repository **inside the WSL filesystem** (`~/…`), not under `/mnt/c/…`. Bind mounts
   from the Windows drive are many times slower.
3. In `%UserProfile%\.wslconfig`, set `memory=10GB` (or more), then run `wsl --shutdown`.
4. Run `sudo apt install make` inside Ubuntu.

## Ubuntu (native)
1. Install Docker Engine and the compose plugin (`docker compose version` must work).
2. Add yourself to the `docker` group: `sudo usermod -aG docker $USER`, then log out and back in.
3. The container runs as root, so generated files are root-owned. Run `sudo chown -R $USER .`
   if you need to delete them from the host.

## First run (every member)
```bash
make build                       # ~10 min the first time (image + pip packages)
make shell                       # inside: spark-submit --version  -> 3.5.3, Scala 2.12, Java 11
make smoke                       # whole pipeline on the synthetic fixture (~5 min) — proves the setup works
make test                        # unit tests
```
Post a screenshot of `spark-submit --version` in the team chat (Phase 0 exit condition).

## Real data
```bash
cp /path/to/flight_data_cleaned_1year.csv data/raw/
make schema-check                # header vs the expected column contract
make sample                      # writes data/samples/sample_100k.csv from the real file
make all                         # quick run on the 100k sample
make all INPUT=full              # the run whose numbers go in the report
```

## Pinned versions
| Component | Version |
|---|---|
| Base image | `spark:3.5.3-scala2.12-java11-python3-ubuntu` |
| Spark / Scala / Java | 3.5.3 / 2.12.18 / OpenJDK 11.0.25 |
| Python | 3.8.10 (shipped in the official image; supported by PySpark 3.5) |
| sbt | 1.10.2 |
| Python packages | `docker/requirements.txt` |

The plan document suggested Python 3.10–3.11. The official Spark 3.5.3 image ships 3.8.10, which
Spark 3.5 supports. All Python code is written to run on 3.8, so no second interpreter is installed.
