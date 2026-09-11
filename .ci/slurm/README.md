# Slurm Container

This directory defines a **pre-built** Slurm container used for automated
pull-request testing. Building Slurm from source is too expensive to do on
every CI run, so the image is built once and published to the GitHub
Container Registry (GHCR). CI then *pulls* the image and installs the
hpc-connect branch under test at runtime.

## What the image contains

- A fully built and configured Slurm `23.02.7` (see `install_slurm.sh`).
- MariaDB (Slurm accounting), munge, MPICH, and Python 3.12.
- It does **not** contain hpc-connect or canary. Both are installed at
  runtime by `test.sh` from the branch/PR being tested, so the same image is
  reusable by every CI run.

The `Dockerfile` in this directory produces exactly such an image. It is
functionally identical to (and interchangeable with) the `canary-slurm`
base image published at `ghcr.io/sandialabs/canary-slurm`, which is the
image CI actually pulls.

## How CI uses it

The `slurm` job in `.github/workflows/workflow.yml` does:

```yaml
docker pull ghcr.io/sandialabs/canary-slurm:latest
docker run --rm \
  -v .../.ci/slurm/test.sh:/root/test.sh \
  -e BRANCH_NAME=$BRANCH_NAME \
  ghcr.io/sandialabs/canary-slurm:latest \
  /bin/bash -c "./test.sh $BRANCH_NAME"
```

`test.sh` creates a venv, `pip install`s `canary-wm` (the test runner) and
`hpc-connect@git+...@$BRANCH_NAME` (the package under test), and runs the
Slurm scheduler test.

## Rebuilding and publishing the image

Because the base image is package-agnostic, hpc-connect reuses the
`ghcr.io/sandialabs/canary-slurm` image and does not normally publish its
own. If you need to build and push an equivalent image by hand:

```console
# 1. Build the base image from this directory
docker build --file Dockerfile --tag ghcr.io/sandialabs/canary-slurm:latest .

# 2. Log in to GHCR with a personal access token that has `write:packages`
echo "$GHCR_TOKEN" | docker login ghcr.io -u <your-github-username> --password-stdin

# 3. Push
docker push ghcr.io/sandialabs/canary-slurm:latest
```

## Running the image locally

```console
docker run -it --rm \
  -v "$PWD/test.sh:/root/test.sh" \
  -e BRANCH_NAME=main \
  ghcr.io/sandialabs/canary-slurm:latest \
  /bin/bash -c "./test.sh main"
```
