FROM ubuntu:24.04

RUN apt update && apt install -y \
  ca-certificates \
  curl \
  git \
  sudo \
  && curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
  && apt install -y nodejs \
  && npm install -g pnpm \
  && rm -rf /var/lib/apt/lists/* \
  && useradd -m -s /bin/bash runner \
  && usermod -aG sudo runner \
  && echo "runner ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER runner
WORKDIR /home/runner

# published Aug 13, 2025
ARG RUNNER_VERSION=2.328.0

RUN mkdir actions-runner \
  && cd actions-runner \
  && curl -o actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz -L https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz \
  && tar xzf actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz \
  && rm actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz

WORKDIR /home/runner/actions-runner

RUN echo '#!/bin/bash\n\
set -e\n\
\n\
if [ -z "$GITHUB_TOKEN" ]; then\n\
  echo "Error: GITHUB_TOKEN environment variable is required"\n\
  exit 1\n\
fi\n\
\n\
if [ -z "$GITHUB_REPO" ]; then\n\
  echo "Error: GITHUB_REPO environment variable is required"\n\
  exit 1\n\
fi\n\
\n\
echo "Configuring GitHub Actions runner..."\n\
./config.sh --url https://github.com/${GITHUB_REPO} \\\n\
  --token ${GITHUB_TOKEN} \\\n\
  --name "docker-runner-${hostname}" \\\n\
  --labels "linux,benchmark" \\\n\
  --work _work \\\n\
  --unattended \\\n\
  --replace\n\
\n\
cleanup() {\n\
  echo "Removing runner..."\n\
  ./config.sh remove --unattended --token ${GITHUB_TOKEN}\n\
}\n\
\n\
trap cleanup EXIT\n\
\n\
echo "Starting GitHub Actions runner..."\n\
./run.sh\n\
' > start.sh && chmod +x start.sh

RUN mkdir -p /home/runner/scripts \
  && echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Repository: $GITHUB_REPOSITORY"\n\
echo "Working directory: $(pwd)"\n\
\n\
echo "Installing pnpm dependencies..."\n\
pnpm install\n\
\n\
echo "Running benchmark..."\n\
pnpm bench\n\
\n\
echo "Benchmark completed successfully!"\n\
' > /home/runner/scripts/run-bench.sh && chmod +x /home/runner/scripts/run-bench.sh

ENV PATH="/home/runner/scripts:${PATH}"

ENTRYPOINT ["./start.sh"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep -f "Runner.Listener" > /dev/null || exit 1
