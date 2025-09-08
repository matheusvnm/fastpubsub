#/bin/bash

set -e
set -x

export PROJECT_PYTHON_VERSION=3.13

if ! command -v uv > /dev/null
then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
    echo 'eval "$(uvx --generate-shell-completion bash)"' >> ~/.bashrc
    source ~/.bashrc
fi
uv python install $PROJECT_PYTHON_VERSION
uv python pin $PROJECT_PYTHON_VERSION
