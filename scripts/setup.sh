#/bin/bash

set -e

export PROJECT_PYTHON_VERSION=3.13

echo "Started 'uv' command installation step"
if ! command -v uv > /dev/null
then    
    echo "We did not found 'uv'. We will install it."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    echo "Enabling autocomplete for 'uv' command."
    echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc

    echo "Enabling autocomplete for 'uvx' alias command."
    echo 'eval "$(uvx --generate-shell-completion bash)"' >> ~/.bashrc

    source ~/.bashrc
else
    echo "The 'uv' command already exists. Bypassing."
fi
echo "Finished 'uv' command installation step"

echo "Started python $PROJECT_PYTHON_VERSION installation step"
uv python install $PROJECT_PYTHON_VERSION
uv python pin $PROJECT_PYTHON_VERSION
echo "Finished python $PROJECT_PYTHON_VERSION installation step"



#echo "Started dependencies installation step"
#uv sync
#echo "Finished installation step"

