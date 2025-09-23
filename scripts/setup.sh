#/bin/bash

set -e
set -x

export PROJECT_PYTHON_VERSION=3.13

if ! command -v uv > /dev/null
then
    echo "Installing 'uv' command"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
    echo 'eval "$(uvx --generate-shell-completion bash)"' >> ~/.bashrc
    source ~/.bashrc
fi
uv python install $PROJECT_PYTHON_VERSION
uv python pin $PROJECT_PYTHON_VERSION

if ! command -v just > /dev/null
then 
    echo "Installing 'just' command runner"
    wget -qO - 'https://proget.makedeb.org/debian-feeds/prebuilt-mpr.pub' | gpg --dearmor | sudo tee /usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg 1> /dev/null
    echo "deb [arch=all,$(dpkg --print-architecture) signed-by=/usr/share/keyrings/prebuilt-mpr-archive-keyring.gpg] https://proget.makedeb.org prebuilt-mpr $(lsb_release -cs)" | sudo tee /etc/apt/sources.list.d/prebuilt-mpr.list   
    sudo apt update && sudo apt install just -y
fi
