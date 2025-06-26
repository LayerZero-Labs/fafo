# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

RUN <<-'EOF' bash
	set -eu -o pipefail
	apt-get update
	apt-get install -y --no-install-recommends ca-certificates shfmt
	rm -rf /var/lib/apt/lists/*
EOF

# Install zsh and oh-my-zsh, and enable plugins
RUN <<-'EOF' bash
	set -eu -o pipefail
	apt-get update
	apt-get install -y --no-install-recommends zsh
	rm -rf /var/lib/apt/lists/*
	[[ ! -d ~/.oh-my-zsh ]] && sh -c "$(curl -L -o- https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended
	git clone --depth=1 https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
	git clone --depth=1 https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting
	git clone --depth=1 https://github.com/zsh-users/zsh-completions ${ZSH_CUSTOM:=~/.oh-my-zsh/custom}/plugins/zsh-completions
	git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k
	find ~/.oh-my-zsh/ -iname '.git' | xargs -I{} rm -rf {} \;
	sed -i'' -e 's/^plugins.*)/plugins=(git zsh-autosuggestions zsh-syntax-highlighting zsh-completions docker)/' -e 's,^ZSH_THEME=.*,ZSH_THEME="powerlevel10k/powerlevel10k",' ~/.zshrc
	cp ~/.oh-my-zsh/custom/themes/powerlevel10k/config/p10k-lean.zsh ~/.p10k.zsh
	sed -i'' -e '/^[ ]*newline.*$/d' -e 's/\(POWERLEVEL9K_PROMPT_ADD_NEWLINE\)=.*/\1=false/g' -e 's/\(POWERLEVEL9K_TRANSIENT_PROMPT\)=.*/\1=always/g' ~/.p10k.zsh
	echo "[[ ! -f ~/.p10k.zsh ]] || source ~/.p10k.zsh" >> ~/.zshrc
EOF
