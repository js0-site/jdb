cargo remove --dev tokio

fd --type file --hidden --exclude .git -x sd tokio compio" $@ "{}"

cargo add compio --dev
